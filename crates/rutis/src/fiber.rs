use std::any::{Any, TypeId};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::ctx::{Ctx, Shared};
use crate::diagnostics::ServiceAccess;
use crate::effect::{Effect, EffectMeta, EffectRecord};
use crate::error::{aggregate_arcs, panic_error, CordisError, ServiceReadFailure};
use crate::event::{CatchUnwind, Event};
use crate::key::{InstanceId, ScopeId, TypeKey};
use crate::{BoxFuture, Plugin, PluginFactory};

/// 六态状态机(§四:保留 TS 六态)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberState {
    /// 等待声明的依赖就绪。
    Pending,
    /// apply 执行中。
    Loading,
    /// 装配完成,提供中。
    Active,
    /// validate/apply 失败。
    Failed,
    /// 已移除,不可重启(root 除外)。
    Disposed,
    /// 清理执行中。
    Unloading,
}

/// fiber 快照(watch 载荷,generation 兼作 sequence,D24)。
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub generation: u64,
    pub state: FiberState,
    pub error: Option<Arc<CordisError>>,
}

/// 插件身份(D10:注册返回的显式 id,非闭包指针)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PluginId(pub u64);

/// 等待卸载的结果。超时只结束本次等待；驱动继续执行清理，再次调用
/// `dispose()` 会 join 同一任务。同步代码若不让出执行权，Tokio 无法
/// 强制中断，deadline 也无法在该 runtime 线程上及时触发。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DisposeWaitError {
    #[error("fiber {plugin_id:?} generation {generation} still {state:?} after {elapsed:?}")]
    TimedOut {
        plugin_id: PluginId,
        generation: u64,
        state: FiberState,
        elapsed: Duration,
    },
    #[error("fiber disposal failed: {0}")]
    Failed(Arc<CordisError>),
}

/// fiber 状态迁移事件(D24:锁内 FIFO 入队、锁外分发;
/// `seq` 保证提交顺序可识别,不保证 listener 完成顺序)。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FiberStatusChanged {
    pub plugin_id: PluginId,
    pub seq: u64,
    pub generation: u64,
    pub from: FiberState,
    pub to: FiberState,
}

impl Event for FiberStatusChanged {
    const NAME: &'static str = "rutis::FiberStatusChanged";
    type Value = ();
}

pub(crate) enum TaskDone {
    Running,
    Done(Option<Arc<CordisError>>),
}

/// 恰好一次的转换任务(D6/D20):锁内单 `Arc`,同代 join 同一个;
/// 完成结果缓存 `Arc<CordisError>`(identity 归这里,D25)。
pub(crate) struct TransitionTask {
    pub done_tx: watch::Sender<TaskDone>,
    /// 常驻 receiver 保活(§八 7 同坑):无 receiver 时 watch 通道视为关闭,
    /// `send` 静默失败——消费者快速完成(如等值合并直接跳过)而 join 方
    /// 尚未订阅时,完成值丢失、join 永等(对拍 isolate 用例实锤)。
    _keepalive: watch::Receiver<TaskDone>,
}

impl TransitionTask {
    pub(crate) fn new() -> Arc<Self> {
        let (done_tx, done_rx) = watch::channel(TaskDone::Running);
        Arc::new(Self {
            done_tx,
            _keepalive: done_rx,
        })
    }

    pub(crate) fn complete(&self, err: Option<Arc<CordisError>>) {
        let _ = self.done_tx.send(TaskDone::Done(err));
    }
}

pub(crate) enum Intent {
    RefreshDeps,
    /// 带完成信号的依赖重查(驱逐 join 用,D14):处理完成后任务收到终态。
    RefreshDepsJoin(Arc<TransitionTask>),
    /// 稳定性栅栏(简化 Settle):FIFO 排到它时,此前入队的意图都已处理完,
    /// 按当时状态完成任务——"稳定了吗"由 mailbox 顺序直接回答。
    Settle(Arc<TransitionTask>),
    Restart(Arc<TransitionTask>),
    Dispose,
    Shutdown(Arc<TransitionTask>),
}

impl Intent {
    fn task(&self) -> Option<&Arc<TransitionTask>> {
        match self {
            Intent::RefreshDepsJoin(task)
            | Intent::Settle(task)
            | Intent::Restart(task)
            | Intent::Shutdown(task) => Some(task),
            _ => None,
        }
    }
}

/// 意图携带的完成信号即刻完成(驱动不存在时防 join 永等)。
fn complete_intent(intent: &Intent, err: Option<Arc<CordisError>>) {
    if let Some(task) = intent.task() {
        task.complete(err);
    }
}

enum NextState {
    Pending,
    Disposed,
}

pub(crate) struct Trans {
    pub generation: u64,
    pub state: FiberState,
    pub error: Option<Arc<CordisError>>,
    pub seq: u64,
    pub status_queue: Vec<FiberStatusChanged>,
    pub terminal_task: Option<Arc<TransitionTask>>,
}

pub(crate) struct FiberInner {
    pub id: PluginId,
    pub instance: InstanceId,
    pub name: String,
    pub is_root: bool,
    pub plugin: Option<Arc<dyn Plugin>>,
    /// 工厂模式(D32):与 `plugin` 互斥(工厂模式 plugin 为 None)。
    pub factory: Option<Arc<dyn ErasedFactory>>,
    /// 工厂模式的当前 config(Arc 存储便于快照;update 原子替换)。
    pub config: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
    pub ctx: Ctx,
    pub parent_fiber: Option<Weak<FiberInner>>,
    pub children: Mutex<Vec<Weak<FiberInner>>>,
    pub closing: Arc<AtomicBool>,
    pub event_flights: Mutex<usize>,
    pub event_flights_tx: watch::Sender<usize>,
    _event_flights_rx: watch::Receiver<usize>,
    pub shutdown_task: Mutex<Option<Arc<TransitionTask>>>,
    pub shutdown_inner: Mutex<Option<Arc<TransitionTask>>>,
    pub driver: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 当前 fiber 代的取消 token(D27):每次 load 新建一代;卸载第②步取消。
    /// 意图发送方(dispose/restart/驱逐)在入队前预取消,使运行中的 apply
    /// 经 `ctx.cancelled()` 协作退出——驱动串行,不预取消则 apply 永远等不到。
    pub token: Mutex<CancellationToken>,
    pub transition: Mutex<Trans>,
    pub snapshot_tx: watch::Sender<Snapshot>,
    /// 常驻 receiver:保活 watch 通道(无 receiver 时通道视为关闭,send 静默失败)。
    pub snapshot_rx: watch::Receiver<Snapshot>,
    pub effects: Mutex<Vec<Arc<EffectRecord>>>,
    /// Weak index keeps draining records visible after `drain_effects` takes
    /// the active list, without retaining cleanups or finished child fibers.
    pub effect_index: Mutex<Vec<Weak<EffectRecord>>>,
    /// 已 drain 记录寄存的清理错误(0.2.1):记录 Done 自摘后,错误在此
    /// 等待 fiber 级卸载/重启统一观察——单一错误保持 Arc 同一性,
    /// 与记录留在列表中被再次 join 的旧语义等价。
    pub drained_errors: Mutex<Vec<Arc<CordisError>>>,
    /// 本 fiber 在 parent 上的 mount 记录(级联 dispose 的 effect,
    /// 0.2.1:终态退出时主动 drain,记录自摘,长寿 parent 不残留)。
    pub mount: Mutex<Option<Arc<EffectRecord>>>,
    /// 注册时捕获的依赖声明快照(spawn 边界一次性读取;终态退出时据此注销
    /// `inject_index`,不再回调用户 `injects()`)。
    pub declared_injects: Vec<TypeKey>,
    pub intents_tx: mpsc::UnboundedSender<Intent>,
    /// 驱动存活标志:置 false 后 post 拒绝投递并即刻完成携带的任务
    ///(评审 #2/#3:驱动退出后的排队意图不得让 join 永等)。
    pub alive: AtomicBool,
    /// 本 fiber 提供的 (key, scope)。
    pub provided: Mutex<Vec<(TypeKey, Option<crate::key::ScopeId>)>>,
    /// 等值合并键:上次成功装载解析到的依赖四元组集(§〇 epoch 语义)。
    /// 亦是驱逐判定的唯一事实源(D21:消费者 = last_deps 含该四元组者;
    /// 含作用域——同一 fiber 在不同作用域提供的同键绑定不可混淆)。
    pub last_deps: Mutex<Option<HashSet<(PluginId, u64, TypeKey, Option<crate::key::ScopeId>)>>>,
    pub accesses: Mutex<Vec<ServiceAccess>>,
}

/// 工厂擦除面(D32):泛型 `PluginFactory<C>` 的类型擦除适配层内部协议。
pub(crate) trait ErasedFactory: Send + Sync + 'static {
    fn config_type_id(&self) -> TypeId;
    fn name(&self) -> &str;
    fn injects(&self) -> &[TypeKey];
    fn validate_config_erased(&self, config: &dyn Any) -> Result<(), CordisError>;
    fn build_erased(&self, config: &dyn Any) -> Result<Box<dyn Plugin>, CordisError>;
}

/// 泛型工厂的擦除包装(同 registry `StoredValue` 手法)。
struct FactoryAdapter<F, C> {
    factory: F,
    _marker: std::marker::PhantomData<fn() -> C>,
}

impl<F, C> ErasedFactory for FactoryAdapter<F, C>
where
    F: PluginFactory<C>,
    C: Send + Sync + 'static,
{
    fn config_type_id(&self) -> TypeId {
        TypeId::of::<C>()
    }

    fn name(&self) -> &str {
        self.factory.name()
    }

    fn injects(&self) -> &[TypeKey] {
        self.factory.injects()
    }

    fn validate_config_erased(&self, config: &dyn Any) -> Result<(), CordisError> {
        config
            .downcast_ref::<C>()
            .ok_or_else(|| CordisError::Validation {
                issues: vec!["config type mismatch".into()],
            })
            .and_then(|c| self.factory.validate_config(c))
    }

    fn build_erased(&self, config: &dyn Any) -> Result<Box<dyn Plugin>, CordisError> {
        config
            .downcast_ref::<C>()
            .ok_or_else(|| CordisError::Validation {
                issues: vec!["config type mismatch".into()],
            })
            .and_then(|c| self.factory.build(c))
    }
}

impl FiberInner {
    pub(crate) fn push_effect(&self, record: Arc<EffectRecord>) {
        self.effects.lock().unwrap().push(record.clone());
        self.effect_index
            .lock()
            .unwrap()
            .push(Arc::downgrade(&record));
    }

    /// 当前代插件实例(D32):静态模式克隆既有实例;工厂模式用当前
    /// config 构造(每代一个新实例 = 配置热更新生效点)。
    /// `build` 是用户回调且 panic 概率高于 validate(评审:三处用户回调
    /// 唯独此处漏边界会杀驱动 → fiber 卡 Loading、join 永等)——panic
    /// 转 `fail_load`,与 validate 的边界对称。
    fn current_plugin(&self) -> Result<Arc<dyn Plugin>, CordisError> {
        if let Some(plugin) = &self.plugin {
            return Ok(plugin.clone());
        }
        let factory = self
            .factory
            .as_ref()
            .ok_or_else(|| CordisError::PluginFailed("fiber has no plugin or factory".into()))?;
        let config = self
            .config
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| CordisError::PluginFailed("factory fiber has no config".into()))?;
        let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            factory.build_erased(config.as_ref())
        }))
        .unwrap_or_else(|p| Err(panic_error(p)));
        built.map(Arc::from)
    }

    /// 当前代 token(`ctx.cancellation_token()`/`ctx.cancelled()` 暴露给插件)。
    pub(crate) fn current_token(&self) -> CancellationToken {
        self.token.lock().unwrap().clone()
    }

    pub(crate) fn cancel_current(&self) {
        self.token.lock().unwrap().cancel();
    }

    /// Cancel the current token of every live descendant, so callbacks that
    /// cooperate with their owner's cancellation can end before the drain.
    pub(crate) fn cancel_descendants(&self) {
        let mut pending: Vec<_> = self
            .children
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        while let Some(fiber) = pending.pop() {
            fiber.cancel_current();
            pending.extend(
                fiber
                    .children
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(Weak::upgrade),
            );
        }
    }

    pub(crate) fn begin_event(&self) {
        let mut count = self.event_flights.lock().unwrap();
        *count += 1;
        let _ = self.event_flights_tx.send(*count);
    }

    pub(crate) fn finish_event(&self) {
        let mut count = self.event_flights.lock().unwrap();
        *count -= 1;
        let _ = self.event_flights_tx.send(*count);
    }

    pub(crate) async fn wait_events(&self) {
        let mut rx = self.event_flights_tx.subscribe();
        loop {
            if *rx.borrow_and_update() == 0 {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    fn new_generation_token(&self) -> CancellationToken {
        let token = CancellationToken::new();
        *self.token.lock().unwrap() = token.clone();
        token
    }

    /// 投递无完成信号的意图。驱动已退出时返回 false；终态错误
    /// 预留给未来可能携带完成信号的意图。
    pub(crate) fn post(&self, intent: Intent) -> bool {
        if !self.alive.load(Ordering::SeqCst) {
            complete_intent(&intent, self.stopped_error());
            return false;
        }
        match self.intents_tx.send(intent) {
            Ok(()) => true,
            Err(err) => {
                complete_intent(&err.0, self.stopped_error());
                false
            }
        }
    }

    fn stopped_error(&self) -> Option<Arc<CordisError>> {
        self.transition.lock().unwrap().error.clone().or_else(|| {
            self.ctx
                .shared()
                .closing
                .load(Ordering::SeqCst)
                .then(|| Arc::new(CordisError::Closed))
        })
    }

    /// 投递携带完成信号的意图。发送后复查 alive:若发送落在驱动排空之后
    /// (该意图无人处理),任务即刻自完成。重复完成不会留下等待者
    ///(封住"查过 alive → 驱动退出排空 → send 才落地"的竞态,简化红线)。
    /// 自完成携带 fiber 终态错误而非凭空 Ok(评审 3:Failed 态下驱动退出,
    /// None 会把真实错误覆盖成伪装成功)。
    pub(crate) fn post_join(
        &self,
        task: Arc<TransitionTask>,
        make: impl FnOnce(Arc<TransitionTask>) -> Intent,
    ) -> bool {
        if !self.alive.load(Ordering::SeqCst) {
            task.complete(self.stopped_error());
            return false;
        }
        match self.intents_tx.send(make(task.clone())) {
            Ok(()) => {
                if !self.alive.load(Ordering::SeqCst) {
                    task.complete(self.stopped_error());
                }
                true
            }
            Err(_) => {
                task.complete(self.stopped_error());
                false
            }
        }
    }

    pub(crate) fn state(&self) -> FiberState {
        self.transition.lock().unwrap().state
    }

    pub(crate) fn state_snapshot(&self) -> Snapshot {
        let tr = self.transition.lock().unwrap();
        Snapshot {
            generation: tr.generation,
            state: tr.state,
            error: tr.error.clone(),
        }
    }

    /// 锁内:改状态 + FIFO 入队 + watch 发布(绝不持锁跨 await,D5)。
    fn set_state(&self, tr: &mut Trans, new: FiberState) {
        let old = tr.state;
        if old == new {
            return;
        }
        tr.state = new;
        tr.seq += 1;
        tr.status_queue.push(FiberStatusChanged {
            plugin_id: self.id,
            seq: tr.seq,
            generation: tr.generation,
            from: old,
            to: new,
        });
        let _ = self.snapshot_tx.send(Snapshot {
            generation: tr.generation,
            state: new,
            error: tr.error.clone(),
        });
    }

    /// 锁外:排干状态事件队列,fire-and-forget 分发(D24)。
    fn flush_status(&self) {
        let queue: Vec<FiberStatusChanged> = {
            let mut tr = self.transition.lock().unwrap();
            std::mem::take(&mut tr.status_queue)
        };
        for event in queue {
            if let Err(error) =
                self.ctx
                    .shared()
                    .bus
                    .emit(&self.ctx, &crate::EventKey::of(), Arc::new(event))
            {
                self.ctx.error_sink()(Arc::new(error));
            }
        }
    }

    fn resolve_deps(
        &self,
    ) -> (
        HashSet<(PluginId, u64, TypeKey, Option<ScopeId>)>,
        Vec<TypeKey>,
    ) {
        let mut satisfied: HashSet<(PluginId, u64, TypeKey, Option<ScopeId>)> = HashSet::new();
        let mut missing: Vec<TypeKey> = Vec::new();
        // Reuse the declarations captured at registration. The index, gate,
        // and diagnostics must all observe the same keys.
        let registry = &self.ctx.shared().registry;
        for key in self.declared_injects.iter().cloned() {
            if self.ctx.check_instance(&key).is_err() {
                missing.push(key);
                continue;
            }
            let scope = self.ctx.scope_for(&key);
            match registry.resolve_dep(&key, scope.as_ref()) {
                Some((provider_id, provider_gen)) => {
                    satisfied.insert((provider_id, provider_gen, key, scope));
                }
                None => missing.push(key),
            }
        }
        (satisfied, missing)
    }

    /// Whether this generation's strict reads include a declared
    /// dependency found unavailable (removed, provider inactive, check
    /// rejected). The access record is cleared when a generation loads.
    fn read_a_lost_dependency(&self) -> bool {
        self.accesses.lock().unwrap().iter().any(|access| {
            access.strict
                && matches!(access.failure, Some(ServiceReadFailure::Unavailable(_)))
                && self.declared_injects.contains(&access.key)
        })
    }

    async fn refresh_deps(this: &Arc<Self>) {
        if this.closing.load(Ordering::SeqCst) || this.ctx.shared().closing.load(Ordering::SeqCst) {
            return;
        }
        if this.plugin.is_none() && this.factory.is_none() {
            return;
        }
        if this.state() == FiberState::Disposed {
            return;
        }
        let (satisfied, missing) = this.resolve_deps();
        // 被取消的 Loading 代视同"已装载":排干后再定去留
        let loaded = !matches!(this.state(), FiberState::Pending | FiberState::Disposed);
        if !missing.is_empty() {
            // 依赖缺失:Active/Loading 卸载回 Pending;缺依赖长期 Pending
            // 不报错(D22)。**Failed 保持 Failed**(cordis FAILED 粘性:
            // epoch 已是 INACTIVE,`_setEpoch` 早退不迁移,错误持续可见
            // ——fiber.ts:611-639;审计 #2:此前被算作 loaded 降级 Pending,
            // 错误隐入 settle 通道)。依赖恢复走下方装载路径,Failed 照常
            // 重试(cordis 同:epoch 变化触发 reload)。
            if matches!(this.state(), FiberState::Active | FiberState::Loading) {
                Self::unload(this, NextState::Pending).await;
            }
            return;
        }
        // 等值合并:依赖三元组集未变则跳过(§〇 epoch 内容派生相等键)
        let unchanged = {
            let last = this.last_deps.lock().unwrap();
            last.as_ref() == Some(&satisfied)
        };
        if unchanged {
            return;
        }
        if loaded {
            Self::unload(this, NextState::Pending).await;
        }
        Self::load(this, satisfied).await;
    }

    async fn load(this: &Arc<Self>, deps: HashSet<(PluginId, u64, TypeKey, Option<ScopeId>)>) {
        let shared = this.ctx.shared().clone();
        let _admission = shared.admission.lock().unwrap();
        if this.closing.load(Ordering::SeqCst) || shared.closing.load(Ordering::SeqCst) {
            return;
        }
        let generation = {
            let mut tr = this.transition.lock().unwrap();
            // Dispose registration and generation admission share this lock.
            // Either disposal forbids the new generation, or its cancellation
            // sees the token installed here (transition -> token lock order).
            if tr.terminal_task.is_some() {
                return;
            }
            this.new_generation_token();
            tr.generation += 1;
            tr.error = None;
            Self::set_state(this, &mut tr, FiberState::Loading);
            tr.generation
        };
        this.accesses.lock().unwrap().clear();
        this.flush_status();
        drop(_admission);

        // 当前代实例(D32):静态模式既有实例;工厂模式从当前 config 构造,
        // 构造失败按装载失败回滚(fail_load 保持装配原子性)。
        let plugin = match this.current_plugin() {
            Ok(p) => p,
            Err(e) => {
                Self::fail_load(this, e).await;
                return;
            }
        };

        // validate-before-store(D12):validate 失败 → Failed;
        // validate 是用户回调,panic 同样转入 Failed(评审 #6:不杀驱动)
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| plugin.validate())) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                Self::fail_load(this, e).await;
                return;
            }
            Err(p) => {
                Self::fail_load(this, panic_error(p)).await;
                return;
            }
        }

        // 依赖快照(D21:装载窗口内即"绑定中",驱逐判定读它)
        *this.last_deps.lock().unwrap() = Some(deps.clone());

        // apply:直接等待退出(D7 第③步"等 apply 退出",不中止)。
        // 预取消(dispose/restart/驱逐)使观察 token 的插件经 ctx.cancelled()
        // 协作返回;不观察则 dispose 无限等待(协作取消限制,D27)。
        let ctx = this.ctx.for_generation(generation);
        // async block 把创建 Future 的同步回调也放进 unwind 边界。
        let outcome = CatchUnwind::new(async { plugin.apply(&ctx).await }).await;
        let result: Result<Effect, CordisError> = outcome.unwrap_or_else(|p| Err(panic_error(p)));
        match result {
            Ok(effect) => {
                let record = EffectRecord::new(
                    effect,
                    format!("plugin apply: {}", this.name),
                    Arc::downgrade(this),
                );
                this.push_effect(record);
                if this.closing.load(Ordering::SeqCst) {
                    Self::unload(this, NextState::Disposed).await;
                    return;
                }
                // 惯性锁 2(fiber.spec:LOADING 期同 fiber 被重新 provide,in-flight
                // 加载直接完成进 ACTIVE):装载窗口内依赖集若已整体翻新且无缺失,
                // 就地采纳新三元组集——排队的重查意图将看到"未变化"而跳过,
                // 不触发换代重载。仍有缺失则不采纳,按原语义卸载(惯性锁 1)。
                let (fresh, missing) = this.resolve_deps();
                if missing.is_empty() {
                    *this.last_deps.lock().unwrap() = Some(fresh);
                }
                {
                    let mut tr = this.transition.lock().unwrap();
                    tr.error = None;
                    Self::set_state(this, &mut tr, FiberState::Active);
                }
                // 通知本 fiber 提供的服务键:后到消费者激活
                let provided: Vec<TypeKey> = this
                    .provided
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in provided {
                    shared.registry.notify_key_changed(&key);
                }
            }
            // apply 报告声明的依赖在门控之后、取用之前消失。证据是本代的
            // 访问记录:确有一次对声明依赖的严格读取因"不可用"而失败(被摘除、
            // 提供者失活、check 拒绝)——门控之后依赖确实失效过,哪怕此刻已
            // 恢复。这一代视同被驱逐,回滚后回到 Pending(不进粘性 Failed);
            // 已恢复则立即重查装载。没有这样的读取时不认这个理由,按普通
            // 失败处理,避免装载循环。
            Err(CordisError::InjectUnsatisfied(_)) if this.read_a_lost_dependency() => {
                Self::unload(this, NextState::Pending).await;
                if this.resolve_deps().1.is_empty() {
                    this.post(Intent::RefreshDeps);
                }
            }
            Err(e) => Self::fail_load(this, e).await,
        }
        this.flush_status();
    }

    /// 失败装载的回滚(评审 #1,对齐 TS fiber.ts:749-779 的失败路径):
    /// 经 UNLOADING 排干 apply 半注册的资源(监听/服务/子插件),再进 Failed。
    /// 装配失败保持原子性(支柱 1);清理错误路由 ErrorSink 且并入终态错误
    ///(0.2.3:原始装载错误在前、回滚错误随后聚合——join 方拿到完整失败
    /// 画面,sink 仍是额外观察者)。
    async fn fail_load(this: &Arc<Self>, error: CordisError) {
        {
            let _admission = this.ctx.shared().admission.lock().unwrap();
            let mut tr = this.transition.lock().unwrap();
            Self::set_state(this, &mut tr, FiberState::Unloading);
        }
        this.flush_status();
        this.cancel_current();

        this.wait_events().await;

        let cleanup_errors = Self::drain_effects(this).await;
        let sink = this.ctx.error_sink();
        for e in &cleanup_errors {
            sink(e.clone());
        }
        let mut combined = vec![Arc::new(error)];
        combined.extend(cleanup_errors);
        let arc = aggregate_arcs(combined).expect("failure present");
        {
            let mut tr = this.transition.lock().unwrap();
            tr.error = Some(arc);
            Self::set_state(this, &mut tr, FiberState::Failed);
        }
        this.flush_status();
    }

    /// ④ EffectRecord 严格 LIFO 串行清理 + 消费边/依赖快照/提供表复位。
    /// 对照声明(审计):cordis 跨顶层 effect **并发**清理(`Promise.all`,
    /// fiber.ts:676),仅单 effect 内部 LIFO;此处跨 effect 也串行 LIFO——
    /// 完成顺序确定、错误聚合可预期,方向性强化而非语义缺失。
    async fn drain_effects(this: &Arc<Self>) -> Vec<Arc<CordisError>> {
        let handle = this.ctx.handle().clone();
        let effects: Vec<Arc<EffectRecord>> = std::mem::take(&mut *this.effects.lock().unwrap());
        let mut errors: Vec<Arc<CordisError>> = Vec::new();
        for record in effects.into_iter().rev() {
            if let Err(e) = record.drain(&handle).await {
                errors.push(e);
            }
        }
        // 已自摘记录的寄存错误并入(0.2.1):fiber 级观察等价于记录留在
        // 列表中被再次 join——单一错误同一 Arc,多错误进入聚合
        errors.extend(std::mem::take(&mut *this.drained_errors.lock().unwrap()));
        *this.last_deps.lock().unwrap() = None;
        this.provided.lock().unwrap().clear();
        errors
    }

    /// 卸载五步(D7):①标 unloading → ②cancel 当前代 → ③(驱动串行,apply 已退出)
    /// → ④EffectRecord LIFO 清理 → ⑤发布终态。
    async fn unload(this: &Arc<Self>, next: NextState) {
        {
            // Close dispatch admission and transition together. A snapshot
            // must either register its flight first or see Unloading.
            let _admission = this.ctx.shared().admission.lock().unwrap();
            let mut tr = this.transition.lock().unwrap();
            Self::set_state(this, &mut tr, FiberState::Unloading);
        }
        this.flush_status();

        // ② 取消当前代 token(插件后台任务经 ctx.cancelled() 协作退出)
        this.cancel_current();

        // Includes synchronous terminals with no listener cleanup record.
        this.wait_events().await;

        // ④ EffectRecord 严格 LIFO 串行清理
        let errors = Self::drain_effects(this).await;
        let err = aggregate_arcs(errors);

        match next {
            // 非终止卸载(restart/依赖刷新):清理错误路由 ErrorSink,
            // 不改变下一代状态(评审 P2:不得静默吞掉)
            NextState::Pending => {
                if let Some(e) = err {
                    (this.ctx.error_sink())(e);
                }
                let mut tr = this.transition.lock().unwrap();
                Self::set_state(this, &mut tr, FiberState::Pending);
            }
            NextState::Disposed => {
                let mut tr = this.transition.lock().unwrap();
                tr.error = err;
                Self::set_state(this, &mut tr, FiberState::Disposed);
            }
        }
        this.flush_status();
    }
}

/// fiber 驱动任务:意图串行处理(Loading→Unloading→Loading 不丢唤醒;
/// apply 与卸载天然互斥,跨代结果不会串染)。
pub(crate) async fn drive(this: Arc<FiberInner>, mut rx: mpsc::UnboundedReceiver<Intent>) {
    while let Some(intent) = rx.recv().await {
        let current_task = intent.task().cloned();
        let terminal_dispose = matches!(intent, Intent::Dispose) && !this.is_root;
        let shutdown_task = match &intent {
            Intent::Shutdown(task) => Some(task.clone()),
            _ => None,
        };
        let outcome = CatchUnwind::new(async {
            match intent {
                Intent::RefreshDeps => FiberInner::refresh_deps(&this).await,
                Intent::RefreshDepsJoin(task) => {
                    FiberInner::refresh_deps(&this).await;
                    complete_task(&this, task);
                }
                // 稳定性栅栏:FIFO 排到这里时,此前入队的意图已全部处理完。
                // 错误只认 Failed(红线):dispose 聚合错误经 dispose() 的
                // 任务通道返回,不从 settle 漏出。
                Intent::Settle(task) => {
                    let err = {
                        let tr = this.transition.lock().unwrap();
                        (tr.state == FiberState::Failed)
                            .then(|| tr.error.clone())
                            .flatten()
                    };
                    task.complete(err);
                }
                Intent::Restart(task) => {
                    if this.closing.load(Ordering::SeqCst)
                        || this.ctx.shared().closing.load(Ordering::SeqCst)
                    {
                        task.complete(Some(Arc::new(CordisError::Closed)));
                        return;
                    }
                    let state = this.state();
                    if state == FiberState::Disposed {
                        // 仅 root 可重启(§五 root_restart);换新代 token,
                        // 并清除终态任务:重启后的 dispose 必须真正再卸载一轮
                        if this.is_root {
                            this.new_generation_token();
                            {
                                let mut tr = this.transition.lock().unwrap();
                                tr.error = None;
                                tr.terminal_task = None;
                                FiberInner::set_state(&this, &mut tr, FiberState::Active);
                            }
                            this.flush_status();
                        }
                        complete_task(&this, task);
                        return;
                    }
                    if !matches!(state, FiberState::Pending) {
                        FiberInner::unload(&this, NextState::Pending).await;
                    }
                    FiberInner::refresh_deps(&this).await;
                    complete_task(&this, task);
                }
                Intent::Dispose => {
                    if this.closing.load(Ordering::SeqCst) {
                        this.wait_events().await;
                    }
                    if this.state() != FiberState::Disposed {
                        FiberInner::unload(&this, NextState::Disposed).await;
                    }
                    let (task, err) = {
                        let tr = this.transition.lock().unwrap();
                        (tr.terminal_task.clone(), tr.error.clone())
                    };
                    if let Some(task) = task {
                        let _ = task.done_tx.send(TaskDone::Done(err));
                    }
                }
                Intent::Shutdown(_) => {
                    this.wait_events().await;
                    let prior = if !this.is_root && this.state() == FiberState::Failed {
                        this.transition.lock().unwrap().error.clone()
                    } else {
                        None
                    };
                    if this.state() != FiberState::Disposed {
                        FiberInner::unload(&this, NextState::Disposed).await;
                    }
                    if let Some(prior) = prior {
                        let mut tr = this.transition.lock().unwrap();
                        tr.error =
                            aggregate_arcs(std::iter::once(prior).chain(tr.error.take()).collect());
                    }
                }
            }
            this.flush_status();
        })
        .await;
        if let Err(panic) = outcome {
            recover_driver_panic(&this, &mut rx, current_task, panic).await;
            return;
        }
        if let Some(task) = shutdown_task {
            this.alive.store(false, Ordering::SeqCst);
            // dispose() may have passed its closing check before shutdown was
            // posted, then registered terminal_task while Shutdown was queued.
            // Its Dispose intent carries no task, so complete the stored task.
            let (terminal, err) = {
                let tr = this.transition.lock().unwrap();
                (tr.terminal_task.clone(), tr.error.clone())
            };
            while let Ok(intent) = rx.try_recv() {
                let completion = match &intent {
                    // Settle reports Failed only; a clean terminal state is
                    // stable even if its request was queued after Shutdown.
                    Intent::Settle(_) => None,
                    // A second internal shutdown must observe the actual
                    // terminal result, including cleanup errors.
                    Intent::Shutdown(_) => err.clone(),
                    _ => Some(Arc::new(CordisError::Closed)),
                };
                complete_intent(&intent, completion);
            }
            if !this.is_root {
                // The mount disposer may join this internal result. Publish it
                // before release_transient drains that same mount record.
                task.complete(err.clone());
            }
            if !this.is_root {
                release_transient(&this).await;
            }
            if let Some(terminal) = terminal {
                terminal.complete(err.clone());
            }
            drop(this);
            task.complete(err);
            return;
        }
        if terminal_dispose {
            // 退出:先置 false(此后 post_join 的迟到投递自完成),
            // 单遍排空已入队的残留并完成其任务(评审 #2/#3)
            this.alive.store(false, Ordering::SeqCst);
            let terminal_error = this.transition.lock().unwrap().error.clone();
            while let Ok(intent) = rx.try_recv() {
                // Settle 只观察 Failed；其他 join 应拿到 dispose 的
                // 清理错误，而非被排空时伪报成功。
                let error = if matches!(intent, Intent::Settle(_)) {
                    None
                } else {
                    terminal_error.clone()
                };
                complete_intent(&intent, error);
            }
            // 瞬态残留释放(0.2.1):依赖声明注销 + parent mount 记录
            // drain(终态后清理幂等,记录 Done 自摘)。TaskDone 已发,
            // mount 清理里的 dispose() join 缓存终态,即刻完成。
            release_transient(&this).await;
            return; // 非 root 终态后驱动退出(句柄仍可 join 缓存终态)
        }
    }
}

/// 意外 panic 不能让驱动直接消失:撤销已注册资源,让当前及排队等待者
/// 都观察同一个终态错误。正常的 apply/validate panic 走 load 的回滚路径。
async fn recover_driver_panic(
    this: &Arc<FiberInner>,
    rx: &mut mpsc::UnboundedReceiver<Intent>,
    current_task: Option<Arc<TransitionTask>>,
    panic: Box<dyn Any + Send>,
) {
    this.cancel_current();
    {
        let _admission = this.ctx.shared().admission.lock().unwrap();
        let mut tr = this.transition.lock().unwrap();
        FiberInner::set_state(this, &mut tr, FiberState::Unloading);
    }
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| this.flush_status()));
    this.wait_events().await;
    let mut errors = vec![Arc::new(panic_error(panic))];
    let cleanup_errors = match CatchUnwind::new(FiberInner::drain_effects(this)).await {
        Ok(cleanup_errors) => cleanup_errors,
        Err(cleanup_panic) => vec![Arc::new(panic_error(cleanup_panic))],
    };
    let sink = this.ctx.error_sink();
    for error in &cleanup_errors {
        // A sink panic must not prevent the original waiters from settling.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(error.clone())));
    }
    errors.extend(cleanup_errors);
    let error = aggregate_arcs(errors).expect("driver panic present");
    let terminal_task = {
        let mut tr = this.transition.lock().unwrap();
        tr.error = Some(error.clone());
        FiberInner::set_state(this, &mut tr, FiberState::Failed);
        this.alive.store(false, Ordering::SeqCst);
        tr.terminal_task.clone()
    };
    if let Some(task) = current_task {
        task.complete(Some(error.clone()));
    }
    if let Some(task) = terminal_task {
        task.complete(Some(error.clone()));
    }
    while let Ok(intent) = rx.try_recv() {
        complete_intent(&intent, Some(error.clone()));
    }
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| this.flush_status()));
    if !this.is_root {
        let _ = CatchUnwind::new(release_transient(this)).await;
    }
}

/// 以 fiber 当前终态错误完成转换任务(join 者收到同一 `Arc`,D25)。
fn complete_task(this: &Arc<FiberInner>, task: Arc<TransitionTask>) {
    let err = {
        let tr = this.transition.lock().unwrap();
        tr.error.clone()
    };
    task.complete(err);
}

/// 终态 fiber 的瞬态残留释放(0.2.1,长寿 parent 下的瞬态子插件):
/// ①注销 `inject_index` 中的依赖声明(死 driver 不再收门控/重查通知);
/// ②drain parent 上的 mount 记录——清理闭包内的 `dispose()` 返回缓存
/// 终态即刻完成,EffectRecord 进 Done 后从 parent 的 effects 列表自摘。
/// parent 先行卸载(mem::take 整表清理)时两步均自然 no-op。
async fn release_transient(this: &Arc<FiberInner>) {
    let shared = this.ctx.shared().clone();
    shared
        .registry
        .unregister_injects(this, &this.declared_injects);
    let record = this.mount.lock().unwrap().take();
    if let Some(record) = record {
        let handle = this.ctx.handle().clone();
        if let Err(e) = record.drain(&handle).await {
            let sink = this.ctx.error_sink();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(e)));
        }
    }
    if let Some(parent) = this.parent_fiber.as_ref().and_then(Weak::upgrade) {
        let self_weak = Arc::downgrade(this);
        let mut children = parent.children.lock().unwrap();
        children.retain(|child| !Weak::ptr_eq(child, &self_weak) && child.strong_count() > 0);
        if children.capacity() > 64 && children.len() * 4 < children.capacity() {
            children.shrink_to_fit();
        }
    }
}

pub(crate) fn spawn_fiber(
    shared: &Arc<Shared>,
    parent_ctx: Option<&Ctx>,
    plugin: Option<Arc<dyn Plugin>>,
    is_root: bool,
) -> Arc<FiberInner> {
    spawn_fiber_inner(shared, parent_ctx, plugin, None, None, is_root)
}

/// 工厂模式 spawn(D32:`Ctx::plugin_with` 入口)。
pub(crate) fn spawn_factory_fiber<F, C>(
    shared: &Arc<Shared>,
    parent_ctx: Option<&Ctx>,
    factory: F,
    config: C,
    is_root: bool,
) -> Arc<FiberInner>
where
    F: PluginFactory<C>,
    C: Send + Sync + 'static,
{
    let erased: Arc<dyn ErasedFactory> = Arc::new(FactoryAdapter {
        factory,
        _marker: std::marker::PhantomData,
    });
    // 依赖声明静态(D32f):spawn 注册一次,终身不变——由
    // spawn_fiber_inner 从擦除面一次性读取并注册,与静态模式完全同形。
    let boxed: Arc<dyn Any + Send + Sync> = Arc::new(config);
    spawn_fiber_inner(shared, parent_ctx, None, Some(erased), Some(boxed), is_root)
}

fn spawn_fiber_inner(
    shared: &Arc<Shared>,
    parent_ctx: Option<&Ctx>,
    plugin: Option<Arc<dyn Plugin>>,
    factory: Option<Arc<dyn ErasedFactory>>,
    config: Option<Arc<dyn Any + Send + Sync>>,
    is_root: bool,
) -> Arc<FiberInner> {
    let is_closed = || {
        !is_root
            && (shared.closing.load(Ordering::SeqCst)
                || parent_ctx
                    .is_none_or(|p| p.subtree_closing() || p.weak_fiber().upgrade().is_none()))
    };
    let known_closed = is_closed();
    // Capture user metadata before taking the root admission lock.
    let name = if known_closed {
        "closed".to_string()
    } else {
        plugin
            .as_ref()
            .map(|p| p.name().to_string())
            .or_else(|| factory.as_ref().map(|f| f.name().to_string()))
            .unwrap_or_else(|| "root".to_string())
    };
    let declared_injects: Vec<TypeKey> = if known_closed {
        Vec::new()
    } else if let Some(plugin) = &plugin {
        plugin.injects().to_vec()
    } else if let Some(factory) = &factory {
        factory.injects().to_vec()
    } else {
        Vec::new()
    };
    let _admission = shared.admission.lock().unwrap();
    let closed = is_closed();
    let (plugin, factory, config, declared_injects) = if closed {
        (None, None, None, Vec::new())
    } else {
        (plugin, factory, config, declared_injects)
    };
    let id = PluginId(
        shared
            .next_plugin_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
    );
    let instance = InstanceId::allocate();
    let initial = if closed {
        FiberState::Disposed
    } else if is_root {
        FiberState::Active
    } else {
        FiberState::Pending
    };
    let closed_error = closed.then(|| Arc::new(CordisError::Closed));
    let closed_task = closed.then(|| {
        let task = TransitionTask::new();
        task.complete(closed_error.clone());
        task
    });
    let (snapshot_tx, snapshot_rx) = watch::channel(Snapshot {
        generation: 0,
        state: initial,
        error: closed_error.clone(),
    });
    let (event_flights_tx, event_flights_rx) = watch::channel(0);
    let (tx, rx) = mpsc::unbounded_channel();
    let token = CancellationToken::new();
    let closing = Arc::new(AtomicBool::new(closed));
    let parent_fiber = parent_ctx.map(|p| p.weak_fiber());

    let this = Arc::new_cyclic(|weak: &Weak<FiberInner>| {
        let ctx = match parent_ctx {
            Some(parent) => Ctx::new_child(
                shared.clone(),
                parent,
                weak.clone(),
                None,
                instance,
                id,
                closing.clone(),
            ),
            None => Ctx::new_root(shared.clone(), weak.clone(), instance, id, closing.clone()),
        };
        FiberInner {
            id,
            instance,
            name: if closed { "closed".to_string() } else { name },
            is_root,
            plugin,
            factory,
            config: Mutex::new(config),
            ctx,
            parent_fiber,
            children: Mutex::new(Vec::new()),
            closing,
            event_flights: Mutex::new(0),
            event_flights_tx,
            _event_flights_rx: event_flights_rx,
            token: Mutex::new(token),
            transition: Mutex::new(Trans {
                generation: 0,
                state: initial,
                error: closed_error,
                seq: 0,
                status_queue: Vec::new(),
                terminal_task: closed_task,
            }),
            snapshot_tx: snapshot_tx.clone(),
            snapshot_rx,
            effects: Mutex::new(Vec::new()),
            effect_index: Mutex::new(Vec::new()),
            drained_errors: Mutex::new(Vec::new()),
            mount: Mutex::new(None),
            declared_injects,
            intents_tx: tx.clone(),
            alive: AtomicBool::new(!closed),
            provided: Mutex::new(Vec::new()),
            last_deps: Mutex::new(None),
            accesses: Mutex::new(Vec::new()),
            shutdown_task: Mutex::new(None),
            shutdown_inner: Mutex::new(None),
            driver: Mutex::new(None),
        }
    });

    if !closed {
        if let Some(parent) = this.parent_fiber.as_ref().and_then(Weak::upgrade) {
            parent.children.lock().unwrap().push(Arc::downgrade(&this));
        }
    }

    if !closed {
        for key in &this.declared_injects {
            // A foreign instance can never enter this fiber's ancestry, so
            // changes to that key cannot make its Pending gate succeed.
            if this.ctx.in_instance_key(key) {
                shared.registry.register_inject(key.clone(), &this);
            }
        }
        let driver = shared.handle.spawn(drive(this.clone(), rx));
        *this.driver.lock().unwrap() = Some(driver);
    }
    this
}

/// fiber 句柄:注册返回,`IntoFuture` = 等待进入稳定态(启动错误经 Err 返回)。
pub struct FiberView {
    /// 插件身份(D10)。
    pub id: PluginId,
    pub(crate) inner: Arc<FiberInner>,
}

impl Clone for FiberView {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            inner: self.inner.clone(),
        }
    }
}

impl FiberView {
    pub(crate) fn from_inner(inner: Arc<FiberInner>) -> Self {
        Self {
            id: inner.id,
            inner,
        }
    }

    /// 当前快照。
    pub fn state(&self) -> Snapshot {
        self.inner.state_snapshot()
    }

    /// Current cleanup ownership tree for this fiber. Records being drained
    /// remain visible until their cleanup completes. This is a point-in-time
    /// read of metadata only; it never calls user code.
    pub fn effects(&self) -> Vec<EffectMeta> {
        let records = self.inner.effect_index.lock().unwrap().clone();
        records
            .into_iter()
            .filter_map(|entry| entry.upgrade())
            .filter_map(|record| record.snapshot())
            .collect()
    }

    /// 插件显示名。
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Take this fiber's completed, early-disposed effect errors. Taken
    /// errors are excluded from a later unload result; see
    /// [`Ctx::take_cleanup_errors`].
    pub fn take_cleanup_errors(&self) -> Vec<Arc<CordisError>> {
        std::mem::take(&mut *self.inner.drained_errors.lock().unwrap())
    }

    /// Permanently close this fiber and its descendants. Admission closes at
    /// the call site; dropping the returned future does not stop cleanup.
    /// A callback or finalizer must not await shutdown of its own subtree.
    pub fn shutdown(&self) -> BoxFuture<'static, Result<(), Arc<CordisError>>> {
        if self.inner.is_root {
            return self.inner.ctx.shutdown();
        }
        let shared = self.inner.ctx.shared().clone();
        let task = {
            let _admission = shared.admission.lock().unwrap();
            let mut pending = vec![self.inner.clone()];
            while let Some(fiber) = pending.pop() {
                fiber.closing.store(true, Ordering::SeqCst);
                fiber.cancel_current();
                pending.extend(
                    fiber
                        .children
                        .lock()
                        .unwrap()
                        .iter()
                        .filter_map(Weak::upgrade),
                );
            }
            begin_subtree_shutdown_locked(&self.inner)
        };
        Box::pin(async move { join_task(&task).await })
    }

    /// 订阅状态变化(watch:last-value,晚订阅先 `borrow()` 再 `changed()`,D6)。
    pub fn watch(&self) -> watch::Receiver<Snapshot> {
        self.inner.snapshot_rx.clone()
    }

    /// dispose:恰好一次;重复/并发调用 join 同一 `Arc<CordisError>`(D6/D20)。
    /// 入队前预取消当前代 token(运行中的 apply 协作退出,D27 第②步前置)。
    pub fn dispose(&self) -> BoxFuture<'static, Result<(), Arc<CordisError>>> {
        if self.inner.is_root && self.inner.ctx.shared().closing.load(Ordering::SeqCst) {
            let task = self
                .inner
                .ctx
                .shared()
                .shutdown_task
                .lock()
                .unwrap()
                .clone();
            if let Some(task) = task {
                return Box::pin(async move { join_task(&task).await });
            }
        }
        let task = match self.register_dispose(|_| Ok(())) {
            Ok(task) => task,
            Err(_) => unreachable!("an unconditional dispose is always registered"),
        };
        Box::pin(async move { join_task(&task).await })
    }

    /// Register the dispose (or join the one registered) if `check` accepts
    /// the transition state. Check and registration share one lock: no new
    /// generation can start between them, and a restart that comes after
    /// sees the terminal task and is refused.
    pub(crate) fn register_dispose(
        &self,
        check: impl FnOnce(&Trans) -> Result<(), CordisError>,
    ) -> Result<Arc<TransitionTask>, CordisError> {
        // 登记在调用点同步完成(评审 #2):dispose() 返回后,并发的
        // restart() 立刻可见终态任务并拒绝,不依赖本 future 被 poll
        let (task, newly_registered) = {
            let mut tr = self.inner.transition.lock().unwrap();
            check(&tr)?;
            match &tr.terminal_task {
                Some(task) => (task.clone(), false),
                None => {
                    self.inner.cancel_current();
                    let task = TransitionTask::new();
                    tr.terminal_task = Some(task.clone());
                    (task, true)
                }
            }
        };
        if newly_registered {
            self.inner.post(Intent::Dispose);
            if !self.inner.alive.load(Ordering::SeqCst) {
                task.complete(self.inner.stopped_error());
            }
        }
        Ok(task)
    }

    /// 限制等待时间，不强制终止正在执行的插件或清理任务。
    /// 超时后再次调用 `dispose()` 可继续等待同一终态任务及其错误。
    pub fn dispose_with_timeout(
        &self,
        limit: Duration,
    ) -> BoxFuture<'static, Result<(), DisposeWaitError>> {
        let pending = self.dispose();
        let inner = self.inner.clone();
        Box::pin(async move {
            let started = Instant::now();
            match tokio::time::timeout(limit, pending).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(DisposeWaitError::Failed(error)),
                Err(_) => {
                    let snapshot = inner.state_snapshot();
                    Err(DisposeWaitError::TimedOut {
                        plugin_id: inner.id,
                        generation: snapshot.generation,
                        state: snapshot.state,
                        elapsed: started.elapsed(),
                    })
                }
            }
        })
    }

    /// restart:干净卸载后重装配(无状态迁移,Erlang code_change 省略版)。
    /// 已 Disposed(或 Dispose 已入队/驱动已退出)的非 root fiber返回
    /// `InactiveEffect`——不得让 join 永等(评审 #2)。
    /// 经转换任务 join:等待本次 restart 排干并回传终态错误。
    pub fn restart(&self) -> BoxFuture<'static, Result<(), Arc<CordisError>>> {
        let this = self.inner.clone();
        Box::pin(async move {
            {
                let tr = this.transition.lock().unwrap();
                if this.ctx.shared().closing.load(Ordering::SeqCst)
                    || this.closing.load(Ordering::SeqCst)
                {
                    return Err(Arc::new(CordisError::Closed));
                }
                if !this.is_root
                    && (tr.terminal_task.is_some() || !this.alive.load(Ordering::SeqCst))
                {
                    return Err(Arc::new(CordisError::InactiveEffect));
                }
            }
            this.cancel_current(); // 预取消:运行中的 apply 协作退出
            let task = TransitionTask::new();
            this.post_join(task.clone(), Intent::Restart);
            join_task(&task).await
        })
    }

    /// 配置热更新(D32):dry-run 通过后存入并按状态矩阵重启。
    ///
    /// - dry-run = `validate_config` + `build` + 实例 `validate`,任一失败
    ///   返回 Err,**不存不重启**(现状不动);
    /// - 通过后存 config,经 `Intent::Restart` 收敛:Active/Loading/Failed
    ///   → 卸载重载(新 config 构造新实例);Pending → 直接重查,依赖就绪
    ///   即用新 config 装载,未就绪则存着等门控;
    /// - 非工厂 fiber(静态 `plugin()` 装载)返回 `Validation` 错;
    /// - 与 restart 同款终态拒绝(Disposed/终态已登记/驱动已退出)。
    pub fn update<C: Send + Sync + 'static>(
        &self,
        new_config: C,
    ) -> BoxFuture<'static, Result<(), Arc<CordisError>>> {
        let this = self.inner.clone();
        Box::pin(async move {
            {
                let tr = this.transition.lock().unwrap();
                if this.ctx.shared().closing.load(Ordering::SeqCst)
                    || this.closing.load(Ordering::SeqCst)
                {
                    return Err(Arc::new(CordisError::Closed));
                }
                if !this.is_root
                    && (tr.terminal_task.is_some() || !this.alive.load(Ordering::SeqCst))
                {
                    return Err(Arc::new(CordisError::InactiveEffect));
                }
            }
            let factory = this
                .factory
                .as_ref()
                .and_then(|f| (f.config_type_id() == TypeId::of::<C>()).then(|| f.clone()));
            let Some(factory) = factory else {
                let reason = if this.factory.is_some() {
                    "config type mismatch"
                } else {
                    "fiber has no factory (static plugin cannot update)"
                };
                return Err(Arc::new(CordisError::Validation {
                    issues: vec![reason.into()],
                }));
            };
            // dry-run(D32b):validate_config → build → 实例 validate,产物丢弃
            // (build 纯构造契约)。用户回调 panic 转错误不杀调用方。
            // 对照声明(审计):cordis 在非 ACTIVE 态**延迟** config 校验到激活时
            // (fiber.ts:739 只存 `_config`);此处无条件 dry-run(D32b,尽早暴露)。
            // 通过后:二次终态检查(dry-run 无锁同步,期间并发 dispose 可完整
            // 执行,评审 3 探针实测窗口存在)→ 存 config → restart。
            // 依赖声明静态(D32f),update 不触碰注册表。
            let boxed: Arc<dyn Any + Send + Sync> = Arc::new(new_config);
            let dry = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                factory.validate_config_erased(boxed.as_ref())?;
                let instance = factory.build_erased(boxed.as_ref())?;
                instance.validate()?;
                Ok::<(), CordisError>(())
            }));
            match dry {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(Arc::new(e)),
                Err(p) => return Err(Arc::new(panic_error(p))),
            }
            // 二次终态检查:dry-run 期间并发 dispose 可能已登记 terminal_task
            // 并完成卸载——此时不存,现状交由 dispose 收敛(评审 3 探针:dry-run
            // 后存 config 会落进已死 fiber)。
            {
                let tr = this.transition.lock().unwrap();
                if this.ctx.shared().closing.load(Ordering::SeqCst)
                    || this.closing.load(Ordering::SeqCst)
                {
                    return Err(Arc::new(CordisError::Closed));
                }
                if !this.is_root
                    && (tr.terminal_task.is_some() || !this.alive.load(Ordering::SeqCst))
                {
                    return Err(Arc::new(CordisError::InactiveEffect));
                }
            }
            *this.config.lock().unwrap() = Some(boxed);
            this.cancel_current(); // 预取消:运行中的 apply 协作退出(同 restart)
            let task = TransitionTask::new();
            this.post_join(task.clone(), Intent::Restart);
            join_task(&task).await
        })
    }

    /// 当前 config 快照(D32,诊断用)。非工厂 fiber 或类型不符返回 None。
    pub fn current_config<C: Send + Sync + 'static>(&self) -> Option<Arc<C>> {
        let boxed = self.inner.config.lock().unwrap().clone()?;
        boxed.downcast::<C>().ok()
    }

    /// The identity of this fiber, as its contexts report in
    /// [`Ctx::instance`]; use it to address instance events to the plugin.
    pub fn instance(&self) -> crate::InstanceId {
        self.inner.instance
    }

    /// Store `new_config` without restarting: the running instance keeps
    /// going and later loads (restart, dependency reload) build from the new
    /// config. Only `validate_config` runs. For changes the plugin applies
    /// in place, such as cordis's volatile fields; everything else should
    /// use [`FiberView::update`]. Refused like `update` for closed or
    /// disposed fibers, static plugins and a mismatched config type.
    pub fn set_config<C: Send + Sync + 'static>(
        &self,
        new_config: C,
    ) -> Result<(), Arc<CordisError>> {
        let this = &self.inner;
        {
            let tr = this.transition.lock().unwrap();
            if this.ctx.shared().closing.load(Ordering::SeqCst)
                || this.closing.load(Ordering::SeqCst)
            {
                return Err(Arc::new(CordisError::Closed));
            }
            if !this.is_root && (tr.terminal_task.is_some() || !this.alive.load(Ordering::SeqCst)) {
                return Err(Arc::new(CordisError::InactiveEffect));
            }
        }
        let factory = this
            .factory
            .as_ref()
            .filter(|f| f.config_type_id() == TypeId::of::<C>())
            .ok_or_else(|| {
                Arc::new(CordisError::Validation {
                    issues: vec![if this.factory.is_some() {
                        "config type mismatch".into()
                    } else {
                        "fiber has no factory (static plugin has no config)".into()
                    }],
                })
            })?;
        let boxed: Arc<dyn Any + Send + Sync> = Arc::new(new_config);
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            factory.validate_config_erased(boxed.as_ref())
        })) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(Arc::new(e)),
            Err(p) => return Err(Arc::new(panic_error(p))),
        }
        *this.config.lock().unwrap() = Some(boxed);
        Ok(())
    }
}

/// Called with the root admission lock held, after every member has been
/// marked closing. Parent ownership is captured before any child can detach.
fn begin_subtree_shutdown_locked(fiber: &Arc<FiberInner>) -> Arc<TransitionTask> {
    let mut slot = fiber.shutdown_task.lock().unwrap();
    if let Some(task) = slot.as_ref() {
        return task.clone();
    }
    let public = TransitionTask::new();
    *slot = Some(public.clone());
    drop(slot);

    let children: Vec<Arc<FiberInner>> = fiber
        .children
        .lock()
        .unwrap()
        .iter()
        .filter_map(Weak::upgrade)
        .collect();
    let child_tasks: Vec<Arc<TransitionTask>> =
        children.iter().map(begin_subtree_shutdown_locked).collect();

    let (inner, needs_post) = {
        let mut tr = fiber.transition.lock().unwrap();
        match tr.terminal_task.as_ref() {
            Some(task) => (task.clone(), false),
            None => {
                let task = TransitionTask::new();
                tr.terminal_task = Some(task.clone());
                (task, true)
            }
        }
    };
    *fiber.shutdown_inner.lock().unwrap() = Some(inner.clone());
    if needs_post {
        fiber.post(Intent::Shutdown(inner.clone()));
    }
    let owner = fiber.clone();
    let result = public.clone();
    fiber.ctx.handle().spawn(async move {
        let mut errors = Vec::new();
        if let Err(error) = join_task(&inner).await {
            errors.push(error);
        }
        let driver = { owner.driver.lock().unwrap().take() };
        if let Some(driver) = driver {
            if let Err(error) = driver.await {
                errors.push(Arc::new(crate::error::join_panic_error(error)));
            }
        }
        for child in child_tasks {
            if let Err(error) = join_task(&child).await {
                errors.push(error);
            }
        }
        drop(owner);
        result.complete(aggregate_arcs(errors));
    });
    public
}

pub(crate) async fn join_task(task: &Arc<TransitionTask>) -> Result<(), Arc<CordisError>> {
    let mut rx = task.done_tx.subscribe();
    loop {
        match &*rx.borrow() {
            TaskDone::Done(err) => return err.clone().map_or(Ok(()), Err),
            TaskDone::Running => {}
        }
        if rx.changed().await.is_err() {
            // 发送端随 fiber 析构:不得伪装成功(评审 #8)
            return Err(Arc::new(CordisError::PluginFailed(
                "fiber dropped before transition completed".into(),
            )));
        }
    }
}

/// settle(`IntoFuture`)= Settle 栅栏 + join:"稳定了吗"由 mailbox 的 FIFO
/// 顺序直接回答——Settle 排到时,此前入队的意图(初始装载、再装载通知、
/// restart)都已处理完;错误只认 Failed(dispose 聚合错误经 dispose() 返回)。
fn settle(this: &FiberInner) -> BoxFuture<'static, Result<(), Arc<CordisError>>> {
    let task = TransitionTask::new();
    this.post_join(task.clone(), Intent::Settle);
    Box::pin(async move { join_task(&task).await })
}

impl std::future::IntoFuture for FiberView {
    type Output = Result<(), Arc<CordisError>>;
    type IntoFuture = BoxFuture<'static, Result<(), Arc<CordisError>>>;

    fn into_future(self) -> Self::IntoFuture {
        settle(&self.inner)
    }
}

impl std::future::IntoFuture for &FiberView {
    type Output = Result<(), Arc<CordisError>>;
    type IntoFuture = BoxFuture<'static, Result<(), Arc<CordisError>>>;

    fn into_future(self) -> Self::IntoFuture {
        settle(&self.inner)
    }
}

#[cfg(test)]
mod transient_tests;
