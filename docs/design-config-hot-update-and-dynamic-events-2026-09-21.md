# 设计:配置热更新(D32)与动态事件键(D33)

> 2026-09-21。范围:两件独立功能,可并行实施、独立提交、独立回滚。
> D32 = cordis `fiber.update(config)` 的 rutis 化(M4 核心);D33 = TypeKey 限定名放宽 + 事件总线 keyed 面 + 桥事件链路(即旧路线的 M3 事件链路)。
> 参照物:dsh fork 的懒 config 解析(vendor README 修改 #15,port cordiverse/cordis#41)与 `internal/update` 语义;上游 cordis 字符串事件。

## 〇 动机

1. **D32**:cordis 生产最依赖的能力——运行中改插件配置、自动重启生效。dsh 的整条"改 YAML → 插件热更新"链路建立在它上面。rutis 现状:config 烘死在实例里(D19),改配置只能卸载重装。
2. **D33**:dsh 宿主经 `evt/emit` 发来的事件,名字是运行时字符串(`session/event`、`agent/*`),Rust 侧目前只能打 stderr 日志(rutis_dsh.rs:136),无法按名字订阅。事件总线注册面键是裸 `TypeId`(bus.rs:184/189/231/238),同类型多通道不存在。

两者正交:动态插件装载(loader)需要的"名字→工厂"注册表**不在本设计内**(见 §五)。

---

## 一 D32 配置热更新

### 1.1 语义总表(cordis → rutis 映射)

| cordis 语义 | rutis 映射 | 成本 |
|---|---|---|
| `fiber.update(config)`:validate 后重启 | 存 config + 复用 `Intent::Restart`,现有状态机保证恰好一次清理、等清理完再重载 | 零新事务逻辑 |
| config 可引用注入服务,注入激活后才解析(fork `internal/config` waterfall) | 工厂是纯函数(config→实例,不接 ctx);**服务引用在 apply 内解析**——apply 已在依赖门控之后,时序等价 | 不需要 internal/config 对应物 |
| provider 替换 → config 重新解析 | 依赖驱动重载已实现:unload 清 last_deps → load 重新走工厂 = 用当前 config 重造实例 | 免费 |
| pending update 保留 raw config | config 存 fiber,Pending 态等门控放行时自然用新值 load | 免费 |
| update 前校验失败不动现状 | dry-run(见 D32b)失败返回 Err,不存不重启 | 新增 |
| `internal/update` waterfall(持久化钩子 noSave) | 不做;宿主层监听 `FiberStatusChanged` + 自管持久化 | 明确裁剪 |

### 1.2 状态矩阵(update 在各态的行为)

统一路径:**存 config → cancel_current → post_join(Restart) → join**。

| 当前态 | 行为 | 依据 |
|---|---|---|
| Active | unload(清 last_deps)→ refresh_deps → load(新 config) | Restart 分支(fiber.rs:505-508) |
| Loading | cancel 当前代 → apply 协作退出 → unload → Pending → load(新 config) | 同上 |
| Pending | 跳过 unload,refresh_deps:依赖满足则 load(新 config);不满足则继续 Pending,**config 已存,provider 到位后自动用新值** | Restart 分支(fiber.rs:505) |
| Failed | 同 Active 路径(restart 从 Failed 重载,新 config 生效)——修好配置热修复 Failed 插件 | 同上 |
| Unloading(进行中) | 排队等驱动串行处理,收敛同上 | mailbox FIFO |
| Disposed / terminal_task 已登记 / 驱动已退出 | 拒绝 `InactiveEffect` | 同 restart 现有检查(fiber.rs:685-688) |

**等值合并不吃掉 update**:unload 清 last_deps,refresh_deps 在 `last_deps == None` 时必不等值、必 load(fiber.rs:305-315)。config 不参与依赖身份(D32e),其变化经 restart 的 generation+1 体现;依赖该 fiber 服务的消费者看 provider_gen,自动驱逐重载——免费。

### 1.3 API 草案

```rust
// plugin.rs 新增(D32f 修订后)
/// 工厂:每代从当前 config 构造插件实例。build 必须是纯构造(D32b 契约):
/// 无副作用或幂等——dry-run 与 load 各调用一次。
pub trait PluginFactory<C: Send + Sync + 'static>: Send + Sync + 'static {
    /// 依赖门控声明(静态,spawn 注册一次终身不变,与 Plugin::injects 对称;
    /// 初版为 injects(&config) 派生,由 D32f 撤销,见 §十一)。
    fn injects(&self) -> &[TypeKey] { &[] }
    /// config 级校验(不构造实例)。
    fn validate_config(&self, _config: &C) -> Result<(), CordisError> { Ok(()) }
    /// 构造插件实例。失败 = config 无法产出可用实例。
    fn build(&self, config: &C) -> Result<Box<dyn Plugin>, CordisError>;
}

// ctx.rs 新增(plugin() 现有路径不变,零迁移成本)
impl Ctx {
    pub fn plugin_with<C: Send + Sync + 'static>(
        &self, factory: impl PluginFactory<C>, config: C,
    ) -> FiberView;

    /// 闭包便捷形态(等价于单方法工厂)。
    pub fn plugin_from<C: Send + Sync + 'static>(
        &self,
        build: impl Fn(&C) -> Result<Box<dyn Plugin>, CordisError> + Send + Sync + 'static,
        config: C,
    ) -> FiberView;
}

// fiber.rs FiberView 新增
impl FiberView {
    /// 热更新:dry-run 通过后存入并重启。join 返回 restart 终态。
    pub fn update<C: Send + Sync + 'static>(
        &self, new_config: C,
    ) -> BoxFuture<'static, Result<(), Arc<CordisError>>>;

    /// 当前 config 快照(诊断用;类型不符返回 None)。
    pub fn current_config<C: Send + Sync + 'static>(&self) -> Option<Box<C>>;
}
```

### 1.4 内部形态

- `FiberInner` 增两字段:
  ```rust
  factory: Option<Arc<dyn ErasedFactory>>,          // 工厂模式才有
  config: Mutex<Option<Box<dyn Any + Send + Sync>>>, // 同上
  ```
- `ErasedFactory`(内部):`config_type_id() / injects_erased(&dyn Any) -> Vec<TypeKey> / validate_config_erased(&dyn Any) / build_erased(&dyn Any) -> Result<Box<dyn Plugin>>`。泛型 `PluginFactory<C>` 由包装结构擦除实现(同 registry 的 `StoredValue` 手法)。
- **两形态并存**(D32a):`plugin()` 静态模式(`FiberInner.plugin: Some(..)`)不动;工厂模式 `plugin` 字段为 None。`load()` 取实例改为 `this.current_plugin()`:静态模式克隆既有实例,工厂模式 `factory.build(当前 config)`(失败走 `fail_load`,保持装配失败原子性)。validate/apply 下游路径完全复用。
- **update 流程**(1.2 矩阵的展开):
  1. transition 锁内检查 terminal_task / Disposed / 驱动退出 → 拒绝;
  2. `TypeId::of::<C>()` 与 `factory.config_type_id()` 匹配,不符 → `Validation` 错;
  3. dry-run:`validate_config(&new)` → `build(&new)` → 实例 `validate()`(D32b);任一失败返回 Err,**不存不重启**;
  4. 存入 config;
  5. `cancel_current()` + `post_join(Restart)` + `join_task`。

### 1.5 并发语义

| 场景 | 收敛 |
|---|---|
| update × update | mailbox FIFO,后者覆盖 config;两个 join 各自等到自己的 Restart 处理完 |
| update × dispose | terminal_task 先登记则 update 拒绝；update 先登记时，dispose 仍阻止 restart 尚未准入的新代，并取消已准入代的 token，等待旧代清理后完成卸载 |
| update × 驱逐(RefreshDepsJoin) | mailbox 序决定先后;两者都收敛到"用当前 config 重载",无竞态窗口 |
| update × 运行中 apply | cancel_current 协作退出(与 restart 现有语义一致,D27 限制照旧:不观察 token 的 apply 无限等待) |

### 1.6 决策点

- **D32a 双形态并存**:`plugin()` 不变(零迁移、115 测试零改动);统一为 identity 工厂留后续简化批次评估。不做进本期。
- **D32b dry-run = `validate_config` + `build` + 实例 `validate`**,造出的实例丢弃;`build` 纯构造是工厂契约(违反 = dry-run 与实跑不一致,自担)。错误尽早暴露,避免重启到一半才发现 config 造不出实例;代价是 build 跑两次。
- **D32c 状态矩阵**如 1.2(对齐 cordis + fork 的非 ACTIVE 态行为)。
- **D32d 不做 `internal/update` waterfall**:持久化归宿主层。`noSave` 语义无对应物。
- **D32e config 不进依赖身份**:变化经 restart 换代体现,消费者看 provider_gen。与 cordis 四元组语义一致。

### 1.7 测试计划(契约测试)

1. Active 态 update:新 config 生效(提供的服务值变)、generation+1、旧代清理恰好一次(LIFO 顺序断言);
2. dry-run 失败三种(validate_config / build / 实例 validate):返回 Err,状态与服务不变;
3. Pending 态 update:依赖未到时 update,provider 到位后装载用**新** config;
4. Failed 态 update:修复配置后重启成功回 Active;
5. update 后消费者重载:provider update → 依赖它的消费者自动驱逐并用其当前 config 重载;
6. 并发 update × dispose / update × update(FIFO、join 均正确落定);
7. 工厂模式 `injects`(静态声明)门控:依赖缺失时 Pending,不 apply;
8. config 类型不匹配的 update 报 Validation(测试实际捆了"静态 fiber 拒绝"断言——同为 Validation 路径的设计行为);
9. 依赖驱动重载用当前 config 重造实例(provider 摘除 → 重提供 → 重载仍用当前 config;update 后再驱逐则用新 config);
10. `current_config` 快照与类型不符路径。

**评审增补(§八 之 2)**:11. Loading 态 update(apply 运行中,协作取消后重载新 config);12. Unloading 态 update(慢清理中排队,收敛后重新提供依赖装载新 config);13. update × 驱逐并发(provider 换代与消费者热更新并发,双方收敛);15. build panic → Failed 且驱动存活/join 不挂起。
**第二轮增补(§九)与第三轮增补(§十)的 14/16/17/18(injects 漂移/panic/恢复族)随 D32f 撤销一并删除**——它们测的是被删机制的缺陷类,机制不存在则缺陷类不存在(§十一)。

---

## 二 D33 动态事件键

### 2.1 现状与思路

bus 注册面键是裸 `TypeId`:`hooks: HashMap<TypeId, ..>`、`wf_hooks`、`dispatch_tail`(bus.rs:107-113)。思路:**不抄 cordis 的字符串事件面**(四分发做一套字符串版本 = 语义面翻倍、类型安全丢失),而是把服务键已有的 TypeKey 机制搬进事件总线——限定名支持动态字符串后,一个类型化事件类型 + 动态键即可承载"运行时才知道名字的事件",四分发、fiber 生命周期清理、once/prepend 全部免费继承。

### 2.2 TypeKey 限定名放宽(key.rs)

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Qualifier {
    Static(&'static str),   // 现有路径,零分配
    Dynamic(Arc<str>),      // 运行时名字
}

pub struct TypeKey {
    type_id: TypeId,
    qualifier: Option<Qualifier>,
}

impl TypeKey {
    pub fn keyed<T: ?Sized + 'static>(qualifier: &'static str) -> Self;          // 不变,走 Static
    pub fn keyed_dynamic<T: ?Sized + 'static>(name: impl Into<Arc<str>>) -> Self; // 新增,走 Dynamic
}
```

- Hash/Eq 按字符串内容:Static("foo") == Dynamic("foo"),互查互通;
- **放弃 Cow / 全量 Arc<str> 的理由**:Cow<String> 每次克隆 memcpy、Arc<str> 从静态串构造也要分配;双轨 enum 让现有 `keyed::<T>("name")` / `Key<T>` 常量零变化零分配,只有动态路径付 Arc 成本;
- **TypeKey 失去 `Copy`**(enum 含 Arc)。受影响点:`provided` 遍历(fiber.rs:372-378 的 `map(|(k,_)| *k)`)、`notify_key_changed` 传值等改 `clone()`。TypeKey 克隆在装载/注册路径,频率低,可接受。逐点清点在实施时做,`clippy -D warnings` 保证无漏网。

### 2.3 事件总线 keyed 面(bus.rs / event.rs)

`BusInner` 三个注册面键 `TypeId` → `TypeKey`(hooks / wf_hooks / dispatch_tail)。**现有 API 签名一律不动**(115 项测试零改动),新增 keyed 变体:

```rust
impl EventBus {
    // 注册面
    pub fn on_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                              l: impl Listener<E>) -> Result<Disposer, CordisError>;
    pub fn on_keyed_opt<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                                  l: impl Listener<E>, opts: EventOptions) -> Result<Disposer, CordisError>;
    pub fn once_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                                l: impl Listener<E>) -> Result<Disposer, CordisError>;
    pub fn on_waterfall_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                                        l: impl WaterfallListener<E>) -> Result<Disposer, CordisError>;

    // 分发面(四语义全套;签名与非 keyed 对齐,多 name 参数)
    pub fn emit_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>, e: Arc<E>);
    pub async fn parallel_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                                          e: Arc<E>) -> Result<(), CordisError>;
    pub async fn serial_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                                        e: &E) -> Result<Option<E::Value>, CordisError>;
    pub fn waterfall_keyed<'a, E: Event, T: Terminal<E> + 'a>(
        &self, ctx: &'a Ctx, name: impl Into<Arc<str>>, e: &'a E, terminal: T,
    ) -> BoxFuture<'a, Result<E::Value, CordisError>>;
}
```

- 键构造即烙类型:`keyed_dynamic::<E>(name)` 的 TypeId 来自 E;类型隔离由键内嵌的 type_id 保证——**错位键无法经公开 API 构造**(TypeKey 字段私有),监听器适配层的 downcast mismatch 兜底实际不可达(评审 2 注:设计草案此处的"take_hooks 校验 type_id fail-fast"为防御层高估,实际机制即此);
- keyed 变体内部一律转 TypeKey 走同一条注册/快照/派发代码路径(与非 keyed 共享实现,非 keyed = qualifier None 的特例),不复制四套逻辑;
- **D31 尾链键跟随 TypeKey**:同名(跨 Static/Dynamic 拼写)同类型共享一条派发链,跨名不保序——边界声明照旧。

### 2.4 HostEvent 与桥事件链路(rutis-cordis / rutis-dsh)

HostEvent 定义在 **rutis-cordis**(内核零 serde 红线不破):

```rust
/// 宿主透传事件:evt/emit 翻译产物。name 冗余存于载荷(qualifier 在注册键里,
/// 监听器从事件值内取名字)。serial/waterfall 的 Value 用 Value,宿主可短路。
pub struct HostEvent {
    pub name: String,
    pub payload: serde_json::Value,
    pub origin: EventOrigin, // 评审补:三预留字段透传
}
impl Event for HostEvent {
    const NAME: &'static str = "host/*";
    type Value = serde_json::Value;
}
```

- **实施形态**(评审 2 注:与草案差异)——不做 `InboundHooks.on_event` 字段、`Bridge::start` 不持 `Ctx`;改为 `forward_host_events(ctx, observe) -> NotifyHook` 纯函数由 runner 组装进 `on_notify`(桥保持零 rutis 运行时知识,分层更干净);
- 泵的 `dispatch_notify` 对 `evt/emit` 经上述钩子调 `ctx.events().emit_keyed::<HostEvent>(name, HostEvent{..})`;其余 Ntf 维持现状;
- 三字段透传(scopeId/sessionId/turnId,rpc.rs 预留)——**实施已定**(评审补):`EventOrigin{scope_id, session_id, turn_id}` 独立结构体,泵解构 Ntf 时构造并随 `NotifyHook(method, params, origin)` 下传;`HostEvent.origin` 携带,订阅方区分同事件名下不同会话/回合;
- 订阅方用法:`ctx.events().on_keyed::<HostEvent>("session/event", |ctx, e| ..)`,随注册方 fiber 卸载自动清理(D28 照旧)。

### 2.5 决策点

- **D33a 双轨 Qualifier enum**(Static 零分配)而非 Cow/Arc 全量;TypeKey 失去 Copy 的清点责任在实施提交内。
- **D33b HostEvent 放 rutis-cordis**,内核不引 serde;payload 用 `serde_json::Value`(桥线已是它的形状)。
- **D33c 现有 API 不改签名**,keyed 全家桶为增量;内部共享一条 TypeKey 路径。
- **D33d Context.filter 事件作用域过滤仍不做**(D29 维持):跨 isolate 事件不过滤;宿主事件过滤由订阅方在监听器内自查(与 dsh 桥 v1"只收纯 emit"的现状一致)。
- **D33e loader(名字→工厂注册表)不在本设计**:动态插件装载是另一个课题,D32 的 PluginFactory 是它的底座。

### 2.6 测试计划(契约 + e2e,目标 ~9 条)

1. keyed 注册/分发基础:同类型不同名互不串扰,静态/动态同名互通;
2. 动态名(运行时 `format!` 构造)注册并 emit 命中;
3. D31 回归:keyed emit 同名保序、不同名不进同链;
4. `serial_keyed` 短路值 / `waterfall_keyed` veto / `parallel_keyed` 聚合(复用现有四语义断言模式);
5. once_keyed 恰好一次、prepend_keyed 顺序;
6. 类型不符(手工构造的 key 与分发 E 错位)fail fast;
7. 监听器随注册方 fiber 卸载自动摘除(D28 keyed 路径);
8. parity 补拍:events.spec 的 `ctx.on()/ctx.once()/ctx.waterfall()` 字符串名用例,原判"部分对拍(JS 载体)",keyed 落地后内核可全拍(名字 → keyed_dynamic);
9. 桥 e2e(tcp_e2e 模式):宿主发 `evt/emit` → Rust 侧 `on_keyed::<HostEvent>` 收到,含 payload 断言。

---

## 三 里程碑

| 阶段 | 内容 | 交付判据 |
|---|---|---|
| M1 | D32:plugin.rs + ctx.rs + fiber.rs + 契约测试 1.7 | 全绿 + clippy/fmt 清零 |
| M2 | D33:key.rs + bus.rs + event.rs + 契约测试 2.6(1-8) | 同上;现有 115 项零改动 |
| M3 | 桥:HostEvent + InboundHooks evt 缝 + rutis_dsh 组装 + e2e | tcp_e2e 扩展全绿 |

M1/M2 可并行(不同文件为主,fiber.rs 仅 M1 触及)。每阶段独立提交、独立可回滚。

## 四 风险

- **TypeKey 失 Copy 的连锁**:编译器全量暴露,无静默风险;工作量在逐点 clone,预计 ≤ 20 处。
- **D32 工厂双形态**:`load()` 分支多一条取实例路径;fail_load 复用保证原子性不破。
- **bridge 持 Ctx 的所有权环**:Bridge(Arc) → Ctx(Arc<CtxInner>) → fiber(Weak),无强环;实施时以现有 `Shared` 结构为准核对。
- **事件风暴**:宿主高频 evt/emit 下,emit_keyed 每次构造 Arc<str> 键(一次分配);dsh 现实测事件频率低(session/agent 级),不构成瓶颈,先不做缓存。

## 五 不做清单

- `internal/update` waterfall 与 noSave(D32d);
- config schema / schemastery 对应物(D32e 连带);
- Context.filter 事件过滤(D33d);
- loader(插件名→工厂注册表、配置文件驱动装载)(D33e)——D32 的 PluginFactory 是其底座,本期不建注册表;
- `internal/*` 协议面整体(维持原对拍裁决);
- 事件名通配符/glob 订阅(宿主侧真需要再加,当前无需求证据);
- **dispatch_tail 已完成 JoinHandle 的清理**(评审遗留:存量问题,非本设计引入;键在静态事件类型下有界,D33 动态名后有界性依赖宿主事件名集合——数量级仍是"事件名数",动 D31 结构的改造留后续简化批次)。

## 六 实施顺序建议

M1 → M2 → M3。M1 先行的理由:独立收益最大、不动总线;M2 改 TypeKey 触碰全仓签名,单独一个提交便于 review 与回滚。

## 七 实施记录(2026-09-21 执行完毕;§八 为评审修复轮)

**全部落地,全 workspace 测试全绿(新增 29:config_update 16 + event_keys 11 + host_events 2)**,clippy 对新增代码零警告(存量警告未动)。

- **M1(D32)**:`PluginFactory`(plugin.rs)+ `Ctx::plugin_with/plugin_from`(ctx.rs,与 `plugin()` 共享 mount 收尾)+ `FiberView::update/current_config`(fiber.rs)。内部:`FiberInner.factory/config` 双字段,`ErasedFactory` 擦除适配;`load()` 经 `current_plugin()` 统一取实例,工厂模式每代重造。**实施中补的一个设计缺口**:`resolve_deps` 原从 `plugin` 实例读 injects,工厂模式 plugin 为 None → 门控失效(直接装载);已改为工厂模式从当前 config 派生(config 先 clone 出锁再进用户回调,防重入死锁)。`update` 的 dry-run 用户回调裹 catch_unwind。测试 16 条:tests/config_update.rs(§1.7 的 1-10 + 评审增补 11-16,第二轮复审再增 17,见 §八)。
- **M2(D33)**:`Qualifier` 双轨 enum(key.rs,Static 零分配 / Dynamic `Arc<str>`;**Eq/Hash 手写按字符串内容**,derive 会按变体判等——测试抓出);bus 三个注册面键 `TypeId` → `TypeKey`,keyed API 全家桶(`on/once/on_waterfall/emit/parallel/serial/waterfall` 的 `_keyed` 变体,内部共享一条 TypeKey 路径)。TypeKey 失 `Copy` 的连锁由编译器全量暴露,registry 的 lookup/consumers_of/notify_key_changed 顺势改吃借用。测试 11 条:tests/event_keys.rs(含 parity 补拍:cordis events.spec 字符串事件名内核,原判"部分对拍"升级为可全拍)。
- **M3**:rutis-cordis 新模块 events.rs(`HostEvent{name, payload, origin}` + `forward_host_events(ctx, observe)`——**转发先于 observe**(评审修订),恶形 evt 打一行截断日志后丢弃);rutis_dsh runner 的 on_notify 换装(stderr 摘要降为观察者)。e2e:tests/host_events.rs(MemoryWire 双名订阅 + origin 透传断言 + 恶形不进总线 + 观察者全量 + 桥断连后总线存活,2 条)。

**与设计的偏差**:
- `current_config` 返回 `Option<Arc<C>>`(设计草案写 `Option<Box<C>>`;Arc 存储使快照零拷贝,且 update 的存入也走 Arc);
- `PluginFactory` 增加了带默认实现的 `name()` 方法(fiber 显示名,工厂模式 spawn 时取用)——草案未列,补此申报。

## 八 评审轮记录(2026-09-21,4 个独立评审员并行)

评审对象:PR #1(feat/config-hot-update-and-dynamic-events)。**阻断 0**;分层纪律、键机制正确性、数字声明(README 136(评审时点,修复后为 142)/ 全绿 / clippy)全部核实通过。

### 已修(全部)

1. **`load()` 的 build 无 panic 边界**(并发评审,应修):build panic 穿透驱动 → fiber 卡 Loading、join 永等。修复:`current_plugin()` 的 `build_erased` 包 catch_unwind,panic 转 `fail_load`,与 validate 对称;补契约测试 15(build panic → Failed、restart 可重试不挂起)。
2. **injects 随 config 漂移的静默失效**(并发评审,应修):registry 只在 spawn 注册一次,`injects(&config)` 漂移会让 notify/驱逐静默失效,且与 resolve_deps 每代重派生自相矛盾。修复:update dry-run 校验新 config 派生声明与 spawn 快照(`FiberInner.spawn_injects`)集合相等,不等 `Validation` 拒绝;`plugin.rs` 文档写明契约与替代方案。补契约测试 14。
3. **三字段透传缺失**(桥层评审,应修):泵 `Frame::Ntf { .. }` 丢弃 scopeId/sessionId/turnId 且未记录决定。修复:`EventOrigin` 独立结构体,`NotifyHook(method, params, origin)` 三参签名,`HostEvent.origin` 携带;e2e 断言透传。
4. **observe panic 吞事件**(桥层评审,应修):observe 先 await,panic 则该 Ntf 的转发静默丢失。修复:**转发先于 observe**(emit_keyed 只入队尾链);恶形帧由静默丢弃改为 eprintln 一行(可观测性)。
5. **测试 sleep 等待的 flaky 风险**(事件评审,应修):全部换 Notify 信号化等待;负断言改走确定性路径(无监听器键的 take_hooks 同步返回空,不产生任务;dispose await 返回即注册表摘除)——event_keys/host_events 全文件零 sleep。
6. **Loading 态 update 无测试**(一致性评审,应修)→ 契约测试 11(apply 运行中协作取消后重载新 config,兼覆盖 §1.5 "update × 运行中 apply")。
7. **update×驱逐 无测试**(一致性评审,应修)→ 契约测试 13(provider 换代与消费者热更新并发,双方收敛,末次重载读到 provider 新代值)。
8. **`injects_erased` 无 panic 边界**(并发评审,建议,与 1 同类):resolve_deps 与 spawn 两处调用均包 catch_unwind,panic 视为依赖不就绪(哨兵键 `InjectsUnavailable` 留 Pending,错误路由 ErrorSink;同 check() 既有语义)。补契约测试 16。
9. **桥断连场景无测试**(桥层评审,建议)→ host_events 增 `event_bus_survives_bridge_drop`(ctx 独立于 bridge 存活)。
10. **Qualifier Debug 打印变体名但 Eq 按内容**(事件评审,建议):手写 Debug 只显示字符串内容,与 Eq 一致。
11. **Unloading 态 update 无测试**(一致性评审,建议)→ 契约测试 12(慢清理中排队,收敛后重提供依赖装载新 config)。

### 文档修正

- §1.7 计划条目 #9/#10 与初版实施的映射错位已重写(原 #9"build 失败恢复"并入 #4 的 apply 失败路径语义,原 #10 parity 对拍由 event_keys 第 8 条承担);
- §2.3 分发面补全签名;§2.4 三字段的 proto 决定落档(见上);§五 增补 dispatch_tail 遗留项;
- parity 补拍测试注明 dispose 断言由 `keyed_listener_removed_with_owner_fiber` 承担(跨测试拼合覆盖)。

### 归档无行动

- update×dispose 的 TOCTOU 窗口:与 restart 完全同构,mailbox + post_join 复查封死,无坏状态无永等(并发评审明确结论);
- build 纯构造契约的并发双跑窗口:D32b 已声明契约,维持;
- `dispatch_tail` 已完成 JoinHandle 不清理:存量,键有界,记入 §五 遗留。

**修复后状态**:config_update 17 条 + event_keys 11 条(信号化)+ host_events 2 条 = 全 workspace 测试全绿,clippy 对新增代码零警告(存量 3 条:services.rs 1 + aimux-llm 2,与本设计无关)。

## 九 第二轮复审记录(2026-09-22,4 评审员并行复审修复轮)

复审对象:976d2f1。**阻断 0**;修复轮的关键声明(§1.7 映射 1:1、268 全绿、README 142、零 sleep、11 项修复全部落地)逐条验证**属实**;EventOrigin 透传链(含出站/握手期帧的 origin 语义)、转发先于 observe、信号化 Notify permit 语义、负断言确定性论证、签名适配、桥所有权环、CI 覆盖全部验证通过。

### 已修(第二轮发现)

1. **哨兵交互盲区(应修,窄)**:第一轮修复 #2/#8 的交互——spawn 时 injects panic 的哨兵同时进 `spawn_injects` 快照与 `inject_index`:死 Weak 无界累积(append-only)+ panic 工厂"既装不上也 update 不了"(漂移校验拿哨兵当基线必拒绝)。修复:spawn panic 时基线记空集且**不注册**(泄漏消除);update 对空基线**跳过漂移校验**并**补注册 derived**(panic 工厂获得恢复路径);`register_inject` 加指针去重(补注册与 spawn 注册重叠时 no-op)。测试 17:spawn 期 injects panic → update 换 config 恢复装载(装载次数随 mailbox 时序为 1-2 次,断言按终态语义)。
2. **spawn injects panic 路由 ErrorSink**(并发复审 S1):原 `Err(_p)` 静默丢弃,与 resolve_deps 路径不一致且 doc 不实。修复:panic 转 `PluginFailed` 经 `ctx.error_sink()` 路由,注释同步。
3. **恶形 eprintln 无截断**(桥层复审,应修):巨型 params 可刷爆 stderr。修复:200 字符截断(`truncate`,防御纵深);runner observe 跳过恶形帧,消除双打印(评审 2 建议 4)。
4. **文档内部矛盾三处**(一致性复审,应修):§七 M1 测试数 10→16、§七 M3 过时描述(并行/静默丢弃 → 转发先于/截断日志)、§八 README 引用 136→142——均已改。
5. **文档漂移**(盲区复审,建议):§2.3 "take_hooks 校验 type_id" 措辞改为"类型烙在键里,错位键无法经公开 API 构造"(实际防御机制);§2.4 `on_event`/`Bridge 持 Ctx` 草稿加实施注记(实际为 `forward_host_events` 钩子组合,桥不持 Ctx);§2.4 HostEvent 代码块补 `origin`;§五 dispatch_tail "键有界"补动态名说明;§1.7 计划 #8 补静态 fiber 拒绝;`events.rs` 文档"所有帧"改"所有通知帧(Ntf)";rutis-cordis lib.rs crate 文档补 events 模块。

### 归档无行动(第二轮)

- spawn 时 register_inject 与驱动启动的窗口(provider notify 先于注册到达则丢失):与静态模式完全同构的预存设计,实际使用模式下不可触发;
- `wait_until_state` 对瞬态的脆弱性:本组测试全部等待受控稳定中间态(SlowFactory gate / drain_gate 保持),helper 语义已在测试 17 注释说明;
- 测试 1 的"LIFO 断言"实为 apply 顺序断言(清理恰好一次已单独锁定)——计划 #1 措辞略宽,不改测试;
- 工作区 `examples/tui.rs` 的 fmt 残留(6 行)收入本修复提交。

## 十 第三轮复审记录(2026-09-22,3 评审员:空基线专审 / 不变量证伪(含探针实测) / 终态通读)

复审对象:c7043e2。前两轮的修复声明复核属实;本轮发现集中在**第二轮修复引入的新逻辑**与其交互。

### 已修(第三轮发现)

1. **空基线两种来源无法区分**(空基线专审,阻断):`spawn_injects` 用空 Vec 同时表示"panic 基线未知"与"明确声明无依赖"——后者 update 漂移到有依赖被当恢复静默放行,打穿 fail-fast。修复:`spawn_injects` 改 `Option<Vec<TypeKey>>`——`None`=panic 未知(跳过校验),`Some(含空集)`=明确声明(严格校验)。测试 18:真空声明漂移被拒。
2. **基线逃逸链**(证伪,应修,探针实测):None 基线恢复后若不写回,后续**所有** update 永远逃逸校验(探针:恢复 [A,B] 后漂移到 [B] 仍被允许,inject_index 累积过期条目)。修复:update 成功后写回 `Some(derived)` 成为新基线。测试 17 扩展:恢复后再漂移被拒。
3. **dry-run 后的并发 dispose 窗口**(证伪,应修,探针实测 10 中 6):dry-run 无锁同步,期间并发 dispose 可完整执行——update 返回 Ok 但 config 落进已死 fiber + 注册死 Weak。修复:dry-run 通过后、存 config 前**二次终态检查**(terminal_task/alive),被抢先则返回 InactiveEffect 不存不注册。
4. **`post_join` 自完成覆盖真实错误**(终态通读,应修,存量但触发面在 update 语义内):二次 alive 检查以 `None` 兜底,Failed 态驱动退出时真实终态错误被覆盖成伪装成功。修复:自完成携带 `tr.error`。注:post_join 是存量代码,此缺陷由本 PR 的 update 重度使用而暴露。
5. **observe 双打印残留**(空基线专审,应修):observe 用 `is_some()`、forward 用 `as_str`——`event: null/123` 类帧(字段存在非字符串)仍双打印。修复:observe 对齐 `and_then(as_str)` 判定。
6. **config=None 死分支哨兵化**(终态通读,建议):`resolve_deps` 工厂分支 config 为 None 时原返回空 Vec(未来若引入 None 路径会被当无依赖直接装载)——改返回哨兵键(留 Pending)。恶形 eprintln 补 `origin={origin:?}`(证伪,建议)。

### 归档无行动(第三轮,记入遗留)

- `inject_index` 死 Weak 永不清除:动态键场景无界增长,建议后续加定期 prune(append-only 保 Weak 计数、地址不复用,ABA 去重因此安全——证伪确认);
- `update()` 提取 dry-run 子函数 / 哨兵逻辑内聚为 `resolve_injects_safe`(终态通读,可读性):留后续简化批次;
- 测试魔法数字常量化、注释去"评审:"过程前缀(终态通读,风格):留后续;
- 证伪确认封住的不变量:死 Weak 幽灵消费者(upgrade 过滤)、EventOrigin 串扰(独立 Arc)、emit 重入/dispose/注册交叉(尾链快照语义)、测试 17 在 multi_thread 下的断言稳定性(所有 apply 均 v2)。

**修复后状态**:config_update 18 条 + event_keys 11 + host_events 2 = 全 workspace 270 项全绿;clippy 存量 3 条不变。

## 十一 D32f 修订:撤销 injects(config) 派生,声明改静态(2026-09-22)

**触发**:三轮评审修复(§八-§十)的复盘。6 个核心修复——哨兵键、基线快照、漂移校验、空基线歧义、逃逸链封堵、补注册去重——全部在防御同一个设计决定:`injects(&config)` 让依赖声明从配置派生。

**对照源码验证**:cordis(上游与 fork)的 `inject` 是 fiber 构造时固化的 `Dict` 字段(fiber.ts:125 构造注入,全文件无重新赋值;依赖门控 `Object.keys(this.inject)`,fiber.ts:166/388);fork 的 `update(config)`(fiber.ts:478)只改 `fiber.config`,从不触碰 inject 声明;懒 config 解析(internal/config waterfall)延迟的也只是**配置值**的解析时机。运行时换依赖集合的 cordis 形态是 `registry.inject(deps, cb)` 注册**新 fiber**(拆插件),不是改现有声明。"依赖随 config 变"在本仓库与 cordis 生态均无用例——**无中生有的能力**。

**修订内容**:

- `PluginFactory::injects` 改 `fn injects(&self) -> &[TypeKey]`,与 `Plugin::injects` 完全对称;spawn 注册一次、终身不变。按配置选依赖的标准形态 = 拆多个插件、配置决定装哪个(与 cordis loader 生态一致)。
- **删除的机制族**(约 -150 行):`FiberInner.spawn_injects` 基线快照、update 的漂移校验/基线写回/补注册、`register_inject` 指针去重、`InjectsUnavailable` 哨兵键、resolve_deps/spawn/update 对 injects 的三处 panic 边界、resolve_deps 的 config=None 分支。
- **保留的修复**(与被删机制无关,各自独立成立):dry-run 三步(§1.3)、二次终态检查(§十-3 的 dispose 竞态)、post_join 终态错误(§十-4)、EventOrigin/observe/truncate 全套。
- **测试影响**:删除 14/16/17/18(它们锁定的是被删机制的缺陷类——机制不存在则缺陷类不存在,这是优于"防住"的修复);config_update 18 → 14 条。
- `update()` 瘦回 ~50 行:终态检查 → factory 匹配 → dry-run → 二次终态检查 → 存 config → restart。

**教训(写给后续批次)**:修复轮引入的新语义(哨兵/基线/恢复)复杂度与原设计同级,却没有走设计审视——三轮评审打出的全部第三层问题(歧义/逃逸/竞态)都源于此。**修复的语义复杂度达到设计级时,修复也必须先写清楚语义再动手。**

**§八-§十 的修复记录保留为历史**;其中被 D32f 撤销的条目(§八-2/8、§九-1/2、§十-1/2)以本节为准。

## 十二 cordis 对照偏差审计(2026-09-22,D32f 后全量)

独立审计员三方源码对照(rutis / cordis 上游 / dsh fork)逐机制核对。**未声明的意外偏差仅 2 处**,其余全部落在已声明面(D1-D33 + 38 项不移植)或语言差异。

### 已修(审计 #2)

**Failed 纤维的依赖摘除行为**:cordis 的 FAILED 粘性——依赖摘除时 `_setEpoch(INACTIVE)` 因 epoch 已是 INACTIVE 早退,状态保持 Failed、错误持续可见(fiber.ts:611-639);依赖恢复时 epoch 变化触发 reload。rutis 此前把 Failed 算作 loaded,摘除时 `unload(Pending)` 降级,错误隐入 settle 通道。修复:`refresh_deps` 的 missing 分支只对 Active/Loading 卸载回 Pending,Failed 保持;依赖恢复路径(下方装载分支)不变——Failed 照常重试,与 cordis 的 reload 语义一致。测试 19:摘除保持 Failed(settle 持续报错)+ 恢复重试再失败仍 Failed。

### 封口声明(不修,注释落档)

- **审计 #1 跨 effect 清理并发性**:cordis 跨顶层 effect `Promise.all` 并发(fiber.ts:676)、单 effect 内 LIFO;rutis 跨 effect 也严格串行 LIFO——完成序确定、错误聚合可预期,方向性强化。drain_effects 注释已补对照。
- **审计 #7 update 非 ACTIVE 态校验时机**:cordis 延迟到激活时 `_resolveConfig`(fiber.ts:739);rutis 无条件 dry-run(D32b,尽早暴露)。update 注释已补对照。

### 语言差异(合理,不修)

serial 短路值(typed Option 替代 JS truthiness)、`check()` 谓词无 this 绑定(闭包等价)、`injects` 为方法而非构造固化字段(契约静态,D32f 已对齐语义)、依赖通知不做 isolate 前置过滤(各 fiber `scope_for` 解析时过滤,语义等价)。

**结论**:三方对照后,rutis 与 cordis 的差异全部显式化——要么在决策表/不移植清单里,要么在本节/源码注释有对照声明。
