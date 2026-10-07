//! 范式契约测试(设计 §五):assembly / lifecycle / gating / dispose /
//! events / registry / reload / cancel。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rutis::{
    BoxFuture, CordisError, Ctx, DisposeWaitError, Effect, Event, FiberState, FiberStatusChanged,
    FiberView, Listener, Next, Plugin, Terminal, TypeKey, WaterfallListener,
};

// ── 测试助手 ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Ping {
    value: u32,
}

impl Event for Ping {
    const NAME: &'static str = "test::Ping";
    type Value = u32;
}

#[derive(Debug)]
struct LlmSvc {
    n: u32,
}

#[derive(Debug)]
struct Dep1;
#[derive(Debug)]
struct Dep2;

type Order = Arc<Mutex<Vec<&'static str>>>;

fn order() -> Order {
    Arc::new(Mutex::new(Vec::new()))
}

fn recorder() -> (Arc<AtomicUsize>, Arc<tokio::sync::Notify>) {
    (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(tokio::sync::Notify::new()),
    )
}

/// 计数监听器(可配置短路值与错误)。
struct Rec {
    hits: Arc<AtomicUsize>,
    bail: Option<u32>,
    err: bool,
    notify: Option<Arc<tokio::sync::Notify>>,
    gate: Option<Arc<tokio::sync::Notify>>,
}

impl Listener<Ping> for Rec {
    fn call<'a>(
        &'a self,
        _ctx: &'a Ctx,
        e: &'a Ping,
    ) -> BoxFuture<'a, Result<Option<u32>, CordisError>> {
        let hits = self.hits.clone();
        let value = e.value;
        Box::pin(async move {
            if let Some(gate) = &self.gate {
                gate.notified().await;
            }
            hits.fetch_add(1, Ordering::SeqCst);
            if let Some(n) = &self.notify {
                n.notify_one();
            }
            if self.err {
                return Err(CordisError::ServiceNotFound("boom".to_string()));
            }
            Ok(self.bail.map(|b| b + value))
        })
    }
}

/// 闭包插件助手。
struct Simple<F> {
    name: &'static str,
    injects: Vec<TypeKey>,
    apply: F,
}

impl<F> Plugin for Simple<F>
where
    F: for<'a> Fn(&'a Ctx) -> BoxFuture<'a, Result<Effect, CordisError>> + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.name
    }
    fn injects(&self) -> &[TypeKey] {
        &self.injects
    }
    fn apply<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<Effect, CordisError>> {
        (self.apply)(ctx)
    }
}

fn simple<F>(name: &'static str, apply: F) -> Simple<F>
where
    F: for<'a> Fn(&'a Ctx) -> BoxFuture<'a, Result<Effect, CordisError>> + Send + Sync + 'static,
{
    Simple {
        name,
        injects: vec![],
        apply,
    }
}

fn simple_dep<F>(name: &'static str, injects: Vec<TypeKey>, apply: F) -> Simple<F>
where
    F: for<'a> Fn(&'a Ctx) -> BoxFuture<'a, Result<Effect, CordisError>> + Send + Sync + 'static,
{
    Simple {
        name,
        injects,
        apply,
    }
}

fn counting_effect(counter: Arc<AtomicUsize>, err: Option<CordisError>) -> Effect {
    Effect::Disposer(Box::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }))
}

fn marker_effect(name: &'static str, order: Order) -> Effect {
    Effect::AsyncDisposer(Box::new(move || {
        let order = order.clone();
        Box::pin(async move {
            tokio::task::yield_now().await;
            order.lock().unwrap().push(name);
            Ok(())
        })
    }))
}

async fn soon<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("timed out")
}

/// Polls `done` until it holds; the timeout only guards against a hang.
async fn eventually(mut done: impl FnMut() -> bool) {
    soon(async {
        while !done() {
            tokio::task::yield_now().await;
        }
    })
    .await
}

/// Waits until every accepted `emit` has finished dispatching.
async fn drained(ctx: &Ctx) {
    eventually(|| ctx.diagnostics().event_backlogs.is_empty()).await
}

/// Waits until `view` reaches `state`.
async fn reach(view: &FiberView, state: FiberState) {
    let mut rx = view.watch();
    soon(rx.wait_for(|snapshot| snapshot.state == state))
        .await
        .expect("fiber dropped");
}

/// Polls `fut` once and checks it is still waiting: the caller is then
/// parked on the result before the test lets it complete.
async fn parked<F: std::future::Future + Unpin>(fut: &mut F) {
    tokio::select! {
        biased;
        _ = fut => panic!("finished while it should still be waiting"),
        _ = std::future::ready(()) => {}
    }
}

/// Runs `n` scheduler turns: a stand-in for "takes a while" that does not
/// depend on the clock.
async fn turns(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

// ── assembly(支柱 1)──────────────────────────────────────────────

#[tokio::test]
async fn plugin_provides_n_services() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple("provider", move |ctx: &Ctx| {
        Box::pin(async move {
            ctx.provide(LlmSvc { n: 1 })?;
            ctx.provide(Dep1)?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    assert!(ctx.get::<LlmSvc>().is_some());
    assert!(ctx.get::<Dep1>().is_some());
    view.dispose().await.unwrap();
    assert!(ctx.get::<LlmSvc>().is_none());
    assert!(ctx.get::<Dep1>().is_none());
}

#[tokio::test]
async fn plugin_registers_n_listeners() {
    let ctx = Ctx::root().unwrap();
    let (hits, done) = recorder();
    let h = hits.clone();
    let d = done.clone();
    let view = ctx.plugin(simple("listener-plugin", move |ctx: &Ctx| {
        let h1 = h.clone();
        let h2 = h.clone();
        let d2 = d.clone();
        Box::pin(async move {
            ctx.events().on(
                ctx,
                &rutis::EventKey::of(),
                Rec {
                    hits: h1,
                    bail: None,
                    err: false,
                    notify: None,
                    gate: None,
                },
            )?;
            ctx.events().on(
                ctx,
                &rutis::EventKey::of(),
                Rec {
                    hits: h2,
                    bail: None,
                    err: false,
                    notify: Some(d2),
                    gate: None,
                },
            )?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    ctx.events()
        .emit(&ctx, &rutis::EventKey::of(), Arc::new(Ping { value: 1 }))
        .expect("default event dispatch");
    soon(done.notified()).await;
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    view.dispose().await.unwrap();
    ctx.events()
        .emit(&ctx, &rutis::EventKey::of(), Arc::new(Ping { value: 1 }))
        .expect("default event dispatch");
    drained(&ctx).await;
    assert_eq!(hits.load(Ordering::SeqCst), 2); // 卸载后不再分发
}

#[tokio::test]
async fn effect_yields_disposer() {
    let ctx = Ctx::root().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let d = ctx.effect(move || counting_effect(c, None)).unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    d.dispose().await.unwrap(); // 提前释放(D28)
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    // fiber 卸载不重复执行(恰好一次)
    ctx.root_view().unwrap().dispose().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn auto_ownership() {
    let ctx = Ctx::root().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let view = ctx.plugin(simple("owner", move |ctx: &Ctx| {
        let counter = c.clone();
        Box::pin(async move {
            ctx.provide(LlmSvc { n: 1 })?;
            ctx.effect(move || counting_effect(counter, None))?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    // Disposer 被 drop(未手动 dispose):fiber 卸载仍兜底清理
    view.dispose().await.unwrap();
    assert!(ctx.get::<LlmSvc>().is_none());
    assert_eq!(counter.load(Ordering::SeqCst), 1); // 清理确实执行了一次
}

// ── lifecycle ────────────────────────────────────────────────────

#[tokio::test]
async fn state_transitions() {
    let ctx = Ctx::root().unwrap();
    let seen: Arc<Mutex<Vec<FiberStatusChanged>>> = Arc::new(Mutex::new(Vec::new()));
    // 用监听器结构体收集状态事件(D24 锁外分发)
    struct S(Arc<Mutex<Vec<FiberStatusChanged>>>);
    impl Listener<FiberStatusChanged> for S {
        fn call<'a>(
            &'a self,
            _ctx: &'a Ctx,
            e: &'a FiberStatusChanged,
        ) -> BoxFuture<'a, Result<Option<()>, CordisError>> {
            let seen = self.0.clone();
            let ev = e.clone();
            Box::pin(async move {
                seen.lock().unwrap().push(ev);
                Ok(None)
            })
        }
    }
    ctx.events()
        .on(&ctx, &rutis::EventKey::of(), S(seen.clone()))
        .unwrap();

    let view = ctx.plugin(simple("states", move |_ctx: &Ctx| {
        Box::pin(async { Ok(Effect::Done) })
    }));
    (&view).await.expect("load");
    view.dispose().await.unwrap();
    drained(&ctx).await;

    let mut events: Vec<FiberStatusChanged> = seen.lock().unwrap().clone();
    events.sort_by_key(|e| e.seq);
    let path: Vec<(FiberState, FiberState)> = events.into_iter().map(|e| (e.from, e.to)).collect();
    assert_eq!(
        path,
        vec![
            (FiberState::Pending, FiberState::Loading),
            (FiberState::Loading, FiberState::Active),
            (FiberState::Active, FiberState::Unloading),
            (FiberState::Unloading, FiberState::Disposed),
        ]
    );
}

#[tokio::test]
async fn init_failure_marks_failed() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple("fail", move |_ctx: &Ctx| {
        Box::pin(async { Err(CordisError::ServiceNotFound("nope".into())) })
    }));
    let err = (&view).await.expect_err("must fail");
    assert!(matches!(*err, CordisError::ServiceNotFound(_)));
    assert_eq!(view.state().state, FiberState::Failed);
}

#[tokio::test]
async fn dispose_idempotent() {
    let ctx = Ctx::root().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let view = ctx.plugin(simple("once", move |_ctx: &Ctx| {
        let c = c.clone();
        Box::pin(async move { Ok(counting_effect(c, None)) })
    }));
    (&view).await.expect("load");
    view.dispose().await.unwrap();
    view.dispose().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn root_restart() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple("child", move |_ctx: &Ctx| {
        Box::pin(async { Ok(Effect::Done) })
    }));
    (&view).await.expect("load");
    let root = ctx.root_view().unwrap();
    root.dispose().await.unwrap();
    assert_eq!(view.state().state, FiberState::Disposed);
    // root 可重启,可再装配
    root.restart().await.unwrap();
    let view2 = ctx.plugin(simple("child2", move |_ctx: &Ctx| {
        Box::pin(async { Ok(Effect::Done) })
    }));
    (&view2).await.expect("load after root restart");
}

#[tokio::test]
async fn root_restart_dispose_cycle() {
    // 重启后的第二次 dispose 必须真正再卸载一轮(终态任务不复用陈旧结果)
    let ctx = Ctx::root().unwrap();
    let root = ctx.root_view().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    ctx.effect(move || counting_effect(c, None)).unwrap();
    root.dispose().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    root.restart().await.unwrap();
    let c2 = counter.clone();
    ctx.effect(move || counting_effect(c2, None)).unwrap();
    root.dispose().await.unwrap(); // 第二次:真实清理
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(root.state().state, FiberState::Disposed);
}

#[tokio::test]
async fn concurrent_dispose_join() {
    let ctx = Ctx::root().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let view = ctx.plugin(simple("joinme", move |_ctx: &Ctx| {
        let c = c.clone();
        Box::pin(async move { Ok(counting_effect(c, Some(CordisError::InactiveEffect))) })
    }));
    (&view).await.expect("load");
    let v1 = view.clone();
    let v2 = view.clone();
    let v3 = view.clone();
    let (r1, r2, r3) = tokio::join!(
        async { v1.dispose().await },
        async { v2.dispose().await },
        async { v3.dispose().await },
    );
    let e1 = r1.unwrap_err();
    let e2 = r2.unwrap_err();
    let e3 = r3.unwrap_err();
    // 并发 dispose join 同一 Arc(恰好一次 + identity,D6/D20)
    assert!(Arc::ptr_eq(&e1, &e2));
    assert!(Arc::ptr_eq(&e2, &e3));
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cross_generation_isolation() {
    let ctx = Ctx::root().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let view = ctx.plugin(simple("flaky", move |_ctx: &Ctx| {
        let attempts = attempts.clone();
        Box::pin(async move {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(CordisError::ServiceNotFound("first fails".into()))
            } else {
                Ok(Effect::Done)
            }
        })
    }));
    // 首代失败被 await 观察为 Err
    (&view).await.expect_err("first gen fails");
    // restart:干净重装配,旧代错误不毒化新代
    view.restart().await.unwrap();
    assert_eq!(view.state().state, FiberState::Active);
    assert!(view.state().error.is_none());
}

#[tokio::test]
async fn loading_unload_no_lost_wakeup() {
    let ctx = Ctx::root().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (e, r) = (entered.clone(), release.clone());
    let view = ctx.plugin(simple("slow", move |_ctx: &Ctx| {
        let (e, r) = (e.clone(), r.clone());
        Box::pin(async move {
            e.notify_one();
            r.notified().await;
            Ok(Effect::Done)
        })
    }));
    // Loading 中触发卸载:intent 串行,不丢唤醒、不死锁
    soon(entered.notified()).await;
    let mut disposed = view.dispose();
    parked(&mut disposed).await; // 等待者已挂起,此时 apply 仍未返回
    release.notify_one();
    soon(disposed).await.unwrap();
    assert_eq!(view.state().state, FiberState::Disposed);
}

// ── gating(支柱 2)────────────────────────────────────────────────

#[tokio::test]
async fn waits_for_dependency() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple_dep(
        "consumer",
        vec![TypeKey::of::<LlmSvc>()],
        move |ctx: &Ctx| {
            Box::pin(async move {
                assert!(ctx.get::<LlmSvc>().is_some());
                Ok(Effect::Done)
            })
        },
    ));
    (&view).await.expect("settle on Pending is Ok"); // 初始装载已处理
    assert_eq!(view.state().state, FiberState::Pending); // 依赖未齐 → Pending
    ctx.provide(LlmSvc { n: 7 }).unwrap();
    (&view).await.expect("activates after provider");
}

#[tokio::test]
async fn late_provider_activates() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple_dep(
        "late-consumer",
        vec![TypeKey::of::<LlmSvc>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    // 后到 provider(顺序无关):先 Pending,后激活
    (&view).await.expect("settle on Pending is Ok");
    assert_eq!(view.state().state, FiberState::Pending);
    ctx.provide(LlmSvc { n: 1 }).unwrap();
    reach(&view, FiberState::Active).await;
}

#[tokio::test]
async fn check_evicts() {
    let ctx = Ctx::root().unwrap();
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let flag2 = flag.clone();
    ctx.provide_as_with_check(
        TypeKey::of::<LlmSvc>(),
        Arc::new(LlmSvc { n: 1 }),
        move || flag2.load(Ordering::SeqCst),
    )
    .unwrap();
    let view = ctx.plugin(simple_dep(
        "checked",
        vec![TypeKey::of::<LlmSvc>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    (&view).await.expect("loads while check passes");
    assert_eq!(view.state().state, FiberState::Active);
    // check() 谓词翻转为 false → 驱逐回 Pending(fiber.ts:689-701 语义)
    flag.store(false, Ordering::SeqCst);
    ctx.refresh();
    reach(&view, FiberState::Pending).await;
}

#[tokio::test]
async fn pending_not_failed() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple_dep(
        "forever",
        vec![TypeKey::of::<LlmSvc>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    // 缺依赖长期 Pending 是合法状态,不报错(D22)
    (&view).await.expect("settle on Pending is Ok");
    turns(20).await; // 再给驱动几轮机会:仍不得变成 Failed
    assert_eq!(view.state().state, FiberState::Pending);
    (&view).await.expect("settle on Pending is Ok");
}

// ── dispose(EffectRecord 契约)────────────────────────────────────

#[tokio::test]
async fn lifo_serial() {
    let ctx = Ctx::root().unwrap();
    // 记录 start/end 事件对:串行下严格配对不交错,并发下先执行者让出的
    // 几轮调度里会出现别人的 start(锁死"串行"性质,而非仅完成顺序)
    let trace: Arc<Mutex<Vec<(&'static str, &'static str)>>> = Arc::new(Mutex::new(Vec::new()));
    let t = trace.clone();
    let view = ctx.plugin(simple("lifo", move |ctx: &Ctx| {
        let t = t.clone();
        Box::pin(async move {
            let mk =
                |name: &'static str, n: usize, t: Arc<Mutex<Vec<(&'static str, &'static str)>>>| {
                    Effect::AsyncDisposer(Box::new(move || {
                        let t = t.clone();
                        Box::pin(async move {
                            t.lock().unwrap().push((name, "start"));
                            turns(n).await;
                            t.lock().unwrap().push((name, "end"));
                            Ok(())
                        })
                    }))
                };
            let t1 = t.clone();
            ctx.effect(move || mk("e1", 3, t1))?;
            let t2 = t.clone();
            ctx.effect(move || mk("e2", 2, t2))?;
            let t3 = t.clone();
            ctx.effect(move || mk("e3", 1, t3))?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    view.dispose().await.unwrap();
    // 严格 LIFO 且串行:每个清理的 start/end 配对,不与其它清理交错
    assert_eq!(
        *trace.lock().unwrap(),
        vec![
            ("e3", "start"),
            ("e3", "end"),
            ("e2", "start"),
            ("e2", "end"),
            ("e1", "start"),
            ("e1", "end"),
        ]
    );
}

#[tokio::test]
async fn aggregate_no_flatten() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple("agg", move |ctx: &Ctx| {
        Box::pin(async move {
            // 先注册 nested:LIFO 后跑,其错误排在聚合第二位
            ctx.effect(move || {
                Effect::Disposer(Box::new(move || {
                    Err(CordisError::Aggregate {
                        errors: vec![
                            Arc::new(CordisError::ServiceNotFound("b".into())),
                            Arc::new(CordisError::ServiceNotFound("c".into())),
                        ],
                    })
                }))
            })?;
            ctx.effect(move || {
                Effect::Disposer(Box::new(|| Err(CordisError::ServiceNotFound("a".into()))))
            })?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    let err = view.dispose().await.expect_err("cleanup errors");
    match &*err {
        CordisError::Aggregate { errors } => {
            assert_eq!(errors.len(), 2); // 两个清理各一个错误
                                         // 用户自己的 Aggregate 不被压平:作为单元素出现
            match &*errors[1] {
                CordisError::Aggregate { errors: inner } => assert_eq!(inner.len(), 2),
                other => panic!("expected nested aggregate, got {other:?}"),
            }
        }
        other => panic!("expected aggregate, got {other:?}"),
    }
}

#[tokio::test]
async fn exactly_once_same_error() {
    let ctx = Ctx::root().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    let view = ctx.plugin(simple("exactly", move |_ctx: &Ctx| {
        let c = c.clone();
        Box::pin(async move { Ok(counting_effect(c, Some(CordisError::InactiveEffect))) })
    }));
    (&view).await.expect("load");
    let first = view.dispose().await.expect_err("first");
    // dispose 返回后重复调用 join 同一终态(不是重新清理)
    let again = view.clone().dispose().await.expect_err("again");
    assert!(Arc::ptr_eq(&first, &again));
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dispose_during_dispose() {
    let ctx = Ctx::root().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let counter = Arc::new(AtomicUsize::new(0));
    let s = started.clone();
    let r = release.clone();
    let c = counter.clone();
    let view = ctx.plugin(simple("slowclean", move |_ctx: &Ctx| {
        let s = s.clone();
        let r = r.clone();
        let c = c.clone();
        Box::pin(async move {
            Ok(Effect::AsyncDisposer(Box::new(move || {
                let s = s;
                let c = c;
                Box::pin(async move {
                    s.notify_one();
                    r.notified().await;
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })))
        })
    }));
    (&view).await.expect("load");
    let v = view.clone();
    let first = tokio::spawn(async move { v.dispose().await });
    soon(started.notified()).await; // 清理已开始
    let mut second = view.dispose(); // 清理中再 dispose:join,不死锁
    parked(&mut second).await;
    release.notify_one();
    soon(second).await.unwrap();
    first.await.unwrap().unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn effect_during_cleanup_fails() {
    let ctx = Ctx::root().unwrap();
    let captured: Arc<Mutex<Option<CordisError>>> = Arc::new(Mutex::new(None));
    let cap = captured.clone();
    let view = ctx.plugin(simple("reentrant", move |ctx: &Ctx| {
        let cap = cap.clone();
        let ctx2 = ctx.clone();
        Box::pin(async move {
            let inner = ctx2.clone();
            ctx2.effect(move || {
                Effect::AsyncDisposer(Box::new(move || {
                    let cap = cap.clone();
                    let ctx3 = inner.clone();
                    Box::pin(async move {
                        // 卸载中注册新 effect → InactiveEffect(fiber.ts:434-436)
                        let e = ctx3.effect(move || Effect::Done).err();
                        *cap.lock().unwrap() = e;
                        Ok(())
                    })
                }))
            })?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    view.dispose().await.unwrap();
    assert!(matches!(
        captured.lock().unwrap().take().unwrap(),
        CordisError::InactiveEffect
    ));
}

// ── events(四分发)───────────────────────────────────────────────

#[tokio::test]
async fn emit_async_safe() {
    let ctx = Ctx::root().unwrap();
    let (hits, done) = recorder();
    // 值记录监听器:校验 spawn 任务读到的载荷内容(强度加固)
    struct ValueL(Arc<Mutex<Vec<u32>>>);
    impl Listener<Ping> for ValueL {
        fn call<'a>(
            &'a self,
            _c: &'a Ctx,
            e: &'a Ping,
        ) -> rutis::BoxFuture<'a, Result<Option<u32>, CordisError>> {
            let seen = self.0.clone();
            let v = e.value;
            Box::pin(async move {
                seen.lock().unwrap().push(v);
                Ok(None)
            })
        }
    }
    let seen: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    // fn 项监听器:验证 Listener 的 blanket impl(Fn 路线)可用;
    // HRTB 返回型闭包字面量推断受限,惯用写法为 fn 项或小结构体(见 §八)
    fn ok_ping<'a>(
        _ctx: &'a Ctx,
        e: &'a Ping,
    ) -> rutis::BoxFuture<'a, Result<Option<u32>, CordisError>> {
        Box::pin(async move { Ok(Some(e.value)) })
    }
    ctx.events()
        .on(&ctx, &rutis::EventKey::of(), ok_ping)
        .unwrap();
    ctx.events()
        .on(&ctx, &rutis::EventKey::of(), ValueL(seen.clone()))
        .unwrap();
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: false,
                notify: Some(done.clone()),
                gate: None,
            },
        )
        .unwrap();
    let event = Arc::new(Ping { value: 42 });
    let kept = event.clone();
    ctx.events()
        .emit(&ctx, &rutis::EventKey::of(), event)
        .expect("default event dispatch");
    drop(kept); // 调用栈持有的克隆先释放,spawn 任务仍安全读
    soon(done.notified()).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(*seen.lock().unwrap(), vec![42]); // 载荷在 spawn 任务中安全读出
}

#[tokio::test]
async fn emit_error_sink() {
    let collected: Arc<Mutex<Vec<Arc<CordisError>>>> = Arc::new(Mutex::new(Vec::new()));
    let sink_to = collected.clone();
    let ctx = Ctx::root_with_sink(
        tokio::runtime::Handle::current(),
        Arc::new(move |e: Arc<CordisError>| {
            sink_to.lock().unwrap().push(e);
        }),
    );
    let (hits, _done) = recorder();
    struct Boom;
    impl Listener<Ping> for Boom {
        fn call<'a>(
            &'a self,
            _c: &'a Ctx,
            _e: &'a Ping,
        ) -> BoxFuture<'a, Result<Option<u32>, CordisError>> {
            Box::pin(async { panic!("listener boom") })
        }
    }
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: true,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    ctx.events().on(&ctx, &rutis::EventKey::of(), Boom).unwrap();
    ctx.events()
        .emit(&ctx, &rutis::EventKey::of(), Arc::new(Ping { value: 1 }))
        .expect("default event dispatch");
    // Err 与 panic 都进 ErrorSink(D30),各一次
    drained(&ctx).await;
    let got = collected.lock().unwrap();
    assert_eq!(got.len(), 2);
    assert!(matches!(*got[0], CordisError::ServiceNotFound(_)));
    assert!(matches!(*got[1], CordisError::PluginFailed(_))); // panic 包装
}

#[tokio::test]
async fn parallel_aggregates() {
    let ctx = Ctx::root().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: true,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: true,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    let err = ctx
        .events()
        .parallel(&ctx, &rutis::EventKey::of(), Arc::new(Ping { value: 1 }))
        .await
        .expect_err("aggregates");
    match err {
        CordisError::Aggregate { errors } => assert_eq!(errors.len(), 2),
        other => panic!("expected aggregate, got {other:?}"),
    }
    // 单错原样(§八)
    let ctx2 = Ctx::root().unwrap();
    ctx2.events()
        .on(
            &ctx2,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: true,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    let single = ctx2
        .events()
        .parallel(&ctx2, &rutis::EventKey::of(), Arc::new(Ping { value: 1 }))
        .await
        .expect_err("single");
    assert!(matches!(single, CordisError::ServiceNotFound(_)));
}

#[tokio::test]
async fn serial_bails() {
    let ctx = Ctx::root().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: false,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: Some(7),
                err: false,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    let out = ctx
        .events()
        .serial(&ctx, &rutis::EventKey::of(), &Ping { value: 1 })
        .await
        .unwrap();
    assert_eq!(out, Some(8)); // 短路值 = bail + e.value
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    // 短路后不再继续
    let hits2 = Arc::new(AtomicUsize::new(0));
    let ctx2 = Ctx::root().unwrap();
    ctx2.events()
        .on(
            &ctx2,
            &rutis::EventKey::of(),
            Rec {
                hits: hits2.clone(),
                bail: Some(1),
                err: false,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    ctx2.events()
        .on(
            &ctx2,
            &rutis::EventKey::of(),
            Rec {
                hits: hits2.clone(),
                bail: None,
                err: false,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    let out2 = ctx2
        .events()
        .serial(&ctx2, &rutis::EventKey::of(), &Ping { value: 0 })
        .await
        .unwrap();
    assert_eq!(out2, Some(1));
    assert_eq!(hits2.load(Ordering::SeqCst), 1); // 第二个未调用
}

#[tokio::test]
async fn waterfall_veto_around() {
    struct Wf {
        name: &'static str,
        order: Order,
        veto: bool,
    }
    impl WaterfallListener<Ping> for Wf {
        fn call<'a>(
            &'a self,
            _ctx: &'a Ctx,
            e: &'a Ping,
            next: Next<'a, Ping>,
        ) -> BoxFuture<'a, Result<u32, CordisError>> {
            let order = self.order.clone();
            let base = e.value;
            Box::pin(async move {
                order.lock().unwrap().push(self.name);
                if self.veto {
                    return Ok(base + 100); // veto:不调 next
                }
                let inner = next.call().await?;
                Ok(inner + 10) // 包裹:改写内层结果
            })
        }
    }
    struct BaseTerm;
    impl Terminal<Ping> for BaseTerm {
        fn call<'a>(
            &'a self,
            _ctx: &'a Ctx,
            e: &'a Ping,
        ) -> BoxFuture<'a, Result<u32, CordisError>> {
            Box::pin(async move { Ok(e.value) })
        }
    }

    // 包裹:outer(inner(terminal)) = (1)+10+10
    let ctx = Ctx::root().unwrap();
    let ord = order();
    ctx.events()
        .on_waterfall(
            &ctx,
            &rutis::EventKey::of(),
            Wf {
                name: "outer",
                order: ord.clone(),
                veto: false,
            },
        )
        .unwrap();
    ctx.events()
        .on_waterfall(
            &ctx,
            &rutis::EventKey::of(),
            Wf {
                name: "inner",
                order: ord.clone(),
                veto: false,
            },
        )
        .unwrap();
    let out = ctx
        .events()
        .waterfall(&ctx, &rutis::EventKey::of(), &Ping { value: 1 }, BaseTerm)
        .await
        .unwrap();
    assert_eq!(out, 21); // 最外层返回
    assert_eq!(*ord.lock().unwrap(), vec!["outer", "inner"]);

    // veto:后续监听器与终态都不执行
    let ctx2 = Ctx::root().unwrap();
    let ord2 = order();
    ctx2.events()
        .on_waterfall(
            &ctx2,
            &rutis::EventKey::of(),
            Wf {
                name: "outer",
                order: ord2.clone(),
                veto: true,
            },
        )
        .unwrap();
    ctx2.events()
        .on_waterfall(
            &ctx2,
            &rutis::EventKey::of(),
            Wf {
                name: "inner",
                order: ord2.clone(),
                veto: false,
            },
        )
        .unwrap();
    let out2 = ctx2
        .events()
        .waterfall(&ctx2, &rutis::EventKey::of(), &Ping { value: 1 }, BaseTerm)
        .await
        .unwrap();
    assert_eq!(out2, 101);
    assert_eq!(*ord2.lock().unwrap(), vec!["outer"]);
}

#[tokio::test]
async fn prepend_order() {
    let ctx = Ctx::root().unwrap();
    let ord = order();
    struct Named(&'static str, Order);
    impl Listener<Ping> for Named {
        fn call<'a>(
            &'a self,
            _c: &'a Ctx,
            _e: &'a Ping,
        ) -> BoxFuture<'a, Result<Option<u32>, CordisError>> {
            let o = self.1.clone();
            let n = self.0;
            Box::pin(async move {
                o.lock().unwrap().push(n);
                Ok(None)
            })
        }
    }
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Named("first-registered", ord.clone()),
        )
        .unwrap();
    ctx.events()
        .on_opt(
            &ctx,
            &rutis::EventKey::of(),
            Named("prepended", ord.clone()),
            rutis::EventOptions::default().prepend(true),
        )
        .unwrap();
    ctx.events()
        .serial(&ctx, &rutis::EventKey::of(), &Ping { value: 0 })
        .await
        .unwrap();
    assert_eq!(*ord.lock().unwrap(), vec!["prepended", "first-registered"]);
}

#[tokio::test]
async fn once_once() {
    let ctx = Ctx::root().unwrap();
    let (hits, done) = recorder();
    ctx.events()
        .once(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: false,
                notify: Some(done.clone()),
                gate: None,
            },
        )
        .unwrap();
    ctx.events()
        .emit(&ctx, &rutis::EventKey::of(), Arc::new(Ping { value: 1 }))
        .expect("default event dispatch");
    soon(done.notified()).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    ctx.events()
        .emit(&ctx, &rutis::EventKey::of(), Arc::new(Ping { value: 1 }))
        .expect("default event dispatch");
    drained(&ctx).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1); // 至多一次
}

#[tokio::test]
async fn once_keeps_position() {
    // once 监听器保持注册位置(§四:顺序控制影响 serial 结果),
    // 不因 once 语义被挪到分发队尾
    let ctx = Ctx::root().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    ctx.events()
        .once(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: Arc::new(AtomicUsize::new(0)),
                bail: Some(1),
                err: false,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: false,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    let out = ctx
        .events()
        .serial(&ctx, &rutis::EventKey::of(), &Ping { value: 0 })
        .await
        .unwrap();
    assert_eq!(out, Some(1)); // once 的短路值先到(位置在前)
    assert_eq!(hits.load(Ordering::SeqCst), 0); // 后注册的未短路未调用
                                                // 第二次:once 已消耗,轮到 regular
    let out2 = ctx
        .events()
        .serial(&ctx, &rutis::EventKey::of(), &Ping { value: 0 })
        .await
        .unwrap();
    assert_eq!(out2, None);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn listener_unload_race() {
    let ctx = Ctx::root().unwrap();
    let (hits, _done) = recorder();
    let h = hits.clone();
    // 进入监听器时报到,再交给 Rec 等 gate
    struct Entered(Arc<tokio::sync::Notify>, Rec);
    impl Listener<Ping> for Entered {
        fn call<'a>(
            &'a self,
            ctx: &'a Ctx,
            e: &'a Ping,
        ) -> BoxFuture<'a, Result<Option<u32>, CordisError>> {
            self.0.notify_one();
            self.1.call(ctx, e)
        }
    }
    let gate = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let gate2 = gate.clone();
    let entered2 = entered.clone();
    let view = ctx.plugin(simple("racer", move |ctx: &Ctx| {
        let hits = h.clone();
        let gate = gate2.clone();
        let entered = entered2.clone();
        Box::pin(async move {
            ctx.events().on(
                ctx,
                &rutis::EventKey::of(),
                Entered(
                    entered,
                    Rec {
                        hits,
                        bail: None,
                        err: false,
                        notify: None,
                        gate: Some(gate),
                    },
                ),
            )?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    // 分发进行中(监听器等 gate):快照语义,本轮照常调用
    let ctx2 = ctx.clone();
    let dispatch = tokio::spawn(async move {
        ctx2.events()
            .serial(&ctx2, &rutis::EventKey::of(), &Ping { value: 1 })
            .await
    });
    soon(entered.notified()).await; // 监听器已在 gate 上
    view.dispose().await.unwrap(); // 与进行中分发竞争
    gate.notify_one(); // 放行
    dispatch.await.unwrap().unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    // 卸载后:不再分发
    ctx.events()
        .serial(&ctx, &rutis::EventKey::of(), &Ping { value: 1 })
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

// ── registry(支柱 3)──────────────────────────────────────────────

#[tokio::test]
async fn typed_roundtrip() {
    let ctx = Ctx::root().unwrap();
    ctx.provide(LlmSvc { n: 3 }).unwrap();
    assert_eq!(ctx.get::<LlmSvc>().unwrap().n, 3);
    // 重复注册报错
    let err = ctx.provide(LlmSvc { n: 4 }).unwrap_err();
    assert!(matches!(err, CordisError::ServiceExists(_)));
    // 卸载(root 卸载清空)后可再注册
    ctx.root_view().unwrap().dispose().await.unwrap();
    ctx.root_view().unwrap().restart().await.unwrap();
    ctx.provide(LlmSvc { n: 5 }).unwrap();
    assert_eq!(ctx.get::<LlmSvc>().unwrap().n, 5);
}

#[tokio::test]
async fn mismatched_service_key_never_satisfies_injection() {
    let ctx = Ctx::root().unwrap();
    let key = TypeKey::keyed_dynamic::<Dep1>("session-1");
    let consumer = ctx.plugin(simple_dep(
        "needs-dep1",
        vec![key.clone()],
        move |ctx: &Ctx| {
            let key = TypeKey::keyed::<Dep1>("session-1");
            Box::pin(async move {
                assert!(ctx.get_as::<Dep1>(key).is_some());
                Ok(Effect::Done)
            })
        },
    ));
    soon(async { (&consumer).await }).await.unwrap();
    assert_eq!(consumer.state().state, FiberState::Pending);

    for err in [
        ctx.provide_as(key.clone(), Arc::new(Dep2)).unwrap_err(),
        ctx.provide_as_with_check(key.clone(), Arc::new(Dep2), || true)
            .unwrap_err(),
    ] {
        let CordisError::Validation { issues } = err else {
            panic!("expected a type validation error");
        };
        assert!(issues[0].contains("session-1"));
        assert!(issues[0].contains("Dep1"));
        assert!(issues[0].contains("Dep2"));
    }
    assert_eq!(consumer.state().state, FiberState::Pending);
    assert!(ctx.get_as::<Dep1>(key.clone()).is_none());

    ctx.provide_as(key, Arc::new(Dep1)).unwrap();
    soon(async { (&consumer).await }).await.unwrap();
    assert_eq!(consumer.state().state, FiberState::Active);
}

async fn apply_panic_regression() {
    let root = Ctx::root().unwrap();
    let cleanups = Arc::new(AtomicUsize::new(0));
    let sync_panic = root.plugin(simple("sync-panic", {
        let cleanups = cleanups.clone();
        move |ctx: &Ctx| {
            ctx.provide(Dep1).unwrap();
            let cleanups = cleanups.clone();
            ctx.effect(move || counting_effect(cleanups, None)).unwrap();
            panic!("apply failed before creating future")
        }
    }));
    let err = soon(async { (&sync_panic).await }).await.unwrap_err();
    assert!(format!("{err:?}").contains("apply failed before creating future"));
    assert_eq!(sync_panic.state().state, FiberState::Failed);
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert!(
        root.get::<Dep1>().is_none(),
        "registered service rolled back"
    );
    root.provide(Dep1).unwrap();
    soon(sync_panic.dispose()).await.unwrap();

    let root = Ctx::root().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let poll_panic = root.plugin(simple("poll-panic", {
        let started = started.clone();
        let release = release.clone();
        move |ctx: &Ctx| {
            let started = started.clone();
            let release = release.clone();
            Box::pin(async move {
                ctx.provide(Dep1).unwrap();
                started.notify_one();
                release.notified().await;
                panic!("apply failed while polling")
            })
        }
    }));
    let settle_view = poll_panic.clone();
    let settle = tokio::spawn(async move { (&settle_view).await });
    soon(started.notified()).await;
    release.notify_one();
    let err = soon(settle).await.unwrap().unwrap_err();
    assert!(format!("{err:?}").contains("apply failed while polling"));
    assert_eq!(poll_panic.state().state, FiberState::Failed);
    assert!(
        root.get::<Dep1>().is_none(),
        "poll panic service rolled back"
    );
    root.provide(Dep1).unwrap();
    soon(poll_panic.dispose()).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn apply_panics_complete_and_roll_back_current_thread() {
    apply_panic_regression().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_panics_complete_and_roll_back_multi_thread() {
    apply_panic_regression().await;
}

#[tokio::test]
async fn driver_panic_completes_waiters_with_error() {
    let root = Ctx::root_with_sink(
        tokio::runtime::Handle::current(),
        Arc::new(|_| panic!("error sink failed")),
    );
    let view = root.plugin(simple("panicking-sink", |_ctx: &Ctx| {
        Box::pin(async {
            Ok(Effect::Disposer(Box::new(|| {
                Err(CordisError::InactiveEffect)
            })))
        })
    }));
    soon(async { (&view).await }).await.unwrap();
    let err = soon(view.restart()).await.unwrap_err();
    assert!(format!("{err:?}").contains("error sink failed"));
    assert_eq!(view.state().state, FiberState::Failed);
    assert!(soon(view.dispose()).await.is_err());
    assert!(soon(view.restart()).await.is_err());
}

#[tokio::test]
async fn keyed_multi_instance() {
    use rutis::Key;
    const A: Key<LlmSvc> = Key::new("a");
    const B: Key<LlmSvc> = Key::new("b");
    let ctx = Ctx::root().unwrap();
    ctx.provide_as(A, Arc::new(LlmSvc { n: 1 })).unwrap();
    ctx.provide_as(B, Arc::new(LlmSvc { n: 2 })).unwrap();
    assert_eq!(ctx.get_as::<LlmSvc>(A).unwrap().n, 1);
    assert_eq!(ctx.get_as::<LlmSvc>(B).unwrap().n, 2);
    assert!(ctx.get::<LlmSvc>().is_none()); // 默认键空
}

#[tokio::test]
async fn isolate_scoping() {
    let key = TypeKey::of::<LlmSvc>();
    let ctx = Ctx::root().unwrap();
    let scope_a = ctx.isolate(key.clone(), "A");
    let scope_b = ctx.isolate(key.clone(), "B");
    scope_a.provide(LlmSvc { n: 10 }).unwrap();
    scope_b.provide(LlmSvc { n: 20 }).unwrap();
    // 同类型跨作用域并存(支柱 3)
    assert_eq!(scope_a.get::<LlmSvc>().unwrap().n, 10);
    assert_eq!(scope_b.get::<LlmSvc>().unwrap().n, 20);
    assert!(ctx.get::<LlmSvc>().is_none()); // 默认作用域不受影响
                                            // 同 label 合并作用域(TS 语义)
    let a_again = ctx.isolate(key.clone(), "A");
    assert_eq!(a_again.get::<LlmSvc>().unwrap().n, 10);
}

#[tokio::test]
async fn parent_chain_lookup() {
    let ctx = Ctx::root().unwrap();
    ctx.provide(Dep1).unwrap();
    // 对其它键 isolate 的子上下文:Dep1 沿父链回溯解析
    let child = ctx.isolate(TypeKey::of::<LlmSvc>(), "other");
    assert!(child.get::<Dep1>().is_some());
}

// ── reload(支柱 5)────────────────────────────────────────────────

#[tokio::test]
async fn eviction_order() {
    let ctx = Ctx::root().unwrap();
    let ord = order();
    let o1 = ord.clone();
    let provider = ctx.plugin(simple("P", move |ctx: &Ctx| {
        let o = o1.clone();
        Box::pin(async move {
            ctx.effect(move || marker_effect("P", o.clone()))?; // 先注册 → LIFO 最后跑
            ctx.provide(Dep1)?;
            Ok(Effect::Done)
        })
    }));
    (&provider).await.expect("P active");
    let o2 = ord.clone();
    let middle = ctx.plugin(simple_dep(
        "M",
        vec![TypeKey::of::<Dep1>()],
        move |ctx: &Ctx| {
            let o = o2.clone();
            Box::pin(async move {
                ctx.effect(move || marker_effect("M", o.clone()))?;
                ctx.provide(Dep2)?;
                Ok(Effect::Done)
            })
        },
    ));
    (&middle).await.expect("M active");
    let o3 = ord.clone();
    let consumer = ctx.plugin(simple_dep(
        "C",
        vec![TypeKey::of::<Dep2>()],
        move |ctx: &Ctx| {
            let o = o3.clone();
            Box::pin(async move {
                ctx.effect(move || marker_effect("C", o.clone()))?;
                Ok(Effect::Done)
            })
        },
    ));
    (&consumer).await.expect("C active");
    provider.dispose().await.unwrap();
    // 驱逐顺序:消费者并发排干、provider 最后(D14)
    assert_eq!(*ord.lock().unwrap(), vec!["C", "M", "P"]);
    assert_eq!(consumer.state().state, FiberState::Pending);
    assert_eq!(middle.state().state, FiberState::Pending);
}

#[tokio::test]
async fn self_access_during_cleanup() {
    let ctx = Ctx::root().unwrap();
    let seen: Arc<Mutex<Option<u32>>> = Arc::new(Mutex::new(None));
    let s = seen.clone();
    let view = ctx.plugin(simple("selfaccess", move |ctx: &Ctx| {
        let s = s.clone();
        let ctx2 = ctx.clone();
        Box::pin(async move {
            ctx2.provide(LlmSvc { n: 77 })?;
            // 后注册 → LIFO 先跑:此时 fiber 已 Unloading,普通 get 应拒绝,
            // 但 provider 子树内自访问仍有效(reflect.ts:297-303)
            let inner = ctx2.clone();
            ctx2.effect(move || {
                Effect::AsyncDisposer(Box::new(move || {
                    let s = s.clone();
                    let ctx3 = inner.clone();
                    Box::pin(async move {
                        *s.lock().unwrap() = ctx3.get::<LlmSvc>().map(|v| v.n);
                        Ok(())
                    })
                }))
            })?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    view.dispose().await.unwrap();
    assert_eq!(*seen.lock().unwrap(), Some(77));
}

#[tokio::test]
async fn reentrant_provide_fails() {
    let ctx = Ctx::root().unwrap();
    let captured: Arc<Mutex<Option<CordisError>>> = Arc::new(Mutex::new(None));
    let cap = captured.clone();
    let view = ctx.plugin(simple("reprov", move |ctx: &Ctx| {
        let cap = cap.clone();
        let ctx2 = ctx.clone();
        Box::pin(async move {
            ctx2.provide(LlmSvc { n: 1 })?;
            let inner = ctx2.clone();
            ctx2.effect(move || {
                Effect::AsyncDisposer(Box::new(move || {
                    let cap = cap.clone();
                    let ctx3 = inner.clone();
                    Box::pin(async move {
                        *cap.lock().unwrap() = ctx3.provide::<LlmSvc>(LlmSvc { n: 2 }).err();
                        Ok(())
                    })
                }))
            })?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    view.dispose().await.unwrap();
    assert!(matches!(
        captured.lock().unwrap().take().unwrap(),
        CordisError::InactiveEffect
    ));
}

#[tokio::test]
async fn isolate_no_cross_evict() {
    let key = TypeKey::of::<Dep1>();
    let ctx = Ctx::root().unwrap();
    let scope_a = ctx.isolate(key.clone(), "A");
    let scope_b = ctx.isolate(key.clone(), "B");
    let pa = scope_a.plugin(simple("PA", move |ctx: &Ctx| {
        Box::pin(async move {
            ctx.provide(Dep1)?;
            Ok(Effect::Done)
        })
    }));
    (&pa).await.expect("PA active");
    let pb = scope_b.plugin(simple("PB", move |ctx: &Ctx| {
        Box::pin(async move {
            ctx.provide(Dep1)?;
            Ok(Effect::Done)
        })
    }));
    (&pb).await.expect("PB active");
    let consumer = scope_b.plugin(simple_dep("C", vec![key.clone()], move |_ctx: &Ctx| {
        Box::pin(async { Ok(Effect::Done) })
    }));
    (&consumer).await.expect("C active via B");
    // A 作用域 provider 卸载:B 作用域消费者不受影响(D21 三元组精确驱逐)
    pa.dispose().await.unwrap();
    assert_eq!(consumer.state().state, FiberState::Active);
}

#[tokio::test]
async fn single_service_evict() {
    let ctx = Ctx::root().unwrap();
    // provider 一次装配提供两个服务,仅提前释放其中一个
    let d1 = Arc::new(Mutex::new(None::<rutis::Disposer>));
    let d1c = d1.clone();
    let provider = ctx.plugin(simple("multi", move |ctx: &Ctx| {
        let d1c = d1c.clone();
        Box::pin(async move {
            let d = ctx.provide(Dep1)?;
            *d1c.lock().unwrap() = Some(d);
            ctx.provide(Dep2)?;
            Ok(Effect::Done)
        })
    }));
    (&provider).await.expect("P active");
    let c1 = ctx.plugin(simple_dep(
        "C1",
        vec![TypeKey::of::<Dep1>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    let c2 = ctx.plugin(simple_dep(
        "C2",
        vec![TypeKey::of::<Dep2>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    (&c1).await.expect("C1");
    (&c2).await.expect("C2");
    // 提前释放 Dep1:只有 C1 被驱逐
    let disposer = d1.lock().unwrap().take().unwrap();
    disposer.dispose().await.unwrap();
    assert_eq!(c1.state().state, FiberState::Pending);
    assert_eq!(c2.state().state, FiberState::Active);
}

// ── cancel(D27)───────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn dispose_before_first_load_does_not_admit_a_generation() {
    let root = Ctx::root().unwrap();
    let applied = Arc::new(AtomicUsize::new(0));
    let rescue = Arc::new(tokio::sync::Notify::new());
    let view = root.plugin(simple("dispose-before-load", {
        let applied = applied.clone();
        let rescue = rescue.clone();
        move |ctx: &Ctx| {
            let applied = applied.clone();
            let rescue = rescue.clone();
            Box::pin(async move {
                applied.fetch_add(1, Ordering::SeqCst);
                tokio::select! {
                    _ = ctx.cancelled() => {},
                    _ = rescue.notified() => {},
                }
                Ok(Effect::Done)
            })
        }
    }));
    // No yield: RefreshDeps is queued, but the driver has not started load.
    let disposed = tokio::time::timeout(Duration::from_millis(250), view.dispose()).await;
    // Release a broken implementation's apply before asserting, so no driver
    // waiting forever on cancellation survives this regression test.
    rescue.notify_one();
    soon(view.dispose()).await.unwrap();
    disposed
        .expect("dispose before load must complete")
        .unwrap();
    assert_eq!(view.state().state, FiberState::Disposed);
    assert_eq!(view.state().generation, 0);
    assert_eq!(applied.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn dispose_after_load_admission_cancels_the_installed_token() {
    let root = Ctx::root().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let rescue = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(AtomicUsize::new(0));
    let view = root.plugin(simple("dispose-admitted-load", {
        let started = started.clone();
        let rescue = rescue.clone();
        let cancelled = cancelled.clone();
        move |ctx: &Ctx| {
            let started = started.clone();
            let rescue = rescue.clone();
            let cancelled = cancelled.clone();
            Box::pin(async move {
                started.notify_one();
                tokio::select! {
                    _ = ctx.cancelled() => {
                        cancelled.fetch_add(1, Ordering::SeqCst);
                    },
                    _ = rescue.notified() => {},
                }
                Ok(Effect::Done)
            })
        }
    }));
    soon(started.notified()).await;
    assert_eq!(view.state().generation, 1);
    let disposed = tokio::time::timeout(Duration::from_millis(250), view.dispose()).await;
    rescue.notify_one();
    soon(view.dispose()).await.unwrap();
    disposed
        .expect("dispose must cancel the admitted load")
        .unwrap();
    assert_eq!(cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(view.state().state, FiberState::Disposed);
}

#[tokio::test(flavor = "current_thread")]
async fn dispose_during_restart_cleanup_forbids_the_next_generation() {
    let root = Ctx::root().unwrap();
    let applied = Arc::new(AtomicUsize::new(0));
    let cleaning = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let rescue = Arc::new(tokio::sync::Notify::new());
    let view = root.plugin(simple("dispose-restart-boundary", {
        let applied = applied.clone();
        let cleaning = cleaning.clone();
        let release = release.clone();
        let rescue = rescue.clone();
        move |ctx: &Ctx| {
            let applied = applied.clone();
            let cleaning = cleaning.clone();
            let release = release.clone();
            let rescue = rescue.clone();
            Box::pin(async move {
                if applied.fetch_add(1, Ordering::SeqCst) != 0 {
                    tokio::select! {
                        _ = ctx.cancelled() => {},
                        _ = rescue.notified() => {},
                    }
                    return Ok(Effect::Done);
                }
                Ok(Effect::AsyncDisposer(Box::new(move || {
                    Box::pin(async move {
                        cleaning.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                })))
            })
        }
    }));
    soon(async { (&view).await }).await.unwrap();
    let generation = view.state().generation;
    let restart = tokio::spawn(view.restart());
    soon(cleaning.notified()).await;
    // Register the terminal request while restart's old generation drains.
    let dispose = view.dispose();
    release.notify_one();
    let disposed = tokio::time::timeout(Duration::from_millis(250), dispose).await;
    rescue.notify_one();
    soon(view.dispose()).await.unwrap();
    soon(restart).await.unwrap().unwrap();
    disposed
        .expect("dispose must prevent restart admission")
        .unwrap();
    assert_eq!(view.state().state, FiberState::Disposed);
    assert_eq!(view.state().generation, generation);
    assert_eq!(applied.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_wakes_awaiters() {
    let ctx = Ctx::root().unwrap();
    let exited = Arc::new(AtomicUsize::new(0));
    let e = exited.clone();
    let view = ctx.plugin(simple("waiter", move |ctx: &Ctx| {
        let e = e.clone();
        Box::pin(async move {
            ctx.cancelled().await; // 卸载时被取消唤醒
            e.fetch_add(1, Ordering::SeqCst);
            Ok(Effect::Done)
        })
    }));
    // apply 阻塞在 token 上:等它进入 Loading,再 dispose(预取消唤醒)
    reach(&view, FiberState::Loading).await;
    view.dispose().await.unwrap();
    assert_eq!(exited.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn disposal_deadline_reports_loading_and_later_joins() {
    let root = Ctx::root().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let view = root.plugin(simple("blocked-apply", {
        let started = started.clone();
        let release = release.clone();
        move |_ctx: &Ctx| {
            let started = started.clone();
            let release = release.clone();
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                Ok(Effect::Done)
            })
        }
    }));
    soon(started.notified()).await;
    let timeout = view
        .dispose_with_timeout(Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(matches!(
        timeout,
        DisposeWaitError::TimedOut {
            state: FiberState::Loading,
            ..
        }
    ));
    release.notify_one();
    soon(view.dispose()).await.unwrap();
    assert_eq!(view.state().state, FiberState::Disposed);
}

#[tokio::test]
async fn disposal_deadline_reports_async_cleanup_and_preserves_result() {
    let root = Ctx::root().unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let view = root.plugin(simple("blocked-cleanup", {
        let release = release.clone();
        move |_ctx: &Ctx| {
            let release = release.clone();
            Box::pin(async move {
                Ok(Effect::AsyncDisposer(Box::new(move || {
                    Box::pin(async move {
                        release.notified().await;
                        Err(CordisError::ServiceNotFound("cleanup failed".into()))
                    })
                })))
            })
        }
    }));
    soon(async { (&view).await }).await.unwrap();
    let timeout = view
        .dispose_with_timeout(Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(matches!(
        timeout,
        DisposeWaitError::TimedOut {
            state: FiberState::Unloading,
            ..
        }
    ));
    release.notify_one();
    let first = soon(view.dispose()).await.unwrap_err();
    let second = soon(view.dispose()).await.unwrap_err();
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn disposal_deadline_covers_cleanup_and_consumer_eviction() {
    let root = Ctx::root().unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let provider = root.plugin(simple("provider", |ctx: &Ctx| {
        Box::pin(async move {
            ctx.provide(Dep1)?;
            Ok(Effect::Done)
        })
    }));
    soon(async { (&provider).await }).await.unwrap();
    let consumer = root.plugin(simple_dep("consumer", vec![TypeKey::of::<Dep1>()], {
        let release = release.clone();
        move |_ctx: &Ctx| {
            let release = release.clone();
            Box::pin(async move {
                Ok(Effect::AsyncDisposer(Box::new(move || {
                    Box::pin(async move {
                        release.notified().await;
                        Ok(())
                    })
                })))
            })
        }
    }));
    soon(async { (&consumer).await }).await.unwrap();
    let timeout = provider
        .dispose_with_timeout(Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(matches!(
        timeout,
        DisposeWaitError::TimedOut {
            state: FiberState::Unloading,
            ..
        }
    ));
    release.notify_one();
    soon(provider.dispose()).await.unwrap();
    assert_eq!(consumer.state().state, FiberState::Pending);
}

#[tokio::test]
async fn root_shutdown_releases_driver_and_rejects_new_work() {
    for _ in 0..8 {
        let marker = Arc::new(());
        let weak = Arc::downgrade(&marker);
        let sink_marker = marker.clone();
        let sink = Arc::new(move |_error: Arc<CordisError>| {
            let _ = &sink_marker;
        });
        drop(marker);
        let ctx = Ctx::root_with_sink(tokio::runtime::Handle::current(), sink);
        let root = ctx.root_view().unwrap();
        ctx.provide(Dep1).unwrap();
        soon(ctx.shutdown()).await.unwrap();
        soon(ctx.shutdown()).await.unwrap();
        assert!(matches!(ctx.provide(Dep2), Err(CordisError::Closed)));
        assert!(matches!(
            ctx.effect(|| Effect::Done),
            Err(CordisError::Closed)
        ));
        let rejected = ctx.plugin(simple("late", |_ctx: &Ctx| {
            Box::pin(async { Ok(Effect::Done) })
        }));
        assert!(soon(async { (&rejected).await }).await.is_err());
        assert!(soon(root.restart()).await.is_err());
        soon(root.dispose()).await.unwrap();
        drop(rejected);
        drop(root);
        assert!(ctx.root_view().is_none());
        drop(ctx);
        assert!(weak.upgrade().is_none(), "shutdown leaked root Shared");
    }
}

#[tokio::test]
async fn shutdown_deadline_can_resume_after_noncooperative_apply() {
    let ctx = Ctx::root().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let view = ctx.plugin(simple("slow", {
        let started = started.clone();
        let release = release.clone();
        move |_ctx: &Ctx| {
            let started = started.clone();
            let release = release.clone();
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                Ok(Effect::Done)
            })
        }
    }));
    soon(started.notified()).await;
    let timeout = ctx
        .shutdown_with_timeout(Duration::from_millis(20))
        .await
        .unwrap_err();
    assert!(matches!(timeout, DisposeWaitError::TimedOut { .. }));
    assert!(soon(view.restart()).await.is_err());
    release.notify_one();
    soon(ctx.shutdown()).await.unwrap();
    assert_eq!(view.state().state, FiberState::Disposed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_shutdown_and_dispose_share_completion() {
    let ctx = Ctx::root().unwrap();
    let root = ctx.root_view().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    ctx.effect({
        let started = started.clone();
        let release = release.clone();
        move || {
            Effect::AsyncDisposer(Box::new(move || {
                Box::pin(async move {
                    started.notify_one();
                    release.notified().await;
                    Ok(())
                })
            }))
        }
    })
    .unwrap();
    let first = ctx.shutdown();
    let second = ctx.shutdown();
    let dispose = root.dispose();
    soon(started.notified()).await;
    assert!(soon(root.restart()).await.is_err());
    release.notify_one();
    let (a, b, c) = soon(async { tokio::join!(first, second, dispose) }).await;
    a.unwrap();
    b.unwrap();
    c.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shutdown_and_dispose_always_converge() {
    for iteration in 0..2000 {
        let ctx = Ctx::root().unwrap();
        let root = ctx.root_view().unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let shutdown = tokio::spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                ctx.shutdown().await
            }
        });
        let dispose = tokio::spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                root.dispose().await
            }
        });
        barrier.wait().await;
        let (shutdown, dispose) = tokio::time::timeout(Duration::from_millis(250), async {
            tokio::join!(shutdown, dispose)
        })
        .await
        .unwrap_or_else(|_| panic!("shutdown/dispose stalled in iteration {iteration}"));
        shutdown.unwrap().unwrap();
        dispose.unwrap().unwrap();
    }
}

#[tokio::test]
async fn cancel_during_loading() {
    let ctx = Ctx::root().unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let o = observed.clone();
    let view = ctx.plugin(simple("loading-wait", move |ctx: &Ctx| {
        let o = o.clone();
        Box::pin(async move {
            tokio::select! {
                _ = ctx.cancelled() => {
                    o.fetch_add(1, Ordering::SeqCst); // Loading 中被唤醒
                    Ok(Effect::Done)
                }
                _ = std::future::pending::<()>() => unreachable!(),
            }
        })
    }));
    // 等 apply 进入 Loading(其 select 挂起),再 dispose
    reach(&view, FiberState::Loading).await;
    soon(view.dispose()).await.unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_idempotent() {
    let ctx = Ctx::root().unwrap();
    ctx.provide(Dep1).unwrap();
    let token = ctx.cancellation_token();
    token.cancel();
    token.cancel(); // 多次 cancel 幂等
    ctx.cancelled().await; // 等待者仍被唤醒
}

#[tokio::test]
async fn cancel_cascades() {
    let ctx = Ctx::root().unwrap();
    let child_exited = Arc::new(AtomicUsize::new(0));
    let child_started = Arc::new(tokio::sync::Notify::new());
    let ce = child_exited.clone();
    let cs = child_started.clone();
    let parent = ctx.plugin(simple("parent", move |ctx: &Ctx| {
        let ce = ce.clone();
        let cs = cs.clone();
        Box::pin(async move {
            ctx.plugin(simple("child", move |cctx: &Ctx| {
                let ce = ce.clone();
                let cs = cs.clone();
                Box::pin(async move {
                    cs.notify_one();
                    cctx.cancelled().await; // 父卸载级联取消子代
                    ce.fetch_add(1, Ordering::SeqCst);
                    Ok(Effect::Done)
                })
            }));
            Ok(Effect::Done)
        })
    }));
    (&parent).await.expect("parent load");
    soon(child_started.notified()).await; // 子 fiber 已在 apply 中
    parent.dispose().await.unwrap();
    assert_eq!(child_exited.load(Ordering::SeqCst), 1);
}

// ── 评审修复补测(review-rust-impl-2026-08-17)──────────────────────

// #1:apply 失败回滚半注册资源(对齐 TS 失败路径经 UNLOADING 排干)
#[tokio::test]
async fn apply_failure_rolls_back() {
    let ctx = Ctx::root().unwrap();
    let (hits, _done) = recorder();
    let child_slot: Arc<Mutex<Option<FiberView>>> = Arc::new(Mutex::new(None));
    let h = hits.clone();
    let slot = child_slot.clone();
    let view = ctx.plugin(simple("half-fail", move |ctx: &Ctx| {
        let h = h.clone();
        let slot = slot.clone();
        Box::pin(async move {
            ctx.provide(LlmSvc { n: 1 })?;
            ctx.events().on(
                ctx,
                &rutis::EventKey::of(),
                Rec {
                    hits: h,
                    bail: None,
                    err: false,
                    notify: None,
                    gate: None,
                },
            )?;
            let child = ctx.plugin(simple("inner", move |_c: &Ctx| {
                Box::pin(async { Ok(Effect::Done) })
            }));
            *slot.lock().unwrap() = Some(child);
            Err(CordisError::ServiceNotFound("half fail".into()))
        })
    }));
    let err = (&view).await.expect_err("apply fails");
    assert!(matches!(*err, CordisError::ServiceNotFound(_)));
    assert_eq!(view.state().state, FiberState::Failed);

    // 服务占位已回滚:同键可再注册
    ctx.provide(LlmSvc { n: 2 }).unwrap();
    // 监听器已随回滚卸载:不再收事件
    ctx.events()
        .emit(&ctx, &rutis::EventKey::of(), Arc::new(Ping { value: 1 }))
        .expect("default event dispatch");
    drained(&ctx).await;
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    // 子插件已级联处置
    let child = child_slot.lock().unwrap().take().unwrap();
    assert_eq!(child.state().state, FiberState::Disposed);
}

// #2:dispose 已入队时 restart 立即拒绝,不挂起
#[tokio::test]
async fn restart_after_queued_dispose_rejected() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple("racy", move |_ctx: &Ctx| {
        Box::pin(async { Ok(Effect::Done) })
    }));
    (&view).await.expect("load");
    // 调用即登记终态任务(不依赖 future 被 poll):随后的 restart 必须立刻拒绝
    let dispose_fut = view.clone().dispose();
    let err = soon(view.restart()).await.expect_err("restart rejected");
    assert!(matches!(*err, CordisError::InactiveEffect));
    dispose_fut.await.unwrap();
    assert_eq!(view.state().state, FiberState::Disposed);
}

// #3:消费者先行终止后,provider 摘除不得永等
#[tokio::test]
async fn evict_after_consumer_disposed_completes() {
    let ctx = Ctx::root().unwrap();
    let d1: Arc<Mutex<Option<rutis::Disposer>>> = Arc::new(Mutex::new(None));
    let dc = d1.clone();
    let provider = ctx.plugin(simple("P", move |ctx: &Ctx| {
        let dc = dc.clone();
        Box::pin(async move {
            let d = ctx.provide(Dep1)?;
            *dc.lock().unwrap() = Some(d);
            Ok(Effect::Done)
        })
    }));
    (&provider).await.expect("P");
    let consumer = ctx.plugin(simple_dep(
        "C",
        vec![TypeKey::of::<Dep1>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    (&consumer).await.expect("C");
    // 并发:消费者 dispose 与服务提前释放竞争
    let c = consumer.clone();
    let d = d1.clone();
    let a = tokio::spawn(async move { c.dispose().await });
    let b = tokio::spawn(async move {
        let d = { d.lock().unwrap().take().unwrap() };
        d.dispose().await
    });
    let (ra, rb) = soon(async { tokio::join!(a, b) }).await;
    ra.unwrap().unwrap();
    rb.unwrap().unwrap();
    assert_eq!(consumer.state().state, FiberState::Disposed);
}

// #4:Disposer future 被 drop 不卡死(清理在独立任务完成)
#[tokio::test]
async fn disposer_drop_is_cancel_safe() {
    let ctx = Ctx::root().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let done = Arc::new(tokio::sync::Notify::new());
    let (c, s, r, dn) = (
        counter.clone(),
        started.clone(),
        release.clone(),
        done.clone(),
    );
    let d = ctx
        .effect(move || {
            Effect::AsyncDisposer(Box::new(move || {
                let (c, s, r, dn) = (c.clone(), s.clone(), r.clone(), dn.clone());
                Box::pin(async move {
                    s.notify_one();
                    r.notified().await;
                    c.fetch_add(1, Ordering::SeqCst);
                    dn.notify_one();
                    Ok(())
                })
            }))
        })
        .unwrap();
    // 清理开始后中途丢弃 dispose future
    let mut disposing = Box::pin(d.dispose());
    soon(async {
        tokio::select! {
            _ = &mut disposing => panic!("dispose finished while its cleanup was held"),
            _ = started.notified() => {}
        }
    })
    .await;
    drop(disposing);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    // 独立清理任务继续完成
    release.notify_one();
    soon(done.notified()).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    // fiber 卸载 join 已缓存终态,不重跑
    ctx.root_view().unwrap().dispose().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

// #6:validate panic → Failed,不杀驱动
struct PanicValidate;
impl Plugin for PanicValidate {
    fn name(&self) -> &str {
        "panic-validate"
    }
    fn validate(&self) -> Result<(), CordisError> {
        panic!("validate boom")
    }
    fn apply<'a>(&'a self, _ctx: &'a Ctx) -> BoxFuture<'a, Result<Effect, CordisError>> {
        Box::pin(async { Ok(Effect::Done) })
    }
}

#[tokio::test]
async fn validate_panic_marks_failed() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(PanicValidate);
    let err = soon(async { (&view).await })
        .await
        .expect_err("panic → Failed");
    assert!(matches!(*err, CordisError::PluginFailed(_)));
    assert_eq!(view.state().state, FiberState::Failed);
    view.dispose().await.unwrap(); // 句柄仍可用
}

// #6:check() panic 视为不就绪,refresh 不挂起
#[tokio::test]
async fn check_panic_treated_unsatisfied() {
    let ctx = Ctx::root().unwrap();
    ctx.provide_as_with_check(TypeKey::of::<LlmSvc>(), Arc::new(LlmSvc { n: 1 }), || {
        panic!("check boom")
    })
    .unwrap();
    let view = ctx.plugin(simple_dep(
        "gated",
        vec![TypeKey::of::<LlmSvc>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    soon(async {
        // 消费者因 check panic 保持 Pending;refresh 往返不挂起
        (&view).await.expect("Pending settle is Ok");
    })
    .await;
    ctx.refresh();
    soon(async { (&view).await })
        .await
        .expect("refresh completes");
    assert_eq!(view.state().state, FiberState::Pending);
}

// #7:清理闭包调用 panic 与 future poll panic 都被兜住
#[tokio::test]
async fn cleanup_panics_contained() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple("panicky", move |ctx: &Ctx| {
        Box::pin(async move {
            // f() 调用时 panic
            ctx.effect(move || {
                Effect::AsyncDisposer(Box::new(move || panic!("closure creation boom")))
            })?;
            // future poll 时 panic
            ctx.effect(move || {
                Effect::AsyncDisposer(Box::new(move || Box::pin(async { panic!("poll boom") })))
            })?;
            Ok(Effect::Done)
        })
    }));
    (&view).await.expect("load");
    let err = view.dispose().await.expect_err("both panics aggregated");
    match &*err {
        CordisError::Aggregate { errors } => {
            assert_eq!(errors.len(), 2);
            assert!(errors
                .iter()
                .all(|e| matches!(**e, CordisError::PluginFailed(_))));
        }
        other => panic!("expected aggregate, got {other:?}"),
    }
}

// #9:并发同键 provide 恰好一个成功
#[tokio::test]
async fn concurrent_provide_single_winner() {
    let ctx = Ctx::root().unwrap();
    let a = {
        let c = ctx.clone();
        tokio::spawn(async move { c.provide(LlmSvc { n: 1 }) })
    };
    let b = {
        let c = ctx.clone();
        tokio::spawn(async move { c.provide(LlmSvc { n: 2 }) })
    };
    let (ra, rb) = tokio::join!(a, b);
    let results = [ra.unwrap(), rb.unwrap()];
    let oks = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(oks, 1);
    let err = results
        .iter()
        .find(|r| r.is_err())
        .unwrap()
        .as_ref()
        .unwrap_err();
    assert!(matches!(err, CordisError::ServiceExists(_)));
    assert!(ctx.get::<LlmSvc>().is_some());
}

// #5(加固):serial 按注册序短路——前慢 Some、后快 Some
#[tokio::test]
async fn serial_register_order_adversarial() {
    struct SlowBail {
        value: u32,
        turns: usize,
        hits: Arc<AtomicUsize>,
    }
    impl Listener<Ping> for SlowBail {
        fn call<'a>(
            &'a self,
            _c: &'a Ctx,
            e: &'a Ping,
        ) -> BoxFuture<'a, Result<Option<u32>, CordisError>> {
            let hits = self.hits.clone();
            let bail = self.value;
            let n = self.turns;
            let base = e.value;
            Box::pin(async move {
                turns(n).await;
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(Some(bail + base))
            })
        }
    }
    let ctx = Ctx::root().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            SlowBail {
                value: 1,
                turns: 20,
                hits: hits.clone(),
            },
        )
        .unwrap();
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            SlowBail {
                value: 2,
                turns: 0,
                hits: hits.clone(),
            },
        )
        .unwrap();
    let out = soon(
        ctx.events()
            .serial(&ctx, &rutis::EventKey::of(), &Ping { value: 0 }),
    )
    .await
    .unwrap();
    // 若按完成序,快的(2)会先短路;注册序语义必须返回慢者的 1
    assert_eq!(out, Some(1));
    assert_eq!(hits.load(Ordering::SeqCst), 1); // 后注册者未执行
}

// 支柱 5 后半截:provider 回来后消费者自动重载
#[tokio::test]
async fn provider_reload_reactivates() {
    let ctx = Ctx::root().unwrap();
    let provider = ctx.plugin(simple("P", move |ctx: &Ctx| {
        Box::pin(async move {
            ctx.provide(Dep1)?;
            Ok(Effect::Done)
        })
    }));
    (&provider).await.expect("P");
    let consumer = ctx.plugin(simple_dep(
        "C",
        vec![TypeKey::of::<Dep1>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    (&consumer).await.expect("C active");
    provider.dispose().await.unwrap();
    assert_eq!(consumer.state().state, FiberState::Pending);
    // 新 provider 就位:消费者自动回来
    let p2 = ctx.plugin(simple("P2", move |ctx: &Ctx| {
        Box::pin(async move {
            ctx.provide(Dep1)?;
            Ok(Effect::Done)
        })
    }));
    (&p2).await.expect("P2");
    soon(async {
        let mut rx = consumer.watch();
        loop {
            if rx.borrow().state == FiberState::Active {
                break;
            }
            rx.changed().await.unwrap();
        }
    })
    .await;
}

// check() 谓词恢复后消费者重新激活
#[tokio::test]
async fn check_recovery_reactivates() {
    let ctx = Ctx::root().unwrap();
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let f1 = flag.clone();
    ctx.provide_as_with_check(TypeKey::of::<Dep1>(), Arc::new(Dep1), move || {
        f1.load(Ordering::SeqCst)
    })
    .unwrap();
    let consumer = ctx.plugin(simple_dep(
        "C",
        vec![TypeKey::of::<Dep1>()],
        move |_ctx: &Ctx| Box::pin(async { Ok(Effect::Done) }),
    ));
    (&consumer).await.expect("active");
    flag.store(false, Ordering::SeqCst);
    ctx.refresh();
    soon(async {
        let mut rx = consumer.watch();
        loop {
            if rx.borrow().state == FiberState::Pending {
                break;
            }
            rx.changed().await.unwrap();
        }
    })
    .await;
    flag.store(true, Ordering::SeqCst);
    ctx.refresh();
    soon(async {
        let mut rx = consumer.watch();
        loop {
            if rx.borrow().state == FiberState::Active {
                break;
            }
            rx.changed().await.unwrap();
        }
    })
    .await;
}

// parallel 全等语义:早到的错误也要等全部完成
#[tokio::test]
async fn parallel_waits_all() {
    struct Delayed {
        turns: usize,
        err: bool,
        hits: Arc<AtomicUsize>,
    }
    impl Listener<Ping> for Delayed {
        fn call<'a>(
            &'a self,
            _c: &'a Ctx,
            _e: &'a Ping,
        ) -> BoxFuture<'a, Result<Option<u32>, CordisError>> {
            let hits = self.hits.clone();
            let err = self.err;
            let n = self.turns;
            Box::pin(async move {
                turns(n).await;
                hits.fetch_add(1, Ordering::SeqCst);
                if err {
                    Err(CordisError::ServiceNotFound("fast".into()))
                } else {
                    Ok(None)
                }
            })
        }
    }
    let ctx = Ctx::root().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Delayed {
                turns: 0,
                err: true,
                hits: hits.clone(),
            },
        )
        .unwrap();
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Delayed {
                turns: 20,
                err: false,
                hits: hits.clone(),
            },
        )
        .unwrap();
    let err = soon(ctx.events().parallel(
        &ctx,
        &rutis::EventKey::of(),
        Arc::new(Ping { value: 0 }),
    ))
    .await
    .unwrap_err();
    assert!(matches!(err, CordisError::ServiceNotFound(_))); // 单错原样
    assert_eq!(hits.load(Ordering::SeqCst), 2); // 慢者也已全部完成
}

// serial 内联执行的 panic 遏制:监听器 panic 转 PluginFailed,不击穿分发者
#[tokio::test]
async fn serial_panic_contained() {
    struct BoomL;
    impl Listener<Ping> for BoomL {
        fn call<'a>(
            &'a self,
            _c: &'a Ctx,
            _e: &'a Ping,
        ) -> BoxFuture<'a, Result<Option<u32>, CordisError>> {
            Box::pin(async { panic!("serial boom") })
        }
    }
    let ctx = Ctx::root().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    ctx.events()
        .on(&ctx, &rutis::EventKey::of(), BoomL)
        .unwrap();
    ctx.events()
        .on(
            &ctx,
            &rutis::EventKey::of(),
            Rec {
                hits: hits.clone(),
                bail: None,
                err: false,
                notify: None,
                gate: None,
            },
        )
        .unwrap();
    let err = soon(
        ctx.events()
            .serial(&ctx, &rutis::EventKey::of(), &Ping { value: 1 }),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CordisError::PluginFailed(_)));
    assert_eq!(hits.load(Ordering::SeqCst), 0); // panic 后不再继续
}

// P2:非终止卸载的清理错误路由 ErrorSink,不改变下一代状态
#[tokio::test]
async fn restart_cleanup_errors_to_sink() {
    let sink_errors: Arc<Mutex<Vec<Arc<CordisError>>>> = Arc::new(Mutex::new(Vec::new()));
    let sink_to = sink_errors.clone();
    let ctx = Ctx::root_with_sink(
        tokio::runtime::Handle::current(),
        Arc::new(move |e| sink_to.lock().unwrap().push(e)),
    );
    let view = ctx.plugin(simple("dirty-restart", move |_ctx: &Ctx| {
        Box::pin(async {
            Ok(Effect::Disposer(Box::new(|| {
                Err(CordisError::InactiveEffect)
            })))
        })
    }));
    (&view).await.expect("load");
    // restart:卸载清理失败,但 restart 本身成功、错误进 sink
    view.restart()
        .await
        .expect("restart Ok, cleanup error to sink");
    eventually(|| !sink_errors.lock().unwrap().is_empty()).await;
    assert!(matches!(
        *sink_errors.lock().unwrap()[0],
        CordisError::InactiveEffect
    ));
    assert_eq!(view.state().state, FiberState::Active); // 重载成功
                                                        // 终止卸载的清理错误走 dispose() 自己的通道(与 restart 路由不同)
    let err = view
        .dispose()
        .await
        .expect_err("terminal unload reports cleanup error");
    assert!(matches!(*err, CordisError::InactiveEffect));
}

// Settle 栅栏:并发 settle/restart/dispose 混合投递,全部完成无挂起
#[tokio::test]
async fn mixed_concurrent_ops_complete() {
    let ctx = Ctx::root().unwrap();
    let view = ctx.plugin(simple("storm", move |_ctx: &Ctx| {
        Box::pin(async { Ok(Effect::Done) })
    }));
    (&view).await.expect("load");
    let mut handles = Vec::new();
    for i in 0..4 {
        let v = view.clone();
        handles.push(tokio::spawn(async move {
            match i % 4 {
                0 => {
                    let _ = (&v).await;
                }
                1 => {
                    let _ = v.restart().await;
                }
                _ => {
                    let _ = (&v).await;
                }
            }
        }));
    }
    let v = view.clone();
    handles.push(tokio::spawn(async move {
        let _ = v.dispose().await;
    }));
    soon(async {
        for h in handles {
            let _ = tokio::time::timeout(Duration::from_secs(2), h)
                .await
                .expect("op completed");
        }
    })
    .await;
    assert_eq!(view.state().state, FiberState::Disposed);
}
