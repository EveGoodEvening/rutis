# Design: Config Hot Updates (D32) and Dynamic Event Keys (D33)

> 2026-09-21. Scope: two independent features that can be implemented, committed, and rolled back independently or in parallel.
> D32 = rutis adaptation of Cordis `fiber.update(config)` (core M4); D33 = relax TypeKey qualifiers + add keyed event-bus APIs + bridge event path (the former M3 event path).
> References: lazy config resolution in the dsh fork (vendor README change #15, port of cordiverse/cordis#41), `internal/update` semantics, and upstream Cordis string events.

## 0. Motivation

1. **D32:** The capability most relied on in production with Cordis is changing plugin config while running and restarting automatically to apply it. The full dsh “edit YAML → hot-update plugin” flow depends on this. rutis currently bakes config into the instance (D19), so changing config requires unload and reinstall.
2. **D33:** Events sent by the dsh host through `evt/emit` use runtime strings (`session/event`, `agent/*`); Rust currently can only log them to stderr (`rutis_dsh.rs:136`) and cannot subscribe by name. The event bus registration key is a bare `TypeId` (`bus.rs:184/189/231/238`), so multiple channels for one type are impossible.

The features are orthogonal. The “name → factory” registry needed for dynamic plugin loading is outside this design (see §5).

---

## 1. D32: Config Hot Updates

### 1.1 Semantic mapping (Cordis → rutis)

| Cordis semantics | rutis mapping | Cost |
|---|---|---|
| `fiber.update(config)`: validate, then restart | Store config + reuse `Intent::Restart`; existing state machine guarantees exactly-once cleanup and waits before reload | No new transaction logic |
| Config may reference injected services and is resolved only after injection becomes active (fork's `internal/config` waterfall) | Factory is pure (`config → instance`, does not receive ctx); **resolve service references inside apply**. `apply` runs after dependency gating, so timing is equivalent | No `internal/config` equivalent needed |
| Provider replacement → resolve config again | Dependency-driven reload already exists: unload clears `last_deps` → load runs factory again using current config | Free |
| Pending update preserves raw config | Config lives on fiber; Pending waits for gating, then loads naturally with the new value | Free |
| Validation failure before update leaves current state unchanged | Dry-run (see D32b): on failure return Err without storing or restarting | New |
| `internal/update` waterfall (persistence hook `noSave`) | Omit; host layer listens to `FiberStatusChanged` and manages persistence itself | Explicitly trimmed |

### 1.2 State matrix (behavior of update in each state)

Unified path: **store config → cancel_current → post_join(Restart) → join**.

| Current state | Behavior | Basis |
|---|---|---|
| Active | unload (clear `last_deps`) → refresh_deps → load (new config) | Restart branch (`fiber.rs:505-508`) |
| Loading | cancel current generation → let apply exit cooperatively → unload → Pending → load (new config) | Same |
| Pending | Skip unload; refresh_deps: load with new config if dependencies are ready; otherwise remain Pending, **config is already stored and will be used automatically when providers arrive** | Restart branch (`fiber.rs:505`) |
| Failed | Same as Active (restart from Failed and apply new config), enabling config repair of a failed plugin | Same |
| Unloading (in progress) | Queue behind serialized driver; converges as above | Mailbox FIFO |
| Disposed / terminal_task registered / driver exited | Reject with `InactiveEffect` | Existing restart check (`fiber.rs:685-688`) |

Equal-dependency merging cannot swallow an update: unload clears `last_deps`, and refresh_deps cannot compare equal when `last_deps == None`, so it must load (`fiber.rs:305-315`). Config is not part of dependency identity (D32e); its change is represented by restart generation +1. Consumers of the fiber's services observe `provider_gen` and are automatically evicted/reloaded—at no extra cost.

### 1.3 API sketch

```rust
// Added to plugin.rs (after D32f revision)
/// Factory: construct a plugin instance for each generation from current config.
/// build must be pure construction (D32b contract): no side effects or idempotent,
/// because dry-run and load each call it once.
pub trait PluginFactory<C: Send + Sync + 'static>: Send + Sync + 'static {
    /// Dependency-gating declaration (static, registered once at spawn and
    /// immutable for life, symmetric with Plugin::injects; originally derived
    /// from injects(&config), revoked by D32f; see §11).
    fn injects(&self) -> &[TypeKey] { &[] }
    /// Config-level validation without constructing an instance.
    fn validate_config(&self, _config: &C) -> Result<(), CordisError> { Ok(()) }
    /// Construct plugin instance. Failure means config cannot produce a usable instance.
    fn build(&self, config: &C) -> Result<Box<dyn Plugin>, CordisError>;
}

// Added to ctx.rs (existing plugin() path unchanged; no migration cost)
impl Ctx {
    pub fn plugin_with<C: Send + Sync + 'static>(
        &self, factory: impl PluginFactory<C>, config: C,
    ) -> FiberView;

    /// Closure convenience form (equivalent to a one-method factory).
    pub fn plugin_from<C: Send + Sync + 'static>(
        &self,
        build: impl Fn(&C) -> Result<Box<dyn Plugin>, CordisError> + Send + Sync + 'static,
        config: C,
    ) -> FiberView;
}

// Added to FiberView in fiber.rs
impl FiberView {
    /// Hot update: after dry-run succeeds, store and restart. join returns restart's terminal result.
    pub fn update<C: Send + Sync + 'static>(
        &self, new_config: C,
    ) -> BoxFuture<'static, Result<(), Arc<CordisError>>>;

    /// Current config snapshot (diagnostics; returns None for a type mismatch).
    pub fn current_config<C: Send + Sync + 'static>(&self) -> Option<Box<C>>;
}
```

### 1.4 Internal representation

- Add two fields to `FiberInner`:
  ```rust
  factory: Option<Arc<dyn ErasedFactory>>,          // factory mode only
  config: Mutex<Option<Box<dyn Any + Send + Sync>>>, // factory mode only
  ```
- Internal `ErasedFactory`: `config_type_id() / injects_erased(&dyn Any) -> Vec<TypeKey> / validate_config_erased(&dyn Any) / build_erased(&dyn Any) -> Result<Box<dyn Plugin>>`. Erase generic `PluginFactory<C>` through a wrapper type (the same approach as `StoredValue` in registry).
- **Both modes coexist (D32a):** leave static `plugin()` mode (`FiberInner.plugin: Some(..)`) unchanged; in factory mode the `plugin` field is None. `load()` obtains the instance through `this.current_plugin()`: static mode clones the existing instance, factory mode calls `factory.build(current_config)` (failure goes through `fail_load`, preserving atomicity for failed assembly). Reuse the downstream validate/apply path unchanged.
- **Update flow** (expanded from the state matrix):
  1. Under transition lock, check `terminal_task` / Disposed / driver exit; reject if terminal.
  2. Match `TypeId::of::<C>()` with `factory.config_type_id()`; mismatch → `Validation` error.
  3. Dry-run: `validate_config(&new)` → `build(&new)` → instance `validate()` (D32b). Any failure returns Err and **does not store or restart**.
  4. Store config.
  5. `cancel_current()` + `post_join(Restart)` + `join_task`.

### 1.5 Concurrency semantics

| Scenario | Convergence |
|---|---|
| update × update | Mailbox FIFO; later update overwrites config. Each join waits until its own Restart has been processed |
| update × dispose | If terminal_task is registered first, update rejects. If update was registered first, disposal still prevents an unadmitted restart generation, cancels any already-admitted generation's token, and waits for old-generation cleanup before completing |
| update × eviction (`RefreshDepsJoin`) | Mailbox order decides which runs first; both converge to reload using current config, with no race window |
| update × apply in progress | `cancel_current()` requests cooperative exit (same as existing restart semantics; D27 remains: apply that ignores token can wait forever) |

### 1.6 Decision points

- **D32a, keep both modes:** do not change `plugin()` (zero migration; 115 tests unchanged). Consider unifying via identity factory in a later simplification batch; not this release.
- **D32b, dry-run = `validate_config` + `build` + instance `validate`;** discard the constructed instance. `build` must be pure construction (factory contract; violating it makes dry-run and live execution differ, at the implementer's own risk). This exposes errors early instead of discovering that config cannot build halfway through a restart, at the cost of calling build twice.
- **D32c:** state matrix as in §1.2 (aligned with Cordis and fork behavior in non-ACTIVE states).
- **D32d:** no `internal/update` waterfall; persistence belongs to host layer. There is no equivalent of `noSave`.
- **D32e:** config is not part of dependency identity; restart changes generation and consumers use `provider_gen`. This matches Cordis's four-part tuple semantics.

### 1.7 Test plan (contract tests)

1. Update while Active: new config takes effect (provided service value changes), generation +1, old generation cleanup exactly once (assert LIFO order).
2. Three dry-run failures (`validate_config` / `build` / instance `validate`): return Err; state and services remain unchanged.
3. Update while Pending: update before dependency arrives; after provider appears, load uses **new** config.
4. Update while Failed: repair config and restart successfully to Active.
5. Consumer reload after update: provider update automatically evicts dependent consumer and reloads it with its current config.
6. Concurrent update × dispose and update × update (FIFO and both joins settle correctly).
7. Factory-mode `injects` (static declaration) gating: missing dependency keeps it Pending; apply does not run.
8. Update with wrong config type returns Validation (the test also asserts rejection by a static fiber, which is the same designed Validation path).
9. Dependency-driven reload rebuilds with current config (provider removed then provided again; reload still uses current config; eviction after update uses the new config).
10. `current_config` snapshot and mismatched-type path.

**Review additions (§8 item 2):** 11. Update while Loading (apply is running; cooperatively cancel, then reload new config); 12. Update while Unloading (queue behind slow cleanup, then provide dependency again and load with new config); 13. Update × eviction (provider generation change races consumer hot update; both converge); 15. build panic → Failed while driver remains alive and join does not hang.

**Additions from rounds two (§9) and three (§10), items 14/16/17/18 (injects drift/panic/recovery family), were deleted together with D32f's revocation.** They tested defect classes in a removed mechanism; without that mechanism, those defects do not exist (§11).

---

## 2. D33: Dynamic Event Keys

### 2.1 Current state and approach

Bus registrations use bare `TypeId`: `hooks: HashMap<TypeId, ..>`, `wf_hooks`, and `dispatch_tail` (`bus.rs:107-113`). **Do not copy Cordis's string-event surface** (implementing a string variant of all four dispatch modes would double the semantic surface and lose type safety). Instead, bring the existing TypeKey mechanism for service keys into the event bus: once qualifiers accept dynamic strings, one typed event type plus a dynamic key can represent “event names known only at runtime.” All four dispatch modes, fiber-lifecycle cleanup, once, and prepend are inherited for free.

### 2.2 Relax TypeKey qualifiers (`key.rs`)

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Qualifier {
    Static(&'static str),   // Existing path, zero allocation
    Dynamic(Arc<str>),      // Runtime name
}

pub struct TypeKey {
    type_id: TypeId,
    qualifier: Option<Qualifier>,
}

impl TypeKey {
    pub fn keyed<T: ?Sized + 'static>(qualifier: &'static str) -> Self;          // Unchanged, uses Static
    pub fn keyed_dynamic<T: ?Sized + 'static>(name: impl Into<Arc<str>>) -> Self; // New, uses Dynamic
}
```

- Hash/Eq use string contents: `Static("foo") == Dynamic("foo")`; they interoperate in lookups.
- **Why not Cow or all-Arc:** cloning `Cow<String>` copies bytes each time; converting a static string to `Arc<str>` also allocates. The dual-variant enum keeps existing `keyed::<T>("name")` and `Key<T>` constants unchanged and allocation-free; only dynamic paths pay the Arc cost.
- **TypeKey loses `Copy`** because the enum contains Arc. Affected code such as `provided` iteration (`map(|(k,_)| *k)` at fiber.rs:372-378) and `notify_key_changed` by-value calls must use `clone()`. TypeKey clones are on load/registration paths and infrequent. Audit every use in implementation; `clippy -D warnings` guards against omissions.

### 2.3 Keyed event-bus surface (`bus.rs` / `event.rs`)

Change the three registration-key fields in `BusInner` from `TypeId` to `TypeKey` (`hooks` / `wf_hooks` / `dispatch_tail`). **Do not change existing API signatures** (all 115 tests remain unchanged); add keyed variants:

```rust
impl EventBus {
    // Registration surface
    pub fn on_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                              l: impl Listener<E>) -> Result<Disposer, CordisError>;
    pub fn on_keyed_opt<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                                  l: impl Listener<E>, opts: EventOptions) -> Result<Disposer, CordisError>;
    pub fn once_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                                l: impl Listener<E>) -> Result<Disposer, CordisError>;
    pub fn on_waterfall_keyed<E: Event>(&self, ctx: &Ctx, name: impl Into<Arc<str>>,
                                        l: impl WaterfallListener<E>) -> Result<Disposer, CordisError>;

    // Full four-mode dispatch surface; signatures align with unkeyed forms plus name
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

- Type is encoded when constructing the key: `keyed_dynamic::<E>(name)` gets its TypeId from E. The embedded `type_id` provides type isolation—**a mismatched key cannot be constructed through the public API** (TypeKey fields are private). A downcast-mismatch fallback in the listener adapter is practically unreachable. Review note 2: the draft's “take_hooks validates type_id and fails fast” overstates the defensive layer; the actual guarantee is this key encoding.
- Every keyed variant converts internally to TypeKey and follows the same registration/snapshot/dispatch path as unkeyed APIs (unkeyed is the `qualifier = None` case); do not duplicate four implementations.
- **D31 tail key follows TypeKey:** same-type names with identical spelling across Static/Dynamic share a dispatch chain; different names do not preserve order. Keep the existing boundary statement.

### 2.4 HostEvent and bridge event path (`rutis-cordis` / `rutis-dsh`)

Define HostEvent in **rutis-cordis** (preserving the kernel's no-serde boundary):

```rust
/// Host-forwarded event, translated from evt/emit. The name is duplicated in
/// the payload (the qualifier is in the registration key, and listeners read
/// the name from the event value). serial/waterfall use Value so the host can short-circuit.
pub struct HostEvent {
    pub name: String,
    pub payload: serde_json::Value,
    pub origin: EventOrigin, // Added in review: forward three reserved fields
}
impl Event for HostEvent {
    const NAME: &'static str = "host/*";
    type Value = serde_json::Value;
}
```

- **Implementation form** (review 2 note: differs from draft): do not add an `InboundHooks.on_event` field, and do not make `Bridge::start` own Ctx. Instead, use pure function `forward_host_events(ctx, observe) -> NotifyHook`, composed by the runner into `on_notify` (bridge stays unaware of rutis runtime; cleaner layering).
- In the pump's `dispatch_notify`, route `evt/emit` through this hook to call `ctx.events().emit_keyed::<HostEvent>(name, HostEvent{..})`; other notifications retain current behavior.
- Three-field forwarding (`scopeId/sessionId/turnId`, reserved in rpc.rs) is **settled by review**: standalone `EventOrigin{scope_id, session_id, turn_id}`; the pump constructs it when unpacking Ntf and passes it through `NotifyHook(method, params, origin)`; `HostEvent.origin` lets subscribers distinguish sessions/turns sharing an event name.
- Subscriber usage: `ctx.events().on_keyed::<HostEvent>("session/event", |ctx, e| ..)`; registration is automatically removed when its owner fiber unloads (D28 unchanged).

### 2.5 Decision points

- **D33a:** dual-variant Qualifier enum (Static has zero allocation), not Cow or all-Arc; audit the effects of TypeKey losing Copy in the implementation commit.
- **D33b:** put HostEvent in rutis-cordis; do not add serde to kernel; payload uses `serde_json::Value` (already used by bridge path).
- **D33c:** preserve all existing API signatures and add keyed variants; internally share one TypeKey path.
- **D33d:** continue omitting Context.filter event-scope filtering (D29): events are not filtered across isolates. Subscribers filter host events themselves, matching the dsh bridge v1 behavior (“receive pure emit only”).
- **D33e:** loader's name-to-factory registry is outside this design; dynamic plugin loading is a separate task, with D32's PluginFactory as its foundation.

### 2.6 Test plan (contracts + e2e, about nine targets)

1. Basic keyed registration/dispatch: same type, different names are isolated; matching static/dynamic names interoperate.
2. Register and emit a runtime name created with `format!`.
3. D31 regression: keyed emit preserves order for same name; different names do not enter the same chain.
4. `serial_keyed` short-circuit value / `waterfall_keyed` veto / `parallel_keyed` aggregation (reuse existing four-mode assertions).
5. once_keyed runs exactly once; prepend_keyed order.
6. Type mismatch (manually constructed key type differs from dispatch E) fails fast.
7. Listener is removed automatically when its owner fiber unloads (D28 keyed path).
8. Parity update: string-name cases for `ctx.on()`/`ctx.once()`/`ctx.waterfall()` in events.spec were previously “partial parity (JS carrier)”; after keyed support, the kernel can cover them all (`name → keyed_dynamic`).
9. Bridge e2e (`tcp_e2e` mode): host sends `evt/emit` → Rust `on_keyed::<HostEvent>` receives it; assert payload.

---

## 3. Milestones

| Phase | Scope | Acceptance |
|---|---|---|
| M1 | D32: plugin.rs + ctx.rs + fiber.rs + §1.7 contract tests | All green; clippy/fmt clean |
| M2 | D33: key.rs + bus.rs + event.rs + §2.6 tests (1–8) | Same; all existing 115 tests unchanged |
| M3 | Bridge: HostEvent + InboundHooks evt seam + rutis_dsh assembly + e2e | All tcp_e2e tests pass |

M1 and M2 can proceed in parallel (mostly different files; only M1 touches fiber.rs). Commit and rollback each phase independently.

## 4. Risks

- **Cascade from TypeKey losing Copy:** compiler exposes every use; no silent risk. Work is adding clone at each site, estimated ≤20.
- **D32 dual factory modes:** adds a branch for selecting instance in `load()`; reusing `fail_load` preserves atomicity.
- **Ownership cycle if bridge holds Ctx:** Bridge(Arc) → Ctx(Arc<CtxInner>) → fiber(Weak), so no strong cycle; verify against existing `Shared` during implementation.
- **Event storms:** under frequent host `evt/emit`, `emit_keyed` constructs one `Arc<str>` key per event (one allocation). Measured dsh event rates are low (session/agent scale), so no bottleneck; do not cache initially.

## 5. Explicitly Out of Scope

- `internal/update` waterfall and noSave (D32d).
- Config schema / schemastery equivalent (also D32e-related).
- Context.filter event filtering (D33d).
- Loader (plugin-name-to-factory registry and config-driven loading) (D33e); PluginFactory from D32 is a foundation, but do not build the registry in this batch.
- The entire `internal/*` protocol surface (retain the existing parity decision).
- Wildcard/glob event-name subscriptions (add only if the host has a real need; no evidence of demand today).
- **Cleanup of completed JoinHandles in `dispatch_tail`:** a pre-existing issue, not introduced by this design. Keys are bounded for static event types; after D33 dynamic names, boundedness depends on the host event-name set, still on the order of “number of event names.” Defer changes to D31 structure to a later simplification batch.

## 6. Suggested Implementation Order

M1 → M2 → M3. M1 goes first because it has the greatest independent value and does not touch the bus. M2 changes TypeKey across the repository; keeping it to one commit makes review and rollback easier.

## 7. Implementation Record (Completed 2026-09-21; §8 records review-fix rounds)

**All implemented; full workspace tests pass (29 new: config_update 16 + event_keys 11 + host_events 2)**, with no clippy warnings in new code (existing warnings unchanged).

- **M1 (D32):** `PluginFactory` (plugin.rs) + `Ctx::plugin_with/plugin_from` (ctx.rs, shares mount teardown with `plugin()`) + `FiberView::update/current_config` (fiber.rs). Internals: `FiberInner.factory/config` pair, erased by `ErasedFactory`; `load()` uses `current_plugin()` and factory mode rebuilds each generation. **One design gap found during implementation:** `resolve_deps` originally read injects from the plugin instance; factory mode has plugin=None, so gating was bypassed and it loaded directly. Changed to derive dependencies from current config in factory mode (clone config out of lock before calling user code to avoid re-entry deadlock). User callbacks in update dry-run are wrapped in catch_unwind. 16 tests in tests/config_update.rs (§1.7 items 1–10 plus review additions 11–16, second review added 17; see §8).
- **M2 (D33):** dual-variant `Qualifier` enum in key.rs (Static zero allocation / Dynamic `Arc<str>`; **manual Eq/Hash compare string contents**, because derived equality would compare variants—caught by a test); bus's three `TypeId` registration keys changed to `TypeKey`; complete keyed API family (`on/once/on_waterfall/emit/parallel/serial/waterfall` `_keyed` variants), sharing one internal TypeKey path. Compiler exposed all effects of TypeKey losing Copy; registry lookup/consumers_of/notify_key_changed were changed to borrow. 11 tests in tests/event_keys.rs, including parity additions that upgrade Cordis events.spec string-name cases from “partial” to fully coverable by the kernel.
- **M3:** new rutis-cordis `events.rs` (`HostEvent{name, payload, origin}` + `forward_host_events(ctx, observe)`; **forward before observe**, per review; malformed events log one truncated line and are dropped). rutis_dsh runner replaces `on_notify` (stderr summary becomes observer only). E2E in tests/host_events.rs (MemoryWire, two named subscriptions, origin forwarding, malformed events not entering bus, observer receives all, bus survives bridge disconnect; 2 tests).

**Deviations from design:**

- `current_config` returns `Option<Arc<C>>` (draft said `Option<Box<C>>`); Arc storage makes snapshots zero-copy, and update also stores Arc.
- `PluginFactory` adds a default `name()` method for the fiber display name, used when spawning factory-mode fibers; the draft omitted it, now declared here.

## 8. Review-Round Record (2026-09-21, Four Independent Parallel Reviewers)

Review target: PR #1 (`feat/config-hot-update-and-dynamic-events`). **Zero blockers.** Layering discipline, key-mechanism correctness, and numeric claims (README 136 at review time, 142 after fix / all green / clippy) were verified.

### Fixed (all)

1. **No panic boundary around build in `load()`** (concurrency review; must fix): build panic escaped the driver → fiber stuck Loading, join waited forever. Fixed by wrapping `build_erased` in `current_plugin()` with catch_unwind; panic calls `fail_load`, symmetric with validate. Added contract test 15 (build panic → Failed; retry via restart works and does not hang).
2. **Silent failure if injects drift with config** (concurrency review; must fix): registry registers only once at spawn. If `injects(&config)` drifts, notify/eviction silently fail and this contradicts re-derivation in resolve_deps each generation. Fixed by having update dry-run require declarations derived from new config to equal spawn snapshot (`FiberInner.spawn_injects`); otherwise reject with Validation. Documented contract and alternative in plugin.rs. Added test 14.
3. **Missing forwarding of three fields** (bridge review; must fix): pump discarded scopeId/sessionId/turnId from `Frame::Ntf { .. }` without recording a decision. Fixed with standalone `EventOrigin`, three-argument `NotifyHook(method, params, origin)`, carried by `HostEvent.origin`; e2e asserts forwarding.
4. **Observer panic swallowed event** (bridge review; must fix): observe awaited first, so panic silently lost forwarding of that Ntf. Fixed: **forward before observe** (`emit_keyed` only queues tail); malformed frames changed from silently dropped to one-line eprintln for observability.
5. **Flaky sleeps in tests** (event review; must fix): replaced all with Notify-signaled waits. Negative assertions now use deterministic paths (no-listener key makes `take_hooks` synchronously return empty without a task; dispose await returns only after registry removal). No sleeps remain in event_keys/host_events.
6. **No Loading-state update test** (consistency review; must fix): added test 11 (cooperatively cancel running apply and reload new config; also covers §1.5 “update × running apply”).
7. **No update × eviction test** (consistency review; must fix): added test 13 (provider replacement races consumer hot update, both converge, final reload reads new provider generation value).
8. **No panic boundary around `injects_erased`** (concurrency review; recommended, same class as 1): both resolve_deps and spawn calls now catch_unwind; panic means dependency not ready (sentinel `InjectsUnavailable` leaves fiber Pending; route error to ErrorSink; consistent with existing `check()` behavior). Added test 16.
9. **No bridge-disconnect test** (bridge review; recommended): host_events adds `event_bus_survives_bridge_drop` (ctx lives independently of bridge).
10. **Qualifier Debug printed variant although Eq uses contents** (event review; recommended): custom Debug prints string contents, matching Eq.
11. **No Unloading-state update test** (consistency review; recommended): added test 12 (queue behind slow cleanup, then re-provide dependency and load new config).

### Documentation fixes

- Rewrote mapping of §1.7 items #9/#10 to initial implementation (original #9 “build failure recovery” folded into #4 apply-failure path; original #10 parity coverage is handled by event_keys item 8).
- Completed dispatch signatures in §2.3; recorded three-field protocol decision in §2.4; added dispatch_tail legacy note in §5.
- Clarified parity test note: disposal assertion is covered by `keyed_listener_removed_with_owner_fiber` (coverage assembled across tests).

### Closed without action

- TOCTOU window for update × dispose: same shape as restart; mailbox + post_join recheck closes it, with no invalid state or permanent wait (explicit concurrency-review conclusion).
- Concurrent double-run window under pure-build contract: already specified by D32b; keep it.
- Completed JoinHandle cleanup in `dispatch_tail`: pre-existing; keys bounded; record in §5.

**Post-fix status:** config_update 17 + event_keys 11 (signal-based) + host_events 2; full workspace tests pass; no clippy warnings in new code (three existing warnings: one in services.rs and two in aimux-llm, unrelated).

## 9. Second Review Round (2026-09-22, Four Reviewers)

Review target: `976d2f1`. **Zero blockers.** Key claims from the fix round (§1.7 mappings 1:1, 268 green, README 142, zero sleeps, all 11 fixes implemented) were verified. Review also confirmed the EventOrigin forwarding chain (including outbound/handshake-frame origin semantics), forward-before-observe, Notify permit semantics, deterministic negative-assertion argument, signature adaptation, bridge ownership graph, and CI coverage.

### Fixed (new in round two)

1. **Blind spot in sentinel interaction (must fix, narrow):** interaction between first-round fixes #2/#8—the spawn-time injects-panic sentinel was added to both `spawn_injects` snapshot and `inject_index`, so dead Weak entries accumulated unboundedly (append-only), and a panicking factory could neither load nor update (drift check always rejected the sentinel baseline). Fixed: on spawn panic, baseline is empty and **no registration is made** (no leak); for an empty baseline, update **skips drift check** and **registers the newly derived set** (gives panicking factory a recovery path); `register_inject` deduplicates by pointer (no-op if recovery overlaps a spawn registration). Test 17: after spawn-time injects panic, update config recovers and loads (load count is 1–2 based on mailbox timing; assert terminal semantics instead).
2. **Route spawn injects panic to ErrorSink** (parallel review S1): prior `Err(_p)` silently discarded it, unlike resolve_deps and contrary to docs. Fixed: convert to `PluginFailed` and route through `ctx.error_sink()`; update comment.
3. **Malformed-event eprintln unbounded** (bridge review; must fix): huge params could flood stderr. Fixed: truncate at 200 characters; observer skips malformed frames to avoid duplicate logging (review 2 suggestion 4).
4. **Three doc inconsistencies** (consistency review; must fix): §7 M1 test count 10→16; stale §7 M3 description (parallel/silent drop → forward first/truncated log); §8 README reference 136→142. All fixed.
5. **Documentation drift** (blind-spot review; recommended): §2.3's “take_hooks validates type_id” changed to “type is encoded in key; public API cannot construct mismatched key” (actual defense); §2.4 notes that `on_event`/Bridge owning Ctx was only a draft (actual composition uses `forward_host_events`, bridge does not hold Ctx); HostEvent code includes `origin`; §5 dispatch_tail “bounded key” note now clarifies dynamic names; §1.7 item #8 includes rejection by static fiber; `events.rs` docs say “all notification frames (Ntf)” rather than “all frames”; rutis-cordis lib.rs crate docs add events module.

### Closed without action (round two)

- Race where provider notification arrives before `register_inject` during spawn: same as static mode and covered by existing pre-registration design; cannot occur in actual usage pattern.
- Fragility of `wait_until_state` for transients: these tests wait for controlled stable intermediate states (SlowFactory gate / drain_gate held); helper semantics are documented in test 17.
- Test 1's “LIFO assertion” actually asserts apply order (exactly-once cleanup is separately locked); plan item #1 wording is broad, but test stays.
- fmt residue (6 lines) in workspace `examples/tui.rs` included in this fix commit.

## 10. Third Review Round (2026-09-22, Three Reviewers: Empty-Baseline Specialist / Invariant Falsification with Probe / Final Read-Through)

Review target: `c7043e2`. Prior-round fixes were confirmed. This round focused on **new logic introduced by the second-round fixes and interactions among them**.

### Fixed (new in round three)

1. **Cannot distinguish two sources of an empty baseline** (empty-baseline specialist, blocker): `spawn_injects` used an empty Vec for both “panic; baseline unknown” and “explicitly declares no dependencies.” In the latter case, drifting to dependencies during update was silently allowed, breaking fail-fast. Fixed: `spawn_injects` is `Option<Vec<TypeKey>>`—`None` means unknown due to panic (skip check); `Some(empty or non-empty)` means explicit declaration (strict check). Test 18 rejects drift from a genuinely empty declaration.
2. **Baseline escape chain** (falsification, must fix; reproduced by probe): after recovering from a None baseline, failure to write back the new baseline let **every later** update escape validation (probe: recovered with [A,B], then drifted to [B] and was still allowed; stale entries accumulated in inject_index). Fixed: after successful update, write back `Some(derived)` as the new baseline. Test 17 extended to reject drift after recovery.
3. **Concurrent dispose window after dry-run** (falsification, must fix; reproduced in 6 of 10 probes): dry-run is synchronous and unlocked, so concurrent dispose could finish entirely during it. update then returned Ok, stored config in a dead fiber, and registered dead Weak refs. Fixed: after successful dry-run and before storing config, **recheck terminal state** (`terminal_task`/alive); if disposal won, return InactiveEffect and do not store or register.
4. **`post_join` self-completion overwrote real error** (final read-through, must fix; pre-existing but exposed by update): second alive check used None fallback, replacing a real terminal error when the driver exited in Failed state with fake success. Fixed: self-completion carries `tr.error`. Note: post_join was existing code; heavy update use exposed the defect.
5. **Duplicate malformed-event logging remained** (empty-baseline specialist, must fix): observer used `is_some()`, forwarder used `as_str`, so frames like `event: null/123` (field present but not string) were still logged twice. Fixed: observer uses same `and_then(as_str)` check.
6. **Sentinelize the `config=None` dead branch** (final read-through, recommended): in factory mode, resolve_deps previously returned an empty Vec if config was None (a future None path would load as if there were no dependencies). Now returns a sentinel key and remains Pending. Malformed eprintln adds `origin={origin:?}` (falsification recommendation).

### Closed without action (round three; recorded as follow-up)

- Dead Weak values in `inject_index` are never removed: unbounded growth with dynamic keys. Consider periodic prune later (append-only Weak count; addresses are not reused, so ABA dedup is safe—confirmed by falsification).
- Extract dry-run logic into a helper / consolidate sentinel behavior into `resolve_injects_safe` (final review readability suggestion): defer to simplification batch.
- Convert test magic numbers to constants and remove “review:” process prefixes from comments (style): defer.
- Invariants confirmed by falsification: ghost consumers from dead Weak filtered by upgrade; EventOrigin does not cross-contaminate (separate Arc); emit re-entry/dispose/registration crossover follows tail-snapshot semantics; test 17 assertions stable on multi_thread (all apply versions use v2).

**Post-fix status:** config_update 18 + event_keys 11 + host_events 2 = all 270 workspace tests pass; three pre-existing clippy warnings unchanged.

## 11. D32f Revision: Revoke `injects(config)` Derivation; Make Declarations Static (2026-09-22)

**Trigger:** review of the three rounds of fixes (§8–§10). Six core fixes—sentinel key, baseline snapshot, drift check, empty-baseline ambiguity, escape-chain prevention, and duplicate-registration suppression—were all defensive complexity around one design choice: deriving dependency declarations from `injects(&config)`.

**Source comparison:** in upstream Cordis and the fork, `inject` is a `Dict` field fixed at fiber construction (`fiber.ts:125`; never reassigned in the file; dependency gating uses `Object.keys(this.inject)`, `fiber.ts:166/388`). The fork's `update(config)` (`fiber.ts:478`) changes only `fiber.config` and never touches inject declarations. Lazy config resolution (`internal/config` waterfall) delays only **when config values are resolved**. Cordis's runtime way to change dependency set is `registry.inject(deps, cb)`, which registers a **new fiber** (split plugins), not modify the existing declaration. No use case in this repository or Cordis ecosystem changes dependencies based on config—this was an **invented capability**.

**Revision:**

- Change `PluginFactory::injects` to `fn injects(&self) -> &[TypeKey]`, exactly symmetric with `Plugin::injects`; register at spawn once and keep it immutable for life. Standard way to choose dependencies by config: split into multiple plugins and let config choose which one to mount (matches Cordis loader ecosystem).
- **Delete the mechanism family** (about 150 lines): `FiberInner.spawn_injects` baseline snapshot; update drift checks/baseline writeback/registration repair; pointer deduplication in `register_inject`; `InjectsUnavailable` sentinel; three injects panic boundaries in resolve_deps/spawn/update; config=None branch in resolve_deps.
- **Keep independent fixes:** three-step dry-run (§1.3), second terminal check (race from §10 item 3), post_join terminal error preservation (§10 item 4), and the full EventOrigin/observe/truncate behavior.
- **Test impact:** remove tests 14/16/17/18 (they lock behavior for bug classes in the deleted mechanism; deleting the mechanism eliminates those classes, which is better than guarding against them); config_update goes from 18 to 14 tests.
- `update()` shrinks to about 50 lines: terminal check → factory match → dry-run → second terminal check → store config → restart.

**Lesson for future batches:** fixes that introduce semantic machinery (sentinel/baseline/recovery) can be as complex as the original design but may skip design review. Every third-layer issue in these reviews (ambiguity/escape/race) came from this. **When the semantic complexity of a fix reaches design-level complexity, write and review its semantics before implementation.**

Keep the §8–§10 fix records as history; this section supersedes items revoked by D32f (§8 items 2/8, §9 items 1/2, §10 items 1/2).

## 12. Cordis Parity-Deviation Audit (2026-09-22, Full Review After D32f)

Three independent auditors compared rutis, upstream Cordis, and the dsh fork mechanism by mechanism. **Only two undeclared, accidental deviations** were found; all others are covered by declared decisions (D1–D33 and 38 omitted capabilities) or language differences.

### Fixed (audit item 2)

**Dependency removal behavior for Failed fibers:** Cordis Failed state is sticky. On dependency removal, `_setEpoch(INACTIVE)` returns early because epoch is already INACTIVE, leaving state Failed and its error visible (`fiber.ts:611-639`); dependency restoration changes epoch and triggers reload. Previously rutis treated Failed as loaded; removal called `unload(Pending)` and hid the error in settle channel. Fixed: in `refresh_deps`, the missing-dependency branch unloads Active/Loading to Pending but leaves Failed unchanged. The restore/load branch remains unchanged—Failed retries normally, matching Cordis reload semantics. Test 19: removal preserves Failed (settle keeps reporting error); restoration retries and, if it fails again, remains Failed.

### Explicitly accepted (not fixed; documented in comments)

- **Audit item 1, concurrency across effect cleanup:** Cordis runs top-level effects concurrently with `Promise.all` (`fiber.ts:676`) and LIFO within one effect; rutis serializes across effects in strict LIFO. This makes completion order deterministic and error aggregation predictable, a directional strengthening. Added comparison to drain_effects comment.
- **Audit item 7, update validation timing in non-ACTIVE state:** Cordis delays `_resolveConfig` until activation (`fiber.ts:739`); rutis always dry-runs (D32b) to expose errors earlier. Added comparison to update comment.

### Language differences (reasonable; no fix)

Serial short-circuit values (typed Option replaces JS truthiness); `check()` predicate has no `this` binding (closure equivalent); `injects` is a method instead of a constructor-fixed field (static contract; D32f now aligns semantics); dependency notifications do not pre-filter by isolate (each fiber filters during `scope_for` resolution, semantically equivalent).

**Conclusion:** after three-way comparison, all differences between rutis and Cordis are explicit—either in decision/omission lists or in this section/source comments.
