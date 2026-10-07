# Multilingual Plugins: One Runtime Plugin per Language (Design)

Status: design, not implemented. Date: 2026-10-03. M1 implementation design: [Multilingual Plugins M1](design-multilang-m1-2026-10-04.en.md).
Based on the [multilingual decision record](decision-multilang-2026-10-03.en.md) (#107; this document revises several conclusions as noted in §10), [Cordis Runtime as a Plugin](design-cordis-runtime-plugin-2026-10-03.en.md) (#109), [rutis-loader design](design-rutis-loader-2026-10-02.en.md), [compatibility layer design](design-protocol-plugin-mount.en.md), and [research reports](plan/analysis/multilang/README.en.md).
Baseline: `main` `a078630`.

## 1. Problem to solve

rutis currently has two dynamic plugin-loading models: dylib (Rust plugins in-process) and Cordis (JS plugins, running in one Node process through `CordisRuntimePlugin`). Both are loader rows and can appear in the same configuration.

Other languages have useful ecosystems:

- **Python:** AI/ML, data processing, and many SDKs; most agent tools are available in Python first.
- **Swift / Objective-C:** macOS system capabilities such as Calendar, Contacts, Accessibility, screen recording, and Core ML.
- **Go:** infrastructure libraries, cloud-provider SDKs, and network tools.

This design describes how to integrate them while avoiding the two concerns in #107: rebuilding a plugin framework for every language and letting process count grow with plugin count.

Out of scope:

- **Shell / PowerShell:** they pass only data and do not use services, dependency gating, or cleanup in this model. Their value is as agent tools; design a separate “command-tool runtime” with runtime discovery, JSON Schema arguments, and cancellation. Keep them out of the plugin protocol.
- **Remote execution:** running plugins on another machine is separate work and not tied to multilingual support (remote design #110 remains a draft).
- Implementation details for each language runtime. This document sets the model, contract, and phases only.

## 2. Conclusions

1. **One runtime plugin and one process per language runtime.** As with #109 `CordisRuntimePlugin`, the runtime plugin starts the language process and provides a runtime service. Every plugin in that language is a loader row dependent on it and loads into the same process. Ten Python plugins still use one Python process.
2. **Implement the paradigm once, in rutis.** rutis and loader own dependency gating, startup/shutdown, restart, configuration layers, and hot update. Each plugin in another language is one row and one fiber in rutis.
3. **Plugins in other languages are leaves.** Their language side needs only a small SDK: a plugin has `apply(ctx, config)`, uses/provides services there, and returns cleanup. No child plugins, event system, or local dependency graph, so do not reproduce Cordis or rutis in Python/Swift.
4. **Node remains full Cordis.** Reusing existing npm Cordis plugins requires their native interdependencies. This is the only side that needs a complete framework.
5. **Same-process calls stay local.** Plugins in one runtime call each other's services directly without IPC. rutis manages who depends on whom and who stops first, not the call itself. Only cross-language calls use IPC (about 30 µs for sync calls, measured on Node).
6. **Add isolation instances only when needed.** An unstable plugin can go into another runtime instance. This is the same rule as “put plugins needing isolation in different mounts”; do not do it by default.
7. **Two runtime classes:**
   - **Load code by name** (Node, Python, Swift): one process loads multiple plugins by name; Swift uses `dlopen` for plugin bundles.
   - **Plugins are compiled programs** (Go): Go cannot reliably load code at runtime, so compile a group of plugins into one executable and run it as one runtime process. Updating code replaces the binary and restarts the group.
8. **Reuse the current protocol**; no protocol version bump for this work. Keep frames, references, cancellation, sync call chains, and `rows.*` controls (§5). Add only services projected into rutis by name and method shape (sync vs async).
9. **A runtime must not depend on services used by its rows.** The runtime plugin depends only on what is needed to start its process. Gating for services used by plugins (host-provided or from another language) belongs to each consumer row; call routing binds/unbinds dynamically after runtime startup. This prevents runtimes waiting on one another when languages share services (§4).
10. **Release rows in two stages.** After a runtime process starts, provide `<Language>Runtime`; use it to refresh resolution for rows bound to that runtime and obtain plugin declarations; only then provide `<Language>RuntimeRows`, which rows depend on. Plugin dependencies are unavailable until the process starts, so this ensures declarations are complete before rows run (§6).
11. **Python first, then Swift; do Go when there is concrete demand.**

## 3. Existing pieces to reuse

| Existing component | Role in this design |
| --- | --- |
| `CordisRuntimePlugin` (#109): starts Node, injects host services, provides `CordisRuntime`, revokes on crash, no automatic restart | Template for one runtime plugin per language |
| `InteropResolver` + `JsRow`: resolves by name, row depends on runtime service, controls `rows.load / update / unload / schema` | Language-independent controls between rows and runtime |
| Wire protocol: frames, function/async-result references, ref counts, `cancel`, `path` call chain, `SyncWaitCycle` | Language-independent session between runtime process and rutis |
| `host_key` host services | How plugins in other languages use rutis services |
| Python runner research (`python.md`): prototype passed nested sync callbacks, `SyncWaitCycle`, cancellation, and deferral of unrelated calls; sync round-trip about 25–60 µs | Session layer of Python runtime can follow this prototype |

One protocol detail is still hard-coded to Node: Rust accepts only call IDs beginning with `node:`. For the first phase, treat `node:` as the “runtime side” prefix and reuse it for other language runtimes without a protocol version change. Rename it with a future protocol upgrade if needed.

## 4. Model

```text
rutis host
 ├─ CordisRuntimePlugin ── Node process (full Cordis) ── JS rows × N
 ├─ PythonRuntimePlugin ── Python process (leaf SDK) ── Python rows × N
 ├─ SwiftRuntimePlugin  ── Swift helper process (leaf SDK) ── Swift rows × N (bundles)
 ├─ GoRuntimePlugin     ── Go executable (group of plugins compiled together)
 ├─ dylib rows (in-process)
 └─ LoaderPlugin: desired state for all rows
```

**Runtime plugin** (one type per language; can have multiple instances):

- Configuration: launch command and project location (venv, bundle directory, executable).
- Dependencies: only what the process itself needs to start, usually nothing. **Do not put services needed by plugins here.** #109 currently puts host services in runtime `injects`, which works with one runtime. With multiple runtimes using each other's row services, it creates a wait cycle (Python runtime waits for JS row, JS row for Cordis runtime, Cordis runtime for Python row). M1 moves host-service gating out of runtime dependencies and onto consumer rows (§5).
- Provides services in two stages (keys include instance name, e.g. `TypeKey::keyed_dynamic::<PythonRuntime>("py")`, allowing multiple instances per language):
  1. Provide `<Language>Runtime` after process startup and handshake.
  2. Refresh resolution of rows attached to this runtime (§6), then provide `<Language>RuntimeRows`.
- On process crash, revoke both services and do not automatically restart; the application decides, consistent with #109 and #71.

**Plugin rows:**

- Dependencies: the runtime's `<Language>RuntimeRows` plus plugin-declared dependencies (§6). Each row waits for its own services; the runtime does not.
- In `apply`, ask the runtime to load the plugin; on cleanup, unload it. Updates that change only volatile fields are applied in place, following #101.
- Project plugin-provided services into rutis so Rust and other-language plugins can use them by name (§5).

**Placement:** Prefix row names with the runtime, for example `py:weather.plugin`, `swift:Calendar.bundle`, `go:tools/ping`. Unprefixed npm package names continue to use Cordis; `dylib:` is unchanged. The runtime interprets the suffix: Python module name, Swift bundle, or Go plugin name in the executable.

## 5. Runtime contract

The runtime-process/rutis contract is the current Node runtime implementation with three additions:

| Item | Current | Addition |
| --- | --- | --- |
| Startup | Fixed `node --import tsx runner.mjs` | Runtime plugin config supplies launch command; socket path and project location remain arguments |
| Load | `rows.load(key, entry, config, isolate, inject)` | Runtime interprets `entry` (module, bundle, plugin name). `isolate` applies only to Cordis; leaf SDKs may ignore it. |
| Describe | `rows.schema(entry)` returns JSON Schema | Also return plugin-declared dependencies (§6), plus provided service names and method shapes. |
| Services | In row mode, row-provided services are not projected into Rust (`InteropResolver` today) | Runtime reports row-provided services through existing service-slot notification. rutis registers by name under `TypeKey::keyed_dynamic::<RemoteService>(name)` and records them in the loader's service-name catalog. |
| Method shape | Only build-generated mounts know sync vs async | Report sync/async method shape with each service at runtime: Python uses `inspect`; Swift declares it in SDK registration. Node approach in §9. |

A service slot may be withdrawn, reloaded, or replaced by a configuration update, but its handle generation increases per service name throughout the session and is not reset when the slot is deleted. Each handle retains its original object and method declarations until released; releasing an old handle does not affect the newer object of the same service.

Call directions:

- **A plugin in another language calls a rutis or other-language service:** use existing `host:<name>` calls. At call time, rutis looks up the current provider by name and forwards if it belongs to another runtime. Routing is dynamic; the runtime need not have those services at startup:
  - Leaf SDK `ctx.use(name)` returns a name-based proxy. The consuming row lists `name` as a dependency, so rutis starts it only when the service exists and stops it on revocation.
  - Cordis plugins use native `inject` for services. When a service appears/revokes, rutis dynamically registers/removes the corresponding proxy in Node Context using `hosts.provide / hosts.withdraw`, replacing #109's one-time startup registration. Forwarding requires reference transfer across sessions (currently explicitly unsupported in `rpc.rs`); this is the only major session-layer change in M1.
- **Rewrite call chains while forwarding:** synchronous calls carry `path`, which the peer compares with its own pending call IDs to decide whether a reverse call belongs to its call chain. All runtimes use the `node:` prefix, and IDs are unique only within each session. When rutis forwards between sessions, prefix call IDs in `path` from other sessions (e.g. `py/node:3`); otherwise the peer may mistake it for its own `node:3`, misroute a call, or deadlock. Rewritten entries can never match a peer's IDs; entries within the same session remain unchanged. Node and Rust treat `path` entries as strings, so no protocol change is needed.
- **Plugins in one runtime call each other:** runtime passes the object locally to the consumer; do not route through rutis.

## 6. Leaf SDK

Python shape (illustrative):

```python
# weather/plugin.py
inject = ["llm"]                 # Declare dependency: rutis waits for it before start and stops on revocation

def apply(ctx, config):
    llm = ctx.use("llm")         # Use a host, plugin, or other-language service
    ctx.provide("weather", Weather(llm, config["city"]))
    return lambda: ...           # Cleanup
```

Swift shape (illustrative):

```swift
final class CalendarPlugin: RutisPlugin {
    static let inject = ["llm"]
    func apply(_ ctx: Context, config: Config) async throws -> Cleanup {
        ctx.provide("calendar", Calendar(store: EKEventStore()))
        return {}
    }
}
```

The SDK does only:

- Plugin entry (`apply`, `inject`, config type; config type may export JSON Schema).
- `ctx.use` / `ctx.provide`: pass actual objects within the process, proxies across processes.
- Runtime process: implement §5 contract and reentrancy during synchronous waits (Python approach in research §3: I/O thread plus main thread execute reverse calls by call chain).

Dependencies are declared in the plugin (`inject`) and reported to rutis through `rows.schema`. rutis adds them to the row, enabling native gating. rutis alone decides dependency, startup, and stop order; the SDK has no dependency graph.

Dependencies are visible only after runtime startup. If rows resolve before the runtime starts (as with #109 `InteropResolver`, which returns a result without schema and depends only on the runtime), they may start immediately when runtime appears, before `inject` is known and even while `llm` is unavailable. Therefore the runtime does this between its two provides:

- After providing `<Language>Runtime` and before `<Language>RuntimeRows`, use the loader public API (`entries()` + `reload(id)`) to re-resolve rows attached to this runtime that were resolved while offline or have a changed version, obtaining complete dependency declarations.
- Those rows are waiting on `<Language>RuntimeRows`, so `reload` does not start them. A changed dependency declaration may rebuild the fiber, but does not start it.
- After refresh completes, provide `<Language>RuntimeRows`; rows then wait/start using complete declarations.

This is the same two-stage release (`Peer` / `PeerRows`) used by the remote design to break startup cycles.

## 7. Language-specific points

| Language | Runtime process | Notes |
| --- | --- | --- |
| Python | One Python process, load by module name | Implement session layer from research prototype; one locked environment (uv project) per runtime; create another instance for dependency conflicts; Python 3.12+ |
| Swift / ObjC | One signed helper app using `dlopen` for plugin bundles | TCC permissions belong to helper, so it needs its own signature and usage description; Swift can call ObjC directly; Apple platforms only |
| Go | Compile a group of plugins into one executable | Do not use Go `plugin` package; code change rebuilds and restarts group; deployment owns builds, host does not compile at runtime |

## 8. Processes and efficiency

- Process count equals runtime instances: normally one per language; one per Go group; add instances only for isolation.
- Calls inside one runtime do not use IPC.
- Cross-language sync calls cost about 30 µs (Node measurement); Python research reports 25–60 µs. For high-frequency hot paths, put both sides in the same language or use async methods.

## 9. Open questions

- **Method shape on Node:** row mode has no build-generated information. Options: generate a shape manifest with the npm project using existing generator and `.d.ts`, or expose Cordis row services only to same-process JS plugins and not project them into rutis yet. Prefer the manifest.
- **Cross-session reference transfer:** required by §5; design and test separately.
- **SDK location:** separate packages in this repository (`runtimes/python`, `runtimes/swift`) or a separate repository. Prefer this repository, maintained with compatibility tests.
- **Call ID prefix:** keep `node:` initially (§3); decide when to rename later. It does not conflict with Cordis or Node's `node:` module protocol because it appears only in session frames, invisible to plugins. Rewrite `path` on cross-session forwarding to prevent collisions (§5).
- **Call-chain rewrite test:** M1 must test two runtimes synchronously waiting with the same local call ID and prove reverse calls reach the correct side.
- **Avoiding deadlock in cross-runtime sync calls** (discovered during M2; see §2 of [M2 implementation notes](design-multilang-m2-2026-10-04.en.md)): if two runtimes synchronously call each other's services and both process only calls on their own chain while deferring others, both wait forever. This is a protocol limitation, not language-specific. Current state:
  - Node is not reentrant (preserve Cordis/JS semantics).
  - Python is reentrant: it executes all incoming calls while synchronously waiting. Node↔Python therefore does not deadlock, but a Python service may be called during its own sync call (“do not hold locks while calling rutis services”; this cannot be enforced).
  - Crossed sync calls between Node↔Node (two Cordis runtimes) can still deadlock; use async methods for now.

Before integrating each new language (Swift, Go), decide if its runtime is reentrant and what happens to sync calls between non-reentrant runtimes. Options include making every leaf runtime reentrant while Cordis alone is not, forbidding/detecting sync calls between non-reentrant runtimes, or detecting cross-session wait cycles in Rust and returning `SyncWaitCycle`. Decide before adding a third language.

## 10. Relationship to decision #107

| #107 conclusion | This design |
| --- | --- |
| Keep PowerShell, Bash, AppleScript/JXA out of rutis | Retained. Build a separate command-tool runtime for Shell-style tools; keep out of plugin protocol. |
| Python (min_cordis) out of tree, revisit later | Revised: support Python, but with leaf SDK + runtime plugin, not a full min_cordis framework. |
| No neutral interface description | Partially revised: describe only method shape (sync vs async) for cross-language calls; no type IR or code generation. |
| No launch abstraction | Partially revised: runtime plugin can configure launch command, nothing more; no generic launcher layer. |
| Do not relax `node:` prefix | Retained: reuse `node:` as the runtime-side prefix. |
| Keep reverse direction frozen | Retained. |

#107's concerns—“each language will stay at demo quality” and “one maintainer cannot maintain five runtimes”—are addressed with a small leaf SDK per language, one implementation of the paradigm in rutis, and adding languages one at a time when there is real demand.

## 11. Phases

| Phase | Work | Acceptance |
| --- | --- | --- |
| M1 | Extend contract: configurable runtime launch command; `rows.schema` returns dependencies and service shapes; project row services into rutis by name and register in service-name catalog; cross-session reference transfer and call-chain rewrite; move host-service dependencies off runtime and gate by row, dynamically register via `hosts.provide / hosts.withdraw`; two-stage runtime release. First implement on Node runtime. | JS row service can be injected by name into Rust row; JS plugin dependencies participate in rutis gating; resolve a row injecting `llm` before runtime startup and prove it waits until `llm` appears, then stops on revocation; all existing tests pass. |
| M1/M2 boundary | Cold-start two runtimes together: Python provider P and JS provider J each provide a service; JS consumer uses P, Python consumer uses J. | All four rows start without runtimes waiting on each other; revoking either provider stops only its corresponding consumer. |
| M2 | Python runtime plugin + leaf SDK + contract compatibility tests | Same compatibility suite passes on Node and Python; Python and JS plugins use each other's services; same-process calls bypass IPC. |
| M3 | Swift runtime plugin (macOS) + leaf SDK | EventKit plugin runs in signed helper app; system permission prompt identifies the helper. |
| M4 | Go, when there is concrete demand | Compile a group of Go plugins into one executable launched by runtime plugin; replacing binary restarts that group. |
