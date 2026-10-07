# Network Stack: Decoupling Protocol and Channel (Design)

> Package structure changed on 2026-10-06: `rutis-channel`, `rutis-interop`, `rutis-transport-*`, and `rutis-runtime-local` were merged as modules (`channel`, `session`, `runtime`, `transport::*`, `cordis`) in `rutis-bridge`; see [Packages, Tools, and Workflows for Developers](design-developer-packages-2026-10-06.md). The crate names below are retained as they were at design time.

Status: design, not implemented. Revised: 2026-10-05.
Based on: [Compatibility Layer Design](design-protocol-plugin-mount.md), [Requirements: Mounting Cordis Plugins](requirements-protocol-plugins.md).
Consumer: [Remote Plugin Design](design-remote-plugins-2026-10-03.md) (called “remote design” below).

## Scope and Boundaries

This document specifies cross-language channel contracts, connectors, framing and encoding boundaries, WebSocket binding, and implementation acceptance. Rust, JS, and other implementations follow the same contract.

- The protocol depends only on a bidirectional channel that is ordered, reliable, and preserves message boundaries; the channel transports opaque bytes only.
- One Session uses one logical Channel. The Transport Adapter decides how logical channels map to physical connections; they need not be one-to-one. Connection pools, reuse, and multiplexing do not belong in the session protocol; framework nodes are composed at the framework layer.
- Two remote endpoint types are supported: A is a full framework node, with each side managing its own plugins and configuring `export`, `import`, `host`, and `events` as needed; B is a leaf language runtime managed by local rutis and loader, with no rutis running remotely.
- Both endpoint types share transport, identity, link, and Session. Runtime attachment is independent of node-bridge features; a leaf runtime need not implement full framework-node capabilities.
- The remote design specifies session handshake/capability format, endpoint-operation contracts, permissions, and framework-node composition. The rutis kernel and Cordis remain unchanged.
- The rutis-dev development protocol and Windows named-pipe implementation are out of scope.

## Layers and Plugin Structure

| Layer | Responsibility | Implementation and constraints |
|---|---|---|
| Connector | One dial attempt, accept connections, launch subprocess endpoints, perform authentication appropriate to transport | Rust contract in `rutis-channel`, implementations in concrete `rutis-transport-*` crates; JS in I/O worker; no reconnects |
| Channel | Ordered reliable message stream, framing, backpressure, liveness detection, close | Rust contract in `rutis-channel`, implementation in transport crate; JS in bridge package `src/channel/*.mjs`; knows nothing about protocol frames or encoding |
| codec | Convert protocol frames to/from bytes | Rust internal to `rutis-interop`; JS `src/codec.mjs`; independent of channel type |
| Session | Protocol handshake, calls, references, cancellation, call chains, errors | Rust `rpc.rs`; JS `session.mjs`; independent of channel type and authentication mechanism; endpoint contracts wrap operations |
| Endpoint operations | Service announcements, proxy-loaded plugins, event forwarding for full framework nodes; runtime operations for leaf runtimes | Node operations in `rutis-bridge`, runtime contract in `rutis-interop`; adapters per language; independent of channel type; do not force `rows.*` / `hosts.*` and `plugins.*` / `services.*` into one surface |
| Attachment and feature plugins | Share transport, identity, link; configure node-bridge features and runtime attachment separately | Responsibilities below |

`rutis-interop` uses channels only through `Channel`; `rutis-channel` has no dependency on protocol crates. `Channel`, `Session`, and codec are mechanism libraries and need not each be pluginized.

| Plugin | Contract |
|---|---|
| Transport | Provides `Transport#<kind>` service; wraps connector, owns listeners, performs transport-specific authentication and heartbeat, and delivers channels to link |
| Identity | Provides credentials, peer-identity mapping, and validation rules; identity revocation triggers cleanup of dependent links |
| link | Depends on transport; network links depend on Identity, while local subprocess endpoint identity is assigned by launcher; owns listener registrations; handles only connection, session identity check/protocol handshake, retry backoff, session replacement/readiness, and cleanup; knows nothing about loader/schema/rows and does not manage `PeerRows` / `RuntimeRows` |
| Node bridge features | A full framework node configures `export`, `import`, `host`, and `events` independently as needed; permissions depend on feature plugins each side installs for the other, not on dial direction |
| Runtime attachment | interop RuntimePlugin and bridge attachment Adapter connect sessions; loader-side companion plugin manages `PeerRows` / `RuntimeRows` and second-phase row readiness per endpoint contract, and handles session replacement/revocation; node-bridge features are not required |
| `peer` composition | Composes existing plugins only; does not reimplement connection, authentication, heartbeat, or link lifecycle |

Transports ship as native rutis plugins in separate crates: `rutis-transport-local` exports LocalPlugin, `rutis-transport-websocket` exports WebSocketPlugin, and `rutis-transport-memory` exports MemoryPlugin. Keep plugin config names `rutis-bridge/local`, `rutis-bridge/websocket`, and `rutis-bridge/memory`; crate names are not plugin names. Each crate owns both its channel implementation and plugin lifecycle wrapper; do not add a separate `plugin` feature.

Plugins validate config, provide Transport services, manage listeners/connections/subprocesses, revoke services and clean resources on unload, and let dependent links stop through native gating. `local` includes Unix, fd, and spawn; memory can also connect endpoints in-process.

`rutis-bridge` defines the public Transport service interface and owns identity, link, and node-feature plugins. Concrete transports depend on bridge and channel; bridge does not depend back on a concrete transport. The application assembly layer chooses transports and composes any custom transport it needs. The generic runtime session entry point belongs to interop; the Peer-to-entry-point Adapter belongs to bridge. Loader handles row integration only; bridge/interop do not depend on loader. Keep all crates in the same repository; do not change core `rutis`; users who do not need networking incur no WebSocket dependency.

WebSocket is only one transport. Adding a transport must not change link or protocol layers, force other transports to use WebSocket authentication, or weaken their security rules.

## Channel Contract

### Messages and Execution

| Property | Requirement |
|---|---|
| Ordering and delivery | Deliver in send order, without loss or duplication; media that do not guarantee this must add sequence numbers, acknowledgements, and retransmission inside the channel |
| Message boundaries | One frame equals one message; do not split, merge, parse, modify, or inject messages |
| Bidirectional communication | Full duplex; reverse calls can still arrive while a synchronous call waits for its reply |
| Backpressure | Bounded buffering; sender waits under backpressure instead of accumulating without limit |
| Independent progress | Does not depend on caller executor. Rust API is blocking; async transports own a thread/runtime; Node progresses in I/O worker; other languages use an equivalent dedicated thread or mechanism |
| Liveness | A silently failed network medium must detect loss within finite time; local socket uses EOF |
| Identity | Delivered only through `ChannelInfo`, never injected into message stream; endpoint ID in session handshake must match peer identity confirmed by connector |
| Termination | Observable with diagnostic reason; in-flight calls fail as `Transport`; close reason is diagnostic only and must not control program branches |

### Rust Interface

```rust
// rutis-channel, proposed interface
pub struct Channel {
    pub sender: Box<dyn Sender>,
    pub receiver: Box<dyn Receiver>,
    pub closer: Arc<dyn Closer>,
    pub info: ChannelInfo,
}

pub trait Sender: Send {
    // Send one message; blocks under backpressure.
    fn send(&mut self, message: &[u8]) -> Result<(), ChannelError>;
}

pub trait Receiver: Send {
    // Block until the next message; Ok(None) means the peer ended normally.
    fn recv(&mut self) -> Result<Option<Vec<u8>>, ChannelError>;
}

pub trait Closer: Send + Sync {
    // Idempotent; wakes threads blocked in send and recv.
    fn close(&self, reason: &str);
}

pub struct ChannelInfo {
    pub transport: &'static str, // "unix" | "fd" | "memory" | "websocket" …
    pub peer: Option<PeerId>,    // Confirmed by connector; launcher assigns local subprocess identity
    pub label: String,          // Diagnostic label, e.g. "peer mac"
}

pub enum ChannelError {
    // Channel ended; transport determines why (peer disconnected, message too large, heartbeat lost).
    Closed { reason: String },
}
```

- The contract specifies ordering, reliability, boundary preservation, backpressure, and close only. Framing, liveness detection, and maximum message size belong to each transport protocol and its Adapter configuration; they do not enter the `rutis-channel` contract, and Session assigns them no special semantics.
- Session encodes and sends while holding its send lock, keeping reference-table changes consistent with send order.
- `close` first interrupts blocked sends outside the send lock, then takes the lock to clear the table.
- Reader thread loops on `recv()`, decodes, and passes data to Session. Map `Closed { reason }` to `Error::Transport("<label>: <reason>")`. Remove the `disconnected` closure from the Session entry point.
- Session entry point is `Connection::open(Channel, dispatch)`; existing `connect` may remain as a compatibility shorthand for Unix channels.
- `PeerId` is a session endpoint ID and does not require the peer to be a full framework node. Optional `ChannelInfo.peer` supports channels without endpoint identity; link must not mark a channel session-ready unless peer identity is confirmed.

### Node and Other Languages

Node channel exposes an equivalent API inside its I/O worker:

```js
// open(spec, { message(bytes), closed(reason) }) → { send(bytes), close(reason) }
// Establishment failures are returned separately as structured ConnectError;
// do not infer them from closed(reason).
```

- Channel specifications: `unix:<path>`, `fd:<n>`, `ws://…` (loopback only) / `wss://…`, `listen:ws://…` / `listen:wss://…`; a bare path means `unix:`. `listen:` lets a runtime wait for the controller to dial in and accepts the first authenticated connection. Pass credentials, CA, certificate, and private key through `RUTIS_INTEROP_TOKEN`, `RUTIS_INTEROP_CA`, `RUTIS_INTEROP_CERT`, and `RUTIS_INTEROP_KEY`; never put them in channel spec, URL, or command line. Python WebSocket runtime is optional dependency `rutis-runtime[network]`.
- Main thread and worker continue using MessagePort + `Atomics` for handoff; while the main thread blocks synchronously, worker continues sending/receiving and handling heartbeat.
- Session uses newline-free `codec.encode`; worker may call `codec.decode`; channel module does not choose encoding.
- JS WebSocket accept requires a dependency with server support (e.g. `ws`); dialing alone does not require server capability.
- Other languages follow the same contract. Python local subprocess endpoint can open inherited fd with `socket.socket(fileno=3)`.

## Framing and Encoding

| Property | Requirement |
|---|---|
| Encoding | Compact JSON at protocol layer; escape newlines inside strings; codec is internal and not currently replaceable |
| Byte stream | Unix sockets and inherited fds use newline-delimited framing; channel adds newline on send and removes separator on receive |
| WebSocket | One text message per UTF-8 JSON frame, without newline |
| Future binary encoding | Length prefix on byte streams, binary message on WebSocket; connector determines both sides' encoding before session establishment; do not negotiate inside protocol frame |

## Logical Channels and Physical Connections

Multiple upper-layer plugins share a Session: plugins in one Runtime instance share its active control Session; bridge features and proxy-loaded plugins on a node link share that link's active Session. Plugins are distinguished by instance key and resource ID; loading/unloading one plugin does not create/close a Session. This sharing model is independent of Adapter physical-connection reuse. See “Session Sharing and Resource Ownership” in remote design for cleanup scope.

- Adapter chooses whether one logical Channel owns one physical connection or multiple Channels share a physical connection; Session and link never access physical connection handles.
- send/recv, ordering, backpressure, and close contracts apply per logical Channel. Reuse implementation isolates message boundaries and flow control; one blocked channel must not cause unbounded accumulation in another.
- Closing or replacing a session releases only its logical Channel; shared physical connection and other Channels remain valid. Adapter decides when to retain/reclaim idle connections.
- Adapter notifies all affected Channels of physical connection failure. If ordered/reliable guarantees can no longer be met, terminate the Channel; a terminated Channel cannot be revived by switching physical connections and Session requests must not be replayed automatically.
- Adapter maintains physical connections; after Channel failure, link requests a new logical Channel and establishes a new Session. The two layers must not both retry the same session attachment; do not add session recovery or automatic process restart.
- `ChannelInfo.peer`, validation result, and receive registration belong to the corresponding logical Channel. Share a physical connection only when identity and security context are compatible; each Channel still performs its own receive authorization to prevent permission crossover through connection reuse.
- Revoking a registration, Identity, or link cleans only its logical channel and pending deliveries. If revocation of underlying credentials invalidates the shared connection, Adapter closes that connection and notifies every affected channel.

## Connection Establishment and Link Lifecycle

### Connectors

| Connector | Output and constraints |
|---|---|
| `spawn` (inherited fd) | `Channel` + subprocess handle; fd 3 is one socketpair end; default from D2 where supported |
| `spawn` (path) | Temporary-directory socket, subprocess connects back; retained for compatibility |
| `unix::connect` / `unix::listen` | One Channel per call; for tools and frozen reverse direction |
| `memory::pair` | Two connected Channels; for tests or in-process endpoints |
| WebSocket `dial` / `listen` | One Channel per connection, identity in `ChannelInfo.peer`, heartbeat inside channel; `dial` makes one attempt. Request is `Dial { address, peer, identity, protocol }`: upper layer supplies `protocol` as session protocol name; transport checks but does not understand session version |

Each connector call reports one logical Channel establishment result only. It does not retry failed session attachment, replace sessions, or decide link readiness. Adapter may manage a physical connection pool and maintenance internally; one establishment attempt must be cancellable, and internal maintenance must not become an infinite wait or another session-reconnect loop. Link becomes session-ready only after identity check and protocol handshake. This does not mean loader schema or rows are ready. Loader-side companion plugin handles two-phase row readiness independently; it is not a link handshake/readiness condition. On transport or Identity dependency unload, link stops through native framework gating, revokes registrations, and cleans connections; companion plugin cleans rows it manages.

### Establishment Errors

Connection-establishment stage returns structured cross-language categories, separate from `ChannelError` after a channel has been established:

```rust
pub enum ConnectError {
    Retryable { reason: String },
    AuthRejected { reason: String },
    Incompatible { reason: String },
}
```

| Category | Meaning | WebSocket dial-side link behavior |
|---|---|---|
| `Retryable` | Temporary connection failure (refused, timeout, temporary unavailability), or valid credentials but no link yet listening for them (no registration at listener, or listener Identity can validate credentials but corresponding peer link is not registered—for example, dialer becomes ready first) | Exponential backoff and call single-attempt `dial` again |
| `AuthRejected` | Credentials, certificate verification, or identity mapping failed (no listener identity can validate presented credentials) | Slow retry, at most every 30 seconds, and keep reporting error; never bypass verification |
| `Incompatible` | Subprotocol or required transport capability incompatible | Stop automatic retries; wait for config correction or explicit link restart |

- Connector and transport adapter determine category from typed result, protocol state, or verification result; never parse diagnostic text to classify.
- JS and other implementations use equivalent stable category fields; `reason` is diagnostic only and contains no credentials.
- Session handshake occurs after channel establishment. Identity rejection or protocol mismatch during handshake must be returned structurally to link for the corresponding slow-retry/stop behavior; do not infer category from `ChannelError.Closed.reason`.
- `ChannelError` represents runtime channel termination only. Recover normal runtime disconnects per link policy; do not automatically restart local subprocesses.
- After link stops or dependency is revoked, schedule no more retries; an already-started connection result must not be delivered as a new session.

### Shared Listener Registration and Receive Routing

Network listeners are owned by transport plugins; links declare allowed connections through registration handles. Each registration binds at least: listener, transport instance, local endpoint ID, expected peer ID, active Identity validation rules, session protocol, owning link, and registration generation. One listener serves one local endpoint (config entry); registration is unique by (listener, peer). `rutis-bridge`'s `Registrations` implements routing, generation recheck, and mutual exclusion with revocation for reuse by all transports.

| Stage | Mandatory rule |
|---|---|
| Registration validation | Link, transport, and Identity are valid; endpoint ID matches identity binding and validation rule applies to transport; under one listener/local endpoint, only one active link route per peer; reject duplicate or ambiguous registration |
| Inbound authentication | Transport performs applicable identity validation, then looks up registration by verified peer ID; never select authorization target from unverified self-reported ID or request parameters |
| Routing | Deliver only to active registration with matching identity binding. Reject missing/revoked/mismatched registrations directly; do not create implicit link. Rejection category depends on credentials: valid credentials without registration → `Retryable` (WebSocket 503); unverifiable credentials → `AuthRejected` (403) |
| Handoff recheck | After transport handshake and before handing to link, revalidate registration generation, owning link, and Identity. Revocation and handoff must have defined order; old handshake cannot complete handoff after revocation wins |
| Session receive | Link also checks session `hello` endpoint ID equals `ChannelInfo.peer`; not ready before handshake; only link decides whether old session is replaced |
| Revocation | Registration handle is released with link lifecycle. Stop/rebuild of link or Identity revocation revokes old generation and cleans related handshaking connections and transferred channels/sessions; close late results and do not let old registration accept connections |

Revoking one link does not close shared listener or other links. A new link must register again and cannot reuse revoked handles; unloading transport revokes all its registrations.

### WebSocket Reconnect Parameters

- Dial-side link is the sole reconnect owner: exponential backoff, 0.5-second initial delay, 30-second maximum, ±20% jitter; reset after connection stays stable for 60 seconds.
- `AuthRejected`: slow retry every 30 seconds and keep reporting; `Incompatible`: stop retrying.
- Transport `dial` has no retry loop. Subprocess exit does not trigger automatic restart.

## WebSocket Binding

The following applies only to WebSocket transport and is shared across language implementations. This binding maps one WebSocket physical connection to one logical Channel, so close codes, heartbeat, and takeover apply to that connection; this does not constrain the generic Channel contract. If WebSocket multiplexing is needed, Adapter defines a separately negotiated transport framing/binding without changing Session operation format.

| Property | Requirement |
|---|---|
| Address | Configured by listener, e.g. `wss://main.example.com/rutis` |
| Subprotocol | `rutis.<session-protocol-major-version>`; use an actually supported session-format version. Reject mismatch during upgrade handshake with `Incompatible`. Do not reuse a runtime version number for a new incompatible format |
| TLS | Non-loopback address must use `wss`; a front reverse proxy may terminate TLS, in which case backend listener binds only loopback; dialer validates cert with system roots or configured CA |
| Authentication | Upgrade request uses `Authorization: Bearer <token>` or client cert. Identity supplies credentials/rules; transport validates and maps peer ID. Never put token in URL, diagnostics, or logs |
| Peer identity | Listener puts verified endpoint ID in `ChannelInfo.peer`; after cert verification, dialer binds configured expected endpoint ID; both check session `hello` endpoint ID |
| Receive authorization | Follow “Shared Listener Registration and Receive Routing”; valid credentials do not by themselves authorize link delivery |
| Message | One text message per frame, UTF-8 JSON, no newline; binary messages reserved for future binary encoding |
| Size limit | Default 16 MiB, configurable; either direction over limit closes this channel with code 1009; Session sees one `Closed` |
| Heartbeat | Both sides ping every 10 seconds by default; close as lost if no message for 30 seconds; enable TCP keepalive; heartbeat progresses independently of caller executor |
| Close reason | close-frame reason is UTF-8, at most 123 bytes; diagnostics only |
| Normal close | Code 1001 |
| Takeover | After link decides a new connection for the same endpoint ID takes over old session, call old channel's `Closer::replaced()`; WebSocket closes with 4002 and reason `replaced by a new connection`; other transports use ordinary close |

Deployment config determines dial direction between full framework nodes. A remote leaf runtime only listens and is dialed by controller link, because the leaf has no link to own reconnect backoff (see “Remote Lease” in remote design). Direction does not grant feature permissions. Heartbeat uses WebSocket ping/pong, not session-protocol frames.

In the Rust transport, socket-write backpressure does not pause the silence deadline, and explicit close does not wait for a blocked write or flush. Intentional inbound backpressure from a full local receive queue pauses silence detection and restarts it when capacity returns; this is not peer loss. On termination, a close frame or acknowledgement is sent only if the socket is immediately writable; otherwise the physical connection is dropped so a slow-reading peer cannot retain the connection task.

## Local Subprocess Endpoints and Compatibility Interfaces

- `Session` exposes session mechanics through `Connection`; endpoint contracts wrap operations and can use any `Channel`. Leaf runtimes do not need node-bridge operations.
- `Process` = subprocess handle produced by `spawn` + `Session`. D1 preserves all existing constructors/methods; generated code and `CordisRuntimePlugin` continue using this facade.
- `rutis-bridge/local` can launch a full framework node or leaf language runtime (register launch config, dial `spawn:<name>`). Local language runtime uses `rutis-runtime-local::LocalRuntime` to compose transport, link, and runtime attachment; transport itself knows no language. Remove `Process` facade only after generated code moves to its corresponding attachment plugin and passes compatibility acceptance; runtime attachment does not depend on node-bridge features.
- Launch args: `<program> <channel> <plugin-or-project>`; channel is `fd:3`, `unix:/path`, or bare path. Starting with N1, append `--id <id>` if new session format requires endpoint ID; launcher assigns it. Do not pass for compatibility sessions.
- Launcher decides whether to use inherited fd: use if Node runtime package declares `rutisChannels` including `fd`; Python runtime enables through `RuntimePlugin::python`; custom `Launcher` declares `inherit_fd()`, otherwise retain path mode.
- Inherited fd: Rust uses `UnixStream::pair()` and `dup2` to fd 3 in `pre_exec`; inherit only this fd as channel, keep other fds `CLOEXEC`, and preserve stdio for existing uses. stdout carries no protocol frames and may be used by plugin output.
- Cordis bridge package declares `rutisChannels` (e.g. `['unix', 'fd']`) in `package.json`, checked alongside `rutisProtocol` at build time; older versions without `fd` declaration use path mode.
- After EOF, `spawn` Receiver waits at most 1 second for subprocess exit status, then returns `Closed`. Preserve existing exit diagnostics byte-for-byte, e.g. `Cordis process exited with signal: 9 (SIGKILL)`. Do not restart subprocess automatically.
- Frozen reverse-direction `launch` in `server.rs` and `client.mjs` uses Unix connector; keep listen-then-launch behavior unchanged and add no capability.

## Decorators

| Decorator | Contract |
|---|---|
| `trace` | Debug-only, off by default (runtime channel enabled by `RUTIS_INTEROP_TRACE`); records direction, length, and close reason only, never message content |
| `fault` | Test-only (`rutis-channel` `testing` feature): delay, drop then close, half-open (stop forwarding without closing) |

Decorators wrap any Channel, independent of transport, and live in `rutis-channel`. Heartbeat and max message size are implemented/configured by concrete transports, not generic message decorators. Channel contract is exposed as `rutis_channel::testing::contract`, which each implementation runs in its own tests.

## Phases and Versions

| Phase | Deliverables | Acceptance |
|---|---|---|
| D1 | Create `rutis-channel` (contract), `rutis-bridge` (public Transport API), and local/memory transport crates (channel implementations + native plugins); use Channel in `rpc.rs`; split Session out of Process; rename internal session `Peer` to `SessionState` or another name that avoids collision with node Peer; rename JS `peer.mjs` to `session.mjs`; remove newline from Node encoding and modularize worker channel (path mode); move `server::native_error` used by `rpc.rs` out of frozen `server.rs` | Existing tests pass; Unix wire format unchanged; no performance regression; `rpc.rs` and `protocol.rs` no longer reference `std::os::unix` and compile after removing their `cfg(unix)` |
| D2 | Inherited-fd support in Rust, Node, Python; decorators; channel-contract tests; session matrix | Local channels supported by each language pass contract tests and session matrix |
| D3 | Rust `rutis-transport-websocket` + WebSocketPlugin; JS/Python network transport support; single-attempt `dial` / `listen`; structured establishment errors; listener-registration API between transport and link | Rust/JS bidirectional interop and Rust/Python runtime attachment; loopback WebSocket session matrix passes; verify retry categories, registration revocation, runtime leases, and takeover with remote design |

- D1/D2 do not change integration-baseline wire format or protocol version; Unix and inherited fd remain newline-delimited JSON. Do not downgrade already-used multilingual PROTOCOL 2 implementation to version 1.
- WebSocket subprotocol follows actual Session major version; internal channel changes do not independently bump Session protocol. Allocate a major version for incompatible session format added by remote design; settle release number before implementation.
- Remote design specifies shared attachment plugin, node-bridge features, and leaf-runtime attachment. Network attachment requires D3; node-bridge features are not a prerequisite for runtime attachment. Do not treat existing PRs or local-channel support as network support.

## Tests and Completion Criteria

| Test group | Must cover |
|---|---|
| Reusing Adapter contract | Two logical Channels share physical connection; isolate messages, flow control, identity; closing/revoking/replacing one does not affect the other; physical failure notifies every affected channel; terminated channel is not revived and calls are not replayed; only one retry owner per session attach. If no production reuse Adapter exists, use test Adapter to verify upper layers do not assume one-to-one |
| Per-implementation channel contract | Concurrent send ordering after send-lock serialization; boundary preservation and near-limit large messages; backpressure; idempotent close; close wakes blocked send/recv; WebSocket close-reason forwarding; channel keeps progressing and can close while caller `current_thread` is blocked by synchronous call |
| Session matrix | `rpc/tests.rs` uses memory; real Node process parameterized over path, inherited fd, loopback WebSocket; same session-semantic tests pass on every channel |
| Node | In-memory session harness; framing tests for path and fd; worker continues send/receive and heartbeat while main thread synchronously waits |
| Cross-implementation WebSocket | Rust listener ↔ JS dialer, JS listener ↔ Rust dialer; auth failure, cert validation, subprotocol mismatch, message too large, heartbeat timeout, half-open, takeover, close codes |
| Establishment errors/retry | `Retryable` backoff, `AuthRejected` slow retry, `Incompatible` stop; 0.5 sec/30 sec/±20%/60 sec params; changing diagnostic text does not change branch; no reconnect after link stop; no automatic local subprocess restart |
| Shared listener | Reject no registration; reject duplicate/ambiguous registration; route by verified identity; revoke during handshake, Identity revoke, link rebuild, and late handoff never reuse old registration; cleaning one target link does not affect other links |
| Security and identity | Mismatched endpoint ID in `hello` cannot become session-ready; no credential leakage; non-loopback TLS rule; unverified endpoint ID never participates in authorization routing |
| Endpoint and responsibility separation | Full framework node and leaf runtime without rutis can both use shared transport/identity/link/Session; link does not depend on loader/schema/`PeerRows`/`RuntimeRows`; session readiness and two-phase row readiness tested separately (loader-side companion plugin owns latter); companion plugin cleans rows after session replacement/revocation |
| Operation-contract attachment | Integrate `rows.*` / `hosts.*` and `plugins.*` / `services.*` by endpoint contract, do not force-unify; leaf runtime attaches without node-bridge features; full framework nodes manage their own plugins and honor feature permissions |
| Performance | Compare Unix channel before/after D1 using existing bench fixture; regression must stay within noise |

Completion requires tests for corresponding phases to pass; adding a transport must not modify `rutis-interop`; protocol and session layers must no longer depend on Unix; Rust/JS must interoperate bidirectionally; D1/D2 Unix compatibility format remains unchanged. This design does not claim that implementation or acceptance is complete.

## Related Specifications

- [Compatibility Layer Design](design-protocol-plugin-mount.md): preserve local-runtime frame semantics; new network session format follows remote design; channel isolates protocol frames from stdout.
- [Remote Design](design-remote-plugins-2026-10-03.md): two remote endpoint types, session handshake/capability format, endpoint-operation contract, shared attachment plugin, node-bridge features, loader-side companion plugin, and permissions.
- [Requirements: Mounting Cordis Plugins](requirements-protocol-plugins.md): preserve constraint not to modify rutis kernel or Cordis.
