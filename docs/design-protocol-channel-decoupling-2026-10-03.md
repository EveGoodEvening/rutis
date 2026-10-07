# 网络栈：协议与通道解耦（设计稿）

> 包结构已调整（2026-10-06）：`rutis-channel`、`rutis-interop`、`rutis-transport-*`、`rutis-runtime-local` 合并为 `rutis-bridge` 的模块（`channel`、`session`、`runtime`、`transport::*`、`cordis`），见 [面向开发者的包、工具与流程](design-developer-packages-2026-10-06.md)。下文保留设计时的 crate 名。


状态：设计稿，未实现。修订日期：2026-10-05。
依据：[兼容层设计](design-protocol-plugin-mount.md)、[挂载 Cordis 插件：需求](requirements-protocol-plugins.md)。
使用方：[远程插件设计](design-remote-plugins-2026-10-03.md)（下称“远程稿”）。

## 范围与边界

本文规定跨语言通道契约、连接器、分帧与编码边界、WebSocket 绑定及实施验收。Rust、JS、其他语言的实现遵守相同契约。

- 协议只依赖有序、可靠、保持消息边界的双向通道；通道只搬运不透明字节。
- 一个 Session 使用一个逻辑 Channel；逻辑 Channel 与物理连接的映射由 Transport Adapter 决定，不要求一一对应。连接池、连接复用和多路复用不进入会话协议；框架节点组合在框架层完成。
- 支持两种远程端点：A 为完整框架节点，两端各自管理插件，按需配置 `export`、`import`、`host`、`events`；B 为叶子语言运行时，由本地 rutis 与 loader 管理，远端不运行 rutis。
- 两种端点共享承载、身份、link 与 Session；运行时接入独立于节点桥功能，不要求叶子运行时实现完整框架节点能力。
- 会话握手与能力格式、端点操作契约、权限与框架节点组合由远程稿规定；rutis 内核和 Cordis 不改。
- rutis-dev 开发协议、Windows 命名管道实现不在本文范围。

## 分层与插件结构

| 层 | 职责 | 实现与约束 |
| --- | --- | --- |
| 连接器 | 单次拨号、接收连接、拉起子进程端点、执行承载适用的身份验证 | Rust 契约归 `rutis-channel`，实现归具体 `rutis-transport-*` crate；JS 位于 I/O worker；不负责重连 |
| Channel | 有序可靠消息流、分帧、背压、存活检测、关闭 | Rust 契约归 `rutis-channel`，实现归具体承载 crate；JS 位于桥包 `src/channel/*.mjs`；不认识协议帧和编码 |
| codec | 协议帧与字节互转 | Rust 位于 `rutis-interop` 内部；JS 为 `src/codec.mjs`；不依赖通道种类 |
| Session | 协议握手、调用、引用、取消、调用链和错误 | Rust 为 `rpc.rs`；JS 为 `session.mjs`；不依赖通道种类或身份验证机制，按端点契约接入操作封装 |
| 端点操作 | 完整框架节点的服务公告、代装插件、事件转发；叶子运行时的运行时操作 | 节点操作归 `rutis-bridge`，运行时契约归 `rutis-interop`；各语言提供对应适配；不依赖通道种类，不强行合并 `rows.*` / `hosts.*` 与 `plugins.*` / `services.*` |
| 接入与功能插件 | 共享承载、身份、link；节点桥功能与运行时接入分别配置 | 按下表分工 |

`rutis-interop` 只通过 `Channel` 使用通道；`rutis-channel` 不依赖任何协议 crate。`Channel`、`Session`、codec 是机制库，不要求分别插件化。

| 插件 | 契约 |
| --- | --- |
| 承载 | 提供 `Transport#<种类>` 服务；包装连接器，持有监听器，执行承载特有的鉴权与心跳，向 link 交付通道 |
| 身份 | 提供凭据、对端身份映射和验证规则；身份撤销触发依赖 link 的清理 |
| link | 依赖承载；网络链接依赖 Identity，本机子进程端点身份由启动方指定；持有监听注册；仅负责连接、会话身份核对与协议握手、重连退避、会话替换、会话就绪及相关清理；不认识 loader、schema 或行，不负责 `PeerRows` / `RuntimeRows` |
| 节点桥功能 | 完整框架节点按需分别配置 `export`、`import`、`host`、`events`；权限由两端为对方安装的功能插件决定，与拨号方向无关 |
| 运行时接入 | interop 的 RuntimePlugin 与 bridge 的接入 Adapter 负责会话接入；loader 侧配套插件按端点契约管理 `PeerRows` / `RuntimeRows` 和第二阶段行就绪，处理会话替换与撤销；不要求安装节点桥功能 |
| `peer` 组合 | 只组合已有插件，不另实现连接、鉴权、心跳或 link 生命周期 |

承载以独立 crate 中的原生 rutis 插件交付：`rutis-transport-local` 导出 LocalPlugin，`rutis-transport-websocket` 导出 WebSocketPlugin，`rutis-transport-memory` 导出 MemoryPlugin。插件配置名保留 `rutis-bridge/local`、`rutis-bridge/websocket`、`rutis-bridge/memory`；crate 名不等于插件名。每个 crate 同时持有通道实现与插件生命周期包装，不另设附属的 `plugin` feature。

插件校验配置、提供 Transport 服务、管理监听器/连接/子进程，卸载时撤销服务并清理资源，使用它的 link 通过原生门控停止。local 内部包含 Unix、fd、spawn；memory 也可用于进程内互联。

`rutis-bridge` 定义 Transport 公共服务接口，持有 identity、link 和节点功能插件；具体承载依赖 bridge 和 channel，bridge 不反向依赖具体承载。应用装配层选择承载并完成需要自建承载的组合。运行时通用会话入口归 interop，Peer 到该入口的 Adapter 归 bridge；loader 只负责行侧集成，bridge/interop 不依赖 loader。所有 crate 留在同仓库，核心 `rutis` 不变；不用网络不引入 WebSocket 依赖。

WebSocket 仅是一种承载。新增承载不修改 link 或协议层，不强制其他承载采用 WebSocket 的认证方式，也不放宽其安全规则。

## Channel 契约

### 消息与执行

| 项 | 规定 |
| --- | --- |
| 顺序与交付 | 按发送顺序交付，不丢失、不重复；不满足要求的介质须在通道内部补齐序号、确认和重传 |
| 消息边界 | 一帧对应一条消息；不得拆分、合并、解析、修改或注入消息 |
| 双向通信 | 全双工；同步调用等待回复期间仍能接收反向调用 |
| 背压 | 有界缓冲；发送方在背压下等待，不无限积压 |
| 独立推进 | 不依赖调用方执行器；Rust 接口阻塞式，异步承载自带线程和运行时；Node 在 I/O worker 推进，其他语言使用独立线程等等价机制 |
| 存活 | 静默失效的网络介质须在有限时间内发现失联；本机 socket 使用 EOF |
| 身份 | 仅经 `ChannelInfo` 交付，不注入消息流；会话握手中的端点 id 必须与连接器确认的对端身份一致 |
| 结束 | 可观察并带诊断原因；在途调用以 `Transport` 失败；关闭原因仅供诊断，不用于程序分支 |

### Rust 接口

```rust
// rutis-channel，设计接口
pub struct Channel {
    pub sender: Box<dyn Sender>,
    pub receiver: Box<dyn Receiver>,
    pub closer: Arc<dyn Closer>,
    pub info: ChannelInfo,
}

pub trait Sender: Send {
    // 发送一条消息；背压下阻塞。
    fn send(&mut self, message: &[u8]) -> Result<(), ChannelError>;
}

pub trait Receiver: Send {
    // 阻塞到下一条消息；Ok(None) 表示对端正常结束。
    fn recv(&mut self) -> Result<Option<Vec<u8>>, ChannelError>;
}

pub trait Closer: Send + Sync {
    // 幂等；唤醒阻塞在 send 和 recv 上的线程。
    fn close(&self, reason: &str);
}

pub struct ChannelInfo {
    pub transport: &'static str, // "unix" | "fd" | "memory" | "websocket" …
    pub peer: Option<PeerId>,    // 经连接器确认；本机子进程端点由启动方指定
    pub label: String,          // 诊断标签，例如 "peer mac"
}

pub enum ChannelError {
    // 通道结束；为什么结束（对端断开、消息超出承载上限、心跳失联）是承载自己的事。
    Closed { reason: String },
}
```

- 契约只规定有序、可靠、保持边界、背压与关闭。分帧、存活检测、消息大小上限属于具体承载协议，由各 Adapter 实现和配置，不进入 `rutis-channel` 契约，会话也不为它们定义特殊语义。

- 会话在发送锁内完成编码和发送，保持引用表变化与发送顺序一致。
- `close` 先在发送锁外打断阻塞发送，再取得锁清理表。
- 读线程循环 `recv()`，解码后交给会话；`Closed { reason }` 映射为 `Error::Transport("<label>: <reason>")`。移除会话入口的 `disconnected` 闭包。
- 会话入口为 `Connection::open(Channel, dispatch)`；已有 `connect` 可保留为 Unix 通道的兼容简写。
- `PeerId` 表示会话端点 ID，不要求对端是完整框架节点。`ChannelInfo.peer` 的可选类型兼容无需端点身份的通道用途；link 不得将缺少已确认对端身份的通道标为会话就绪。

### Node 与其他语言

Node 通道在 I/O worker 内提供等价接口：

```js
// open(spec, { message(bytes), closed(reason) }) → { send(bytes), close(reason) }
// 建立失败另行返回结构化 ConnectError，不通过 closed(reason) 推断。
```

- 通道规格：`unix:<path>`、`fd:<n>`、`ws://…`（仅回环）/`wss://…`、`listen:ws://…`/`listen:wss://…`；裸路径等同 `unix:`。`listen:` 用于运行时等待控制方拨入，接受第一条通过鉴权的连接；凭据、CA、证书与私钥经环境变量 `RUTIS_INTEROP_TOKEN`、`RUTIS_INTEROP_CA`、`RUTIS_INTEROP_CERT`、`RUTIS_INTEROP_KEY` 传入，不进入通道规格、URL 或命令行。Python 运行时的 WebSocket 接入为可选依赖 `rutis-runtime[network]`。
- 主线程与 worker 沿用 MessagePort + `Atomics` 交接；主线程同步等待时，worker 继续收发和处理心跳。
- 会话使用不带换行的 `codec.encode`；worker 可调用 `codec.decode`，通道模块不决定编码。
- JS 接受 WebSocket 连接时使用支持服务端的依赖（如 `ws`）；仅拨号不要求服务端能力。
- 其他语言遵守相同契约；Python 本机子进程端点可用 `socket.socket(fileno=3)` 打开继承 fd。

## 分帧与编码

| 项 | 规定 |
| --- | --- |
| 编码 | 协议层使用紧凑 JSON；字符串内换行转义；codec 为内部接口，当前不开放替换 |
| 字节流 | Unix socket、继承 fd 按换行分帧；发送时由通道加换行，接收时由通道移除分隔符 |
| WebSocket | 一条文本消息对应一帧 UTF-8 JSON，不附带换行 |
| 后续二进制编码 | 字节流使用长度前缀，WebSocket 使用二进制消息；由连接器在会话建立前确定两端编码，不在协议帧内协商 |

## 逻辑通道与物理连接

上层多个插件共享 Session：一个 Runtime 实例的多个插件共用其活动控制 Session；一个节点 link 的桥功能及其代装插件共用该 link 的活动 Session。插件按实例 key 和资源 ID 区分，装卸单个插件不创建或关闭 Session。该共享模型与 Adapter 的物理连接复用互相独立，资源清理范围见远程稿“Session 共享与资源归属”。

- Adapter 决定一个逻辑 Channel 独占物理连接，或多个 Channel 共享物理连接；Session 和 link 不访问物理连接句柄。
- send/recv、顺序、背压和 close 契约均以逻辑 Channel 为单位。复用实现隔离消息边界与流控，不允许一个通道的阻塞导致其他通道无界积压。
- 关闭或替换会话只释放其逻辑 Channel；共享物理连接及其他 Channel 保持有效。Adapter 决定空闲连接保留与回收。
- 物理连接故障由 Adapter 通知全部受影响的 Channel。无法继续满足有序可靠契约时必须终止 Channel；终止后的 Channel 不得通过换物理连接重新变为可用，也不得自动重放 Session 请求。
- Adapter 维护物理连接；link 在 Channel 失败后重新申请逻辑 Channel 并建立新 Session。两层不得各自重试同一次会话接入；不新增会话恢复或进程自动重启。
- `ChannelInfo.peer`、验证结果和接收注册归属于对应逻辑 Channel。只有身份与安全上下文兼容时才可共享物理连接；每个通道仍须独立完成接收授权，禁止因连接复用串用权限。
- 撤销注册、Identity 或 link 仅清理其逻辑通道与待交付结果；若底层凭据撤销使共享连接整体失效，Adapter 关闭该连接并通知所有受影响通道。

## 连接建立与 link 生命周期

### 连接器

| 连接器 | 产出与约束 |
| --- | --- |
| `spawn`（继承 fd） | `Channel` + 子进程句柄；fd 3 为 socketpair 一端；D2 起作为支持该能力时的默认方式 |
| `spawn`（路径） | 临时目录 socket、子进程回拨；保留兼容 |
| `unix::connect` / `unix::listen` | 每次产出一个 `Channel`；用于工具及冻结的反方向 |
| `memory::pair` | 两条首尾相连的 `Channel`；用于测试或进程内端点 |
| WebSocket `dial` / `listen` | 每次连接产出 `Channel`，身份放入 `ChannelInfo.peer`；心跳在通道内部；`dial` 只尝试一次。拨号请求为 `Dial { address, peer, identity, protocol }`：`protocol` 是会话协议名，由上层传入，承载只核对，不认识会话版本 |

连接器每次调用只报告一次逻辑 Channel 建立结果，不自行重试失败的会话接入、不替换会话、不决定 link 会话就绪。Adapter 可内部管理物理连接池与连接维护；单次建立必须可取消，内部维护不得演变为无限等待或另一个会话重连循环。link 完成身份核对和协议握手后才进入会话就绪状态；此状态不表示 loader 的 schema 或行已就绪。两阶段行就绪由 loader 侧配套插件独立完成，不作为 link 的握手或就绪条件。卸载承载或身份依赖时，link 按框架原生门控停止、撤销注册并清理连接；配套插件清理其管理的行。

### 建立阶段错误

连接建立阶段须返回跨语言一致的结构化类别，独立于已建立通道的 `ChannelError`：

```rust
pub enum ConnectError {
    Retryable { reason: String },
    AuthRejected { reason: String },
    Incompatible { reason: String },
}
```

| 类别 | 含义 | WebSocket 拨号侧 link 行为 |
| --- | --- | --- |
| `Retryable` | 暂时性连接失败，如连接拒绝、超时、临时不可用；以及凭据有效但尚无 link 监听它（监听器上没有任何注册，或监听器的身份能验证该凭据、但对应对端的 link 尚未注册，例如拨号方先于监听 link 就绪） | 指数退避后再次调用单次 `dial` |
| `AuthRejected` | 凭据、证书验证、身份映射未通过（监听器上没有任何身份能验证所出示的凭据） | 按 30 秒上限间隔慢重试并持续报告错误，不绕过验证 |
| `Incompatible` | 子协议或必要承载能力不兼容 | 停止自动重试，等待配置修正或显式重启 link |

- 连接器和承载适配层从类型、协议状态或验证结果生成类别；不得解析诊断文字分类。
- JS 等实现使用等价的稳定类别字段；`reason` 仅供诊断，不含凭据。
- 会话握手发生在通道建立之后；握手的身份拒绝、协议不兼容须以结构化结果交给 link，采用对应慢重试或停止策略，不从 `ChannelError.Closed.reason` 反推类别。
- `ChannelError` 只表达通道运行期的结束。正常运行期断线按 link 策略恢复；本地子进程不自动重启。
- link 停止或依赖撤销后不得继续安排重试；已发起的连接结果不得再交付为新会话。

### 共享监听器注册与接收路由

网络监听器由承载插件持有，link 通过注册句柄声明允许接收的连接。注册至少绑定：监听器、承载实例、本地端点 id、预期对端 id、有效 Identity 验证规则、会话协议、所属 link 及本次注册代次。每个监听器服务一个本地端点（配置项），注册按（监听器，对端）唯一。路由、代次复核与撤销互斥由 `rutis-bridge` 的 `Registrations` 统一实现，各承载复用。

| 环节 | 强制规则 |
| --- | --- |
| 注册验证 | link、承载与 Identity 均须有效；端点 id 与身份绑定须一致，验证规则须适用于该承载；同一监听器与本地端点下，同一对端只允许一个有效 link 路由，重复或歧义注册拒绝 |
| 入站认证 | 承载执行适用的身份验证，以验证得到的对端 id 查找注册；不得以未验证的自报 id 或请求参数直接选定授权目标 |
| 路由 | 仅交给身份绑定匹配的有效注册；无注册、已撤销或身份不符直接拒绝，不创建隐式 link。拒绝的类别按凭据区分：凭据可验证而无注册为 `Retryable`（WebSocket 503），凭据无法验证为 `AuthRejected`（403） |
| 移交复核 | 承载握手完成、移交 link 前，重新核验注册代次、所属 link 和 Identity 仍有效；撤销与移交须有确定的先后顺序，旧握手不能越过撤销完成移交 |
| 会话接收 | link 再核对会话 `hello` 的端点 id 与 `ChannelInfo.peer` 一致；完成握手前不得就绪；旧会话仅由 link 决定是否替换 |
| 撤销 | 注册句柄随 link 生命周期释放；link 停止、重建或 Identity 撤销时撤销旧代次，清理相关握手中连接和已移交通道/会话；迟到结果关闭，旧注册不得继续接受连接 |

撤销一个 link 不关闭共享监听器或其他 link。新 link 必须重新注册，不能复用已撤销句柄；承载卸载时撤销该承载全部注册。

### WebSocket 重连参数

- 拨号侧 link 唯一负责重连：指数退避，初始 0.5 秒，上限 30 秒，±20% 抖动；连接稳定 60 秒后重置。
- `AuthRejected` 按 30 秒间隔慢重试并持续报错；`Incompatible` 停止重试。
- 承载 `dial` 不包含重试循环。子进程退出不触发自动重启。

## WebSocket 绑定

以下规则只约束 WebSocket 承载；各语言实现按此互通。此绑定采用一条 WebSocket 物理连接对应一个逻辑 Channel，因此关闭码、心跳和接管作用于该连接；这不是通用 Channel 契约的限制。需要 WebSocket 多路复用时，由 Adapter 定义独立协商的承载分帧与绑定，不修改 Session 操作格式。

| 项 | 规定 |
| --- | --- |
| 地址 | 监听方配置，例如 `wss://main.example.com/rutis` |
| 子协议 | `rutis.<会话协议主版本>`；使用实际支持的会话格式版本，不符时在升级握手阶段拒绝，返回 `Incompatible`。新增不兼容格式不得复用现有运行时的版本号 |
| TLS | 非回环地址必须使用 `wss`；允许前置反向代理终止 TLS，此时后端监听器只听回环地址；拨号方用系统根证书或配置 CA 校验证书 |
| 鉴权 | 升级请求携带 `Authorization: Bearer <token>`，或使用客户端证书；Identity 提供凭据和规则，承载执行验证并映射对端 id；不得将 token 放入 URL、诊断或日志 |
| 对端身份 | 监听方将验证得到的端点 id 放入 `ChannelInfo.peer`；拨号方在证书验证通过后绑定配置的预期端点 id；双方均核对会话 `hello` 的端点 id |
| 接收授权 | 按“共享监听器注册与接收路由”执行；通过凭据验证不等于获得 link 接收授权 |
| 消息 | 一帧一条文本消息，UTF-8 JSON，不带换行；二进制消息保留给后续二进制编码 |
| 大小上限 | 默认 16 MiB，可配置；任一方向超限都以关闭码 1009 关闭该通道，会话看到的是一次 `Closed` |
| 心跳 | 双方默认每 10 秒发一次 ping；30 秒内收不到任何消息即判定失联并关闭；启用 TCP keepalive；心跳独立于调用方执行器推进 |
| 关闭原因 | close frame 的 reason 为 UTF-8，最多 123 字节；只供诊断 |
| 有序关闭 | 关闭码 1001 |
| 接管 | link 决定同一端点 id 的新连接接管旧会话后，调用旧通道的 `Closer::replaced()`；WebSocket 以 4002 关闭，reason 为 `replaced by a new connection`，其他承载普通关闭 |

完整框架节点之间的拨号方向由部署配置；远程叶子运行时只监听，由控制方 link 拨号，因为叶子侧没有 link 持有重连退避（见远程稿“远端租约”）。方向不授予功能权限。心跳使用 WebSocket ping/pong，不注入会话协议帧。

Rust 承载的 socket 写入背压不暂停静默截止时间，显式关闭也不等待阻塞的写入或 flush。本地接收队列满导致的主动入站背压则暂停静默判断，容量恢复后重新计时；这不是对端失联。关闭时仅在 socket 立即可写时发送关闭帧或确认，否则释放物理连接，避免慢读对端拖住连接任务。

## 本机子进程端点与兼容接口

- `Session` 以 `Connection` 提供会话机制，按端点契约接入操作封装，可建立在任意 `Channel` 上；不要求叶子运行时具备节点桥操作。
- `Process` = `spawn` 产出的子进程句柄 + `Session`；D1 保留全部已有构造函数和方法，生成代码及 `CordisRuntimePlugin` 继续使用该外观。
- `rutis-bridge/local` 可拉起完整框架节点或叶子语言运行时（登记拉起配置，拨号 `spawn:<名字>`）；本机语言运行时由 `rutis-runtime-local` 的 `LocalRuntime` 组合承载、link 与运行时接入，承载本身不认识语言。生成代码迁至对应接入插件并完成兼容验收后，才可移除 `Process` 外观，运行时接入不以节点桥功能落地为前提。
- 启动参数：`<程序> <通道> <插件或 project>`；通道为 `fd:3`、`unix:/path` 或裸路径。N1 起新会话格式需要端点 id 时追加 `--id <id>`，由启动方指定；兼容会话不传。
- 是否用继承 fd 由启动方决定：Node 运行时包声明 `rutisChannels` 含 `fd` 时使用；Python 运行时由 `RuntimePlugin::python` 启用；自定义 `Launcher` 以 `inherit_fd()` 声明，未声明的沿用路径方式。
- 继承 fd：Rust 用 `UnixStream::pair()`，在 `pre_exec` 中 `dup2` 到 fd 3；只允许该 fd 作为通道被继承，其余 fd 保持 `CLOEXEC`，标准流按原有用途保留。stdout 不承载协议帧，可供插件输出。
- cordis 桥包在 `package.json` 声明 `rutisChannels`（如 `["unix", "fd"]`），构建时与 `rutisProtocol` 一并核对；未声明 `fd` 的旧版本使用路径方式。
- `spawn` 的 Receiver 在 EOF 后最多等 1 秒获取子进程退出状态，再返回 `Closed`；保留已有退出诊断的逐字兼容，例如 `Cordis process exited with signal: 9 (SIGKILL)`。子进程不自动重启。
- 冻结反方向的 `server.rs` 和 `client.mjs` 的 `launch` 使用 Unix 连接器；保留监听后拉起模式，行为不变、不增加能力。

## 装饰器

| 装饰器 | 契约 |
| --- | --- |
| `trace` | 调试用，默认关闭（运行时通道由 `RUTIS_INTEROP_TRACE` 开启）；只记录方向、长度与关闭原因，不记录消息内容 |
| `fault` | 仅测试（`rutis-channel` 的 `testing` feature）：延迟、丢弃后关闭、半开（停止转发但不关闭） |

装饰器包装任意 `Channel`，与具体承载无关，位于 `rutis-channel`。心跳与消息大小上限由具体承载实现和配置，不作为通用消息装饰器。通道契约本身以 `rutis_channel::testing::contract` 的形式提供，每种实现在自己的测试中运行。

## 阶段与版本

| 阶段 | 交付 | 验收 |
| --- | --- | --- |
| D1 | 新建 `rutis-channel`（契约）、`rutis-bridge`（Transport 公共接口）及 local/memory 承载 crate（通道实现与原生插件）；`rpc.rs` 使用 `Channel`；从 `Process` 拆出 Session；会话内部 `Peer` 改为 `SessionState` 等不与节点 Peer 混淆的名称，JS `peer.mjs` 改为 `session.mjs`；Node 编码去掉换行，worker 通道模块化（路径方式）；将 `rpc.rs` 借用的 `server::native_error` 移出冻结的 `server.rs` | 现有测试全通过；Unix 线格式不变；性能不退化；`rpc.rs`、`protocol.rs` 不再引用 `std::os::unix`，移除其 `cfg(unix)` 后可编译 |
| D2 | Rust、Node、Python 的继承 fd 接入；装饰器；通道契约测试；会话矩阵 | 各语言支持的本地通道通过契约测试和会话矩阵 |
| D3 | Rust `rutis-transport-websocket` 及 WebSocketPlugin、JS 与 Python 网络承载接入；单次 `dial` / `listen`；结构化建立错误；承载与 link 的监听注册接口 | Rust/JS 双向互通及 Rust/Python 运行时接入；回环 WebSocket 会话矩阵通过；结合远程稿验收重试分类、注册撤销、运行时租约与接管 |

- D1、D2 不修改接入基线的线格式与协议版本；Unix 与继承 fd 上仍为逐行 JSON，不将已使用 PROTOCOL 2 的多语言实现退回版本 1。
- WebSocket 子协议跟随实际会话主版本；通道内部变化不自行升级会话协议。远程稿新增的不兼容会话格式另行分配主版本，实施前确定发布编号。
- 远程稿规定共享接入插件、节点桥功能与叶子运行时接入；网络接入须依赖 D3 交付，节点桥功能不作为运行时接入的前置条件。现有 PR 或本机通道接入不据此视为已支持网络。

## 测试与完成条件

| 测试组 | 必须覆盖 |
| --- | --- |
| 复用 Adapter 契约 | 两个逻辑 Channel 共用物理连接；消息、流控和身份隔离；关闭、撤销、替换一个通道不影响另一个；物理故障通知所有受影响通道；终止通道不复活、不重放调用；会话接入只有一个重试所有者。无生产复用 Adapter 时由测试 Adapter 验证上层无一对一假设 |
| 通道契约：每种实现 | 发送锁串行化后的并发发送顺序；边界保持及接近上限的大消息；背压；幂等关闭；关闭唤醒阻塞的 send/recv；WebSocket 关闭原因传递；调用方 `current_thread` 被同步调用阻塞时通道仍推进、可关闭 |
| 会话矩阵 | `rpc/tests.rs` 使用 memory；真实 Node 进程按路径、继承 fd、回环 WebSocket 参数化；同一组会话语义测试在各通道通过 |
| Node | 会话内存 harness；路径及 fd 的分帧测试；主线程同步等待时 worker 持续处理收发与心跳 |
| WebSocket 跨实现 | Rust 监听 ↔ JS 拨号、JS 监听 ↔ Rust 拨号；鉴权失败、证书验证、子协议不符、消息超限、心跳超时、半开、接管和关闭码 |
| 建立错误与重试 | `Retryable` 退避、`AuthRejected` 慢重试、`Incompatible` 停止；0.5 秒/30 秒/±20%/60 秒参数；改变诊断文字不改变分支；停止 link 后无迟到重连；本地子进程不自动重启 |
| 共享监听器 | 无注册拒绝；重复/歧义注册拒绝；按已验证身份路由；握手期间撤销、Identity 撤销、link 重建、迟到移交均不得复用旧注册；清理目标 link 不影响其他 link |
| 安全与身份 | `hello` 端点 id 不符不得会话就绪；无凭据泄漏；非回环 TLS 约束；未经验证的端点 id 不参与授权路由 |
| 端点与职责隔离 | 完整框架节点与无 rutis 的叶子运行时均可使用共享承载、身份、link、Session；link 不依赖 loader、schema、`PeerRows` / `RuntimeRows`；会话就绪与两阶段行就绪分别验收，后者由 loader 侧配套插件负责；会话替换、撤销后由配套插件清理行 |
| 操作契约接入 | `rows.*` / `hosts.*` 与 `plugins.*` / `services.*` 按端点契约接入，不强行合并；不安装节点桥功能也可完成叶子运行时接入；完整框架节点各自管理插件并按功能权限工作 |
| 性能 | 使用既有 bench fixture 对比 D1 前后 Unix 通道，退化不得超出噪声范围 |

完成条件：以上对应阶段测试通过；新增承载不修改 `rutis-interop`；协议和会话层不再依赖 Unix；Rust/JS 可双向互通；D1、D2 的 Unix 兼容格式保持不变。本稿不代表上述实现或验收已经完成。

## 关联规范

- [兼容层设计](design-protocol-plugin-mount.md)：本机运行时帧语义保持不变，新网络会话格式按远程稿；协议帧与 stdout 隔离由通道保证。
- [远程稿](design-remote-plugins-2026-10-03.md)：两种远程端点、会话握手与能力格式、端点操作契约、共享接入插件、节点桥功能、loader 侧配套插件与权限。
- [挂载 Cordis 插件：需求](requirements-protocol-plugins.md)：保留不修改 rutis 内核和 Cordis 的约束。
