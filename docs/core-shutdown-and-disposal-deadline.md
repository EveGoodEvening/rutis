# Root shutdown 与卸载等待截止时间

`dispose()` 保持原有的无限等待及 root 可重启语义。新增 `FiberView::dispose_with_timeout(limit)`：它同步登记同一个 dispose 任务，只给调用方等待设置截止时间。超时结果包含 plugin id、generation、当前状态和已等待时间；`Loading` 表示 apply 尚未退出，`Unloading` 表示清理或依赖消费者级联仍在进行；意图排在前一项长任务之后时，也可能观察到 `Active` 或 `Pending`。超时后驱动继续工作，重复 `dispose()` 会 join 原任务，最终清理错误沿用同一个 `Arc<CordisError>`。`timeout` 不宣称插件已停止，也不改变服务图状态。

`dispose()` 的同步登记同时关闭新代准入：即使初次依赖刷新或 restart 已在队列里，尚未准入的那一代也不会执行 `apply`。若新代已先准入，dispose 必须取消该代安装的 token，使等待 `ctx.cancelled()` 的 apply 能协作退出；终态登记与换代共用 transition 锁。

`Ctx::shutdown()` 是 root 的最终关闭入口。它先禁止新的服务、effect 与插件装载，再等待准入关闭前完成挂载的子树清理并结束 root 驱动；重复或并发调用共享一次完成结果。若子 fiber 已创建、但挂载恰好被 root 关闭拒绝，它不会进入 `apply`，会独立关闭；root 的完成结果不等待这个未挂载的 driver，持有返回的 `FiberView` 可用 `shutdown()` 等待它退出。`Ctx::shutdown_with_timeout(limit)` 仅限制这次等待，超时后 root 仍处于关闭过程，后续 `shutdown()` 可继续 join。root 的 `dispose()` / `restart()` 仍可配对使用，直到调用 `shutdown()`。关闭后 `restart` 和新注册返回 `CordisError::Closed`；`plugin()` 因原 API 返回 `FiberView`，会返回一个等待结果为 `Closed` 的终态 view。`Ctx::root_view()` 改为 `Option<FiberView>`，在 root 驱动结束且最后一个 view 释放后返回 `None`。

本次不实现超时后的强制隔离。旧代 apply 或清理若不观察取消信号，可能继续持有能力；因此等待超时不能安全地把它标记为已停止，也不能启动下一代。Tokio 的计时器也无法在占满同一 runtime 线程且不让出执行权的同步代码中准时触发。进程及进程树管理由调用方工具层承担。
