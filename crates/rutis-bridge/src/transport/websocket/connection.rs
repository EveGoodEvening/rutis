//! One WebSocket connection as a [`Channel`]: a task on the transport's
//! runtime moves messages between the socket and two pipes, pings, watches
//! for silence, and closes with the code the situation calls for.
//!
//! One physical connection carries one logical channel, so close codes and
//! heartbeats act on the connection.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::channel::{Channel, ChannelError, ChannelInfo, Closer, Receiver, Sender};
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

use crate::transport::websocket::pipe::{Ending, Pipe};
use crate::transport::websocket::Limits;

/// Orderly close.
pub(crate) const GOING_AWAY: u16 = 1001;
/// A message over the size limit.
pub(crate) const TOO_BIG: u16 = 1009;
/// A newer connection of the same endpoint took over.
pub(crate) const REPLACED: u16 = 4002;
/// Close frame reasons are at most 123 bytes.
const REASON_LIMIT: usize = 123;

/// Any byte stream a WebSocket can run on: TCP, or TLS over TCP.
pub(crate) trait Io: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Io for T {}

/// A close the local side asked for.
#[derive(Default)]
struct Control {
    request: Mutex<Option<(u16, String)>>,
    notify: Notify,
}

impl Control {
    fn request(&self, code: u16, reason: &str) {
        self.request
            .lock()
            .unwrap()
            .get_or_insert_with(|| (code, reason.to_owned()));
        self.notify.notify_one();
    }

    async fn requested(&self) -> (u16, String) {
        loop {
            let notified = self.notify.notified();
            if let Some(request) = self.request.lock().unwrap().clone() {
                return request;
            }
            notified.await;
        }
    }
}

struct Shared {
    outgoing: Pipe,
    incoming: Pipe,
    control: Control,
    max_message: usize,
}

impl Shared {
    /// Close locally: both pipes end at once, so blocked callers wake even
    /// before the task sends the close frame.
    fn close(&self, code: u16, reason: &str) {
        self.control.request(code, reason);
        let ending = Ending::Failed(reason.to_owned());
        self.outgoing.close(ending.clone());
        self.incoming.close(ending);
    }
}

struct WsSender(Arc<Shared>);
impl Sender for WsSender {
    fn send(&mut self, message: &[u8]) -> Result<(), ChannelError> {
        if message.len() > self.0.max_message {
            let reason = format!(
                "message of {} bytes exceeds the limit of {}",
                message.len(),
                self.0.max_message
            );
            self.0.close(TOO_BIG, &reason);
            return Err(ChannelError::Closed { reason });
        }
        self.0.outgoing.push_blocking(message)
    }
}

struct WsReceiver(Arc<Shared>);
impl Receiver for WsReceiver {
    fn recv(&mut self) -> Result<Option<Vec<u8>>, ChannelError> {
        self.0.incoming.pop_blocking()
    }
}

struct WsCloser(Arc<Shared>);
impl Closer for WsCloser {
    fn close(&self, reason: &str) {
        self.0.close(GOING_AWAY, reason);
    }

    fn replaced(&self) {
        self.0.close(REPLACED, "replaced by a new connection");
    }
}

/// Counts a running connection task for the transport's shutdown.
pub(crate) struct Live(pub Arc<crate::transport::websocket::Shared>);
impl Live {
    pub(crate) fn new(transport: Arc<crate::transport::websocket::Shared>) -> Self {
        transport
            .live
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(transport)
    }
}
impl Drop for Live {
    fn drop(&mut self) {
        self.0
            .live
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.0.ended.notify_waiters();
    }
}

/// Run `socket` as a channel on `runtime`.
pub(crate) fn channel<S: Io>(
    socket: WebSocketStream<S>,
    info: ChannelInfo,
    limits: &Limits,
    runtime: &tokio::runtime::Handle,
    live: Live,
) -> Channel {
    let shared = Arc::new(Shared {
        outgoing: Pipe::new(limits.buffer),
        incoming: Pipe::new(limits.buffer),
        control: Control::default(),
        max_message: limits.max_message,
    });
    let task = run(socket, shared.clone(), limits.ping, limits.timeout);
    runtime.spawn(async move {
        task.await;
        drop(live);
    });
    Channel {
        sender: Box::new(WsSender(shared.clone())),
        receiver: Box::new(WsReceiver(shared.clone())),
        closer: Arc::new(WsCloser(shared)),
        info,
    }
}

fn truncate(reason: &str) -> String {
    let mut end = reason.len().min(REASON_LIMIT);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_owned()
}

fn close_frame(code: u16, reason: &str) -> Message {
    Message::Close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: truncate(reason).into(),
    }))
}

/// How the far end's close reads to the receiving side.
fn far_close(frame: Option<CloseFrame>) -> Ending {
    match frame {
        None => Ending::Finished,
        Some(frame) => match u16::from(frame.code) {
            1000 | GOING_AWAY => Ending::Finished,
            REPLACED => Ending::Failed("replaced by a new connection".into()),
            code => Ending::Failed(format!("closed by the far end ({code}): {}", frame.reason)),
        },
    }
}

async fn run<S: Io>(
    socket: WebSocketStream<S>,
    shared: Arc<Shared>,
    ping: Duration,
    timeout: Duration,
) {
    let (mut sink, mut stream) = socket.split();
    let (activity, mut seen) = tokio::sync::watch::channel(Some(Instant::now()));
    let flush = Notify::new();
    // Socket writes may wait indefinitely, but cannot own the connection's
    // lifetime. Intentional inbound backpressure pauses silence checks;
    // control always remains independently interruptible.
    let (ending, close) = {
        let writer = async {
            let mut ticker = tokio::time::interval_at(Instant::now() + ping, ping);
            loop {
                tokio::select! {
                    message = shared.outgoing.pop() => {
                        let Some(message) = message else { continue };
                        let text = match String::from_utf8(message) {
                            Ok(text) => text,
                            Err(_) => {
                                let reason = "a message is not UTF-8 text";
                                return (Ending::Failed(reason.into()), Some(close_frame(1007, reason)));
                            }
                        };
                        if let Err(error) = sink.send(Message::Text(text.into())).await {
                            return (Ending::Failed(format!("send failed: {error}")), None);
                        }
                    }
                    _ = flush.notified() => {
                        if let Err(error) = sink.flush().await {
                            return (Ending::Failed(format!("send failed: {error}")), None);
                        }
                    }
                    _ = ticker.tick() => {
                        if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                            return (Ending::Failed("connection lost".into()), None);
                        }
                    }
                }
            }
        };
        let reader = async {
            loop {
                let frame = stream.next().await;
                activity.send_replace(Some(Instant::now()));
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        // At most one incoming message waits outside the bounded pipe.
                        // A local consumer, not a silent peer, stopped reads.
                        activity.send_replace(None);
                        shared.incoming.push(text.as_bytes().to_vec()).await;
                        activity.send_replace(Some(Instant::now()));
                    }
                    Some(Ok(Message::Binary(_))) => {
                        let reason = "binary messages are reserved for a binary encoding";
                        return (
                            Ending::Failed(reason.into()),
                            Some(close_frame(1003, reason)),
                        );
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                        // Tungstenite queues automatic pong replies on the shared socket.
                        flush.notify_one();
                    }
                    Some(Ok(Message::Close(frame))) => {
                        return (far_close(frame), None);
                    }
                    Some(Err(WsError::Capacity(error))) => {
                        let reason = format!("received a message over the limit: {error}");
                        let close = close_frame(TOO_BIG, &reason);
                        return (Ending::Failed(reason), Some(close));
                    }
                    Some(Err(error)) => {
                        return (Ending::Failed(format!("connection failed: {error}")), None)
                    }
                    None => return (Ending::Failed("connection lost".into()), None),
                }
            }
        };
        let silence = async {
            loop {
                let last_seen = *seen.borrow_and_update();
                let Some(last_seen) = last_seen else {
                    let _ = seen.changed().await;
                    continue;
                };
                if tokio::time::timeout_at(last_seen + timeout, seen.changed())
                    .await
                    .is_err()
                {
                    return (
                        Ending::Failed(format!(
                            "no message from the far end for {} s: heartbeat timeout",
                            timeout.as_secs_f32()
                        )),
                        None,
                    );
                }
            }
        };
        tokio::select! {
            biased;
            (code, reason) = shared.control.requested() => {
                let close = close_frame(code, &reason);
                (Ending::Failed(reason), Some(close))
            }
            ending = silence => ending,
            ending = reader => ending,
            ending = writer => ending,
        }
    };
    // A writable peer still receives the appropriate close code. A stalled
    // socket must not extend shutdown: poll once, then drop the physical IO.
    if let Some(close) = close {
        let _ = sink.send(close).now_or_never();
    } else {
        let _ = sink.flush().now_or_never();
    }
    shared.outgoing.close(ending.clone());
    shared.incoming.close(ending);
}
