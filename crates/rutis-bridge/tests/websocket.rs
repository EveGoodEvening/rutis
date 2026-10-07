//! The WebSocket transport against the channel contract and its binding:
//! TLS, authentication, routing, subprotocols, limits, heartbeats, close
//! codes and unloading.
#![cfg(all(feature = "websocket", feature = "testing"))]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use rutis::Ctx;
use rutis_bridge::channel::{Channel, ConnectError, PeerId};
use rutis_bridge::transport::websocket::{
    Config, Limits, ListenerConfig, ServerTls, Trust, WebSocketPlugin, WebSocketTransport,
};
use rutis_bridge::{
    fingerprint, transport_key, Credential, Dial, Identity, Registered, Registration,
    StaticIdentity, Transport,
};

const PROTOCOL: &str = "rutis.2";

fn id(s: &str) -> PeerId {
    PeerId::new(s).unwrap()
}

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// A transport whose loopback listener `public` serves endpoint `main`.
fn server(limits: Limits) -> Arc<WebSocketTransport> {
    WebSocketTransport::start(
        Config::new()
            .listener(ListenerConfig::new("public", loopback(), id("main")))
            .limits(limits),
    )
    .unwrap()
}

/// A dialing transport that trusts only `ca` (or the system when `None`).
fn client(limits: Limits) -> Arc<WebSocketTransport> {
    WebSocketTransport::start(Config::new().limits(limits)).unwrap()
}

/// Accept `peer` with `token` on `public`; accepted channels arrive on the
/// returned receiver.
fn accept(
    transport: &WebSocketTransport,
    peer: &str,
    token: &str,
) -> (Registered, mpsc::Receiver<Channel>) {
    let (sender, accepted) = mpsc::channel();
    let sender = Mutex::new(sender);
    let identity = StaticIdentity::new(id("main")).accept_token(token, id(peer));
    let registered = transport
        .register(Registration {
            listener: "public".into(),
            peer: id(peer),
            identity: Arc::new(identity),
            protocol: PROTOCOL.into(),
            deliver: Box::new(move |channel| {
                let _ = sender.lock().unwrap().send(channel);
            }),
        })
        .unwrap();
    (registered, accepted)
}

/// `peer` dialing endpoint `main` with `token`.
fn dial_as(address: String, peer: &str, token: &str) -> Dial {
    let identity: Arc<dyn Identity> = Arc::new(
        StaticIdentity::new(id(peer)).present(id("main"), Credential::Bearer(token.into())),
    );
    Dial::address(address)
        .peer(id("main"))
        .identity(identity)
        .protocol(PROTOCOL)
}

fn address(transport: &WebSocketTransport) -> String {
    format!("ws://{}/rutis", transport.local_addr("public").unwrap())
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

#[test]
fn meets_the_channel_contract() {
    let server = server(Limits::default());
    let client = client(Limits::default());
    let count = AtomicUsize::new(0);
    let kept = Mutex::new(Vec::new());
    rutis_bridge::channel::testing::contract(|| {
        let peer = format!("peer-{}", count.fetch_add(1, Ordering::SeqCst));
        let (registered, accepted) = accept(&server, &peer, &peer);
        let dialed = block_on(client.dial(&dial_as(address(&server), &peer, &peer))).unwrap();
        let accepted = accepted.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(dialed.info.peer, Some(id("main")));
        assert_eq!(accepted.info.peer, Some(id(&peer)));
        kept.lock().unwrap().push(registered);
        (dialed, accepted)
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn authentication_routing_and_compatibility_failures_carry_their_category() {
    let server = server(Limits::default());
    let client = client(Limits::default());
    let (_mac, _) = accept(&server, "mac", "mac-token");

    let wrong_token = client
        .dial(&dial_as(address(&server), "mac", "guess"))
        .await;
    assert!(
        matches!(wrong_token, Err(ConnectError::AuthRejected { .. })),
        "{wrong_token:?}"
    );
    let no_credential = client
        .dial(&Dial::address(address(&server)).protocol(PROTOCOL))
        .await;
    assert!(matches!(
        no_credential,
        Err(ConnectError::AuthRejected { .. })
    ));
    // A valid token of a far end nobody registered is refused the same way.
    let (pi, _) = accept(&server, "pi", "pi-token");
    drop(pi);
    let revoked = client
        .dial(&dial_as(address(&server), "pi", "pi-token"))
        .await;
    assert!(matches!(revoked, Err(ConnectError::AuthRejected { .. })));

    let other_protocol = client
        .dial(&dial_as(address(&server), "mac", "mac-token").protocol("rutis.99"))
        .await;
    assert!(
        matches!(other_protocol, Err(ConnectError::Incompatible { .. })),
        "{other_protocol:?}"
    );
    let wrong_path = client
        .dial(&Dial {
            address: address(&server).replace("/rutis", "/other"),
            ..dial_as(String::new(), "mac", "mac-token")
        })
        .await;
    assert!(
        matches!(wrong_path, Err(ConnectError::Incompatible { .. })),
        "{wrong_path:?}"
    );
    let plain_remote = client
        .dial(&dial_as(
            "ws://example.com/rutis".into(),
            "mac",
            "mac-token",
        ))
        .await;
    assert!(matches!(
        plain_remote,
        Err(ConnectError::Incompatible { .. })
    ));
    let free = std::net::TcpListener::bind(loopback())
        .unwrap()
        .local_addr()
        .unwrap();
    let nobody = client
        .dial(&dial_as(format!("ws://{free}/rutis"), "mac", "mac-token"))
        .await;
    assert!(
        matches!(nobody, Err(ConnectError::Retryable { .. })),
        "{nobody:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_registration_per_far_end_and_unknown_listeners_are_refused() {
    let server = server(Limits::default());
    let (_mac, _) = accept(&server, "mac", "a");
    let identity: Arc<dyn Identity> = Arc::new(StaticIdentity::new(id("main")));
    let duplicate = server.register(Registration {
        listener: "public".into(),
        peer: id("mac"),
        identity: identity.clone(),
        protocol: PROTOCOL.into(),
        deliver: Box::new(|_| {}),
    });
    assert!(matches!(
        duplicate,
        Err(rutis_bridge::RegistrationError::Duplicate(_))
    ));
    let nowhere = server.register(Registration {
        listener: "private".into(),
        peer: id("pi"),
        identity,
        protocol: PROTOCOL.into(),
        deliver: Box::new(|_| {}),
    });
    assert!(matches!(
        nowhere,
        Err(rutis_bridge::RegistrationError::NoListener(_))
    ));
}

/// A CA, a server certificate for `localhost` and a client certificate.
struct Pki {
    ca_pem: String,
    server: ServerTls,
    client_chain: String,
    client_key: String,
    client_der: Vec<u8>,
}

fn pki() -> Pki {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    // Distinct names: OpenSSL takes a certificate whose issuer is its own
    // subject for self-signed.
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "rutis test CA");
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);
    let server_key = KeyPair::generate().unwrap();
    let server = CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .signed_by(&server_key, &issuer)
        .unwrap();
    let client_key = KeyPair::generate().unwrap();
    let client = CertificateParams::new(vec!["mac".into()])
        .unwrap()
        .signed_by(&client_key, &issuer)
        .unwrap();
    Pki {
        ca_pem: ca.pem(),
        server: ServerTls {
            certificate_pem: server.pem().into_bytes(),
            key_pem: server_key.serialize_pem().into_bytes(),
            client_ca_pem: Some(ca.pem().into_bytes()),
        },
        client_chain: client.pem(),
        client_key: client_key.serialize_pem(),
        client_der: client.der().to_vec(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn wss_verifies_the_server_and_accepts_client_certificates() {
    let pki = pki();
    let server =
        WebSocketTransport::start(Config::new().listener(
            ListenerConfig::new("public", loopback(), id("main")).tls(pki.server.clone()),
        ))
        .unwrap();
    let address = format!(
        "wss://localhost:{}/rutis",
        server.local_addr("public").unwrap().port()
    );

    // A bearer token over TLS, the server verified against our CA.
    let (_mac, accepted) = accept(&server, "mac", "mac-token");
    let trusting = WebSocketTransport::start(
        Config::new().trust(Trust::only(pki.ca_pem.clone().into_bytes())),
    )
    .unwrap();
    let mut dialed = trusting
        .dial(&dial_as(address.clone(), "mac", "mac-token"))
        .await
        .unwrap();
    let mut accepted = accepted.recv_timeout(Duration::from_secs(5)).unwrap();
    dialed.sender.send(b"{\"over\":\"tls\"}").unwrap();
    assert_eq!(
        accepted.receiver.recv().unwrap().unwrap(),
        b"{\"over\":\"tls\"}"
    );

    // A server certificate from an unknown CA is an authentication failure.
    let system = WebSocketTransport::start(Config::new().trust(Trust {
        system: true,
        ca_pem: Vec::new(),
    }))
    .unwrap();
    let untrusted = system
        .dial(&dial_as(address.clone(), "mac", "mac-token"))
        .await;
    assert!(
        matches!(untrusted, Err(ConnectError::AuthRejected { .. })),
        "{untrusted:?}"
    );

    // A client certificate proves the far end without a token.
    let (sender, accepted) = mpsc::channel();
    let sender = Mutex::new(sender);
    let _pi = server
        .register(Registration {
            listener: "public".into(),
            peer: id("pi"),
            identity: Arc::new(
                StaticIdentity::new(id("main"))
                    .accept_certificate(fingerprint(&pki.client_der), id("pi")),
            ),
            protocol: PROTOCOL.into(),
            deliver: Box::new(move |channel| {
                let _ = sender.lock().unwrap().send(channel);
            }),
        })
        .unwrap();
    let identity: Arc<dyn Identity> = Arc::new(StaticIdentity::new(id("pi")).present(
        id("main"),
        Credential::ClientCertificate {
            chain_pem: pki.client_chain.clone().into_bytes(),
            key_pem: pki.client_key.clone().into_bytes(),
        },
    ));
    let _dialed = trusting
        .dial(
            &Dial::address(address)
                .peer(id("main"))
                .identity(identity)
                .protocol(PROTOCOL),
        )
        .await
        .unwrap();
    let accepted = accepted.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(accepted.info.peer, Some(id("pi")));
}

#[test]
fn listeners_off_loopback_need_tls() {
    let open = Config::new().listener(ListenerConfig::new(
        "public",
        "0.0.0.0:0".parse().unwrap(),
        id("main"),
    ));
    assert!(open.validate().unwrap_err().contains("without TLS"));
}

/// A pair of channels over a real connection between two transports.
fn connected(
    server: &WebSocketTransport,
    client: &WebSocketTransport,
) -> (Channel, Channel, Registered) {
    let (registered, accepted) = accept(server, "mac", "mac-token");
    let dialed = block_on(client.dial(&dial_as(address(server), "mac", "mac-token"))).unwrap();
    let accepted = accepted.recv_timeout(Duration::from_secs(5)).unwrap();
    (dialed, accepted, registered)
}

#[test]
fn messages_over_the_limit_close_the_channel_either_way() {
    let small = Limits {
        max_message: 1024,
        ..Limits::default()
    };
    // Sending too much: refused, and the channel closes (1009).
    let big_server = server(Limits::default());
    let small_client = client(small.clone());
    let (mut dialed, mut accepted, _r) = connected(&big_server, &small_client);
    assert!(dialed.sender.send(&[b'x'; 2048]).is_err());
    let ended = accepted.receiver.recv();
    assert!(
        matches!(&ended, Err(rutis_bridge::channel::ChannelError::Closed { reason }) if reason.contains("1009")),
        "{ended:?}"
    );

    // Receiving too much: the receiving side closes with 1009.
    let small_server = server_with(small);
    let big_client = client(Limits::default());
    let (mut dialed, mut accepted, _r) = connected(&small_server, &big_client);
    dialed.sender.send(&[b'y'; 4096]).unwrap();
    assert!(accepted.receiver.recv().is_err());
    let ended = dialed.receiver.recv();
    assert!(
        matches!(&ended, Err(rutis_bridge::channel::ChannelError::Closed { reason }) if reason.contains("1009")),
        "{ended:?}"
    );
}

fn server_with(limits: Limits) -> Arc<WebSocketTransport> {
    server(limits)
}

#[test]
fn a_takeover_closes_the_old_connection_with_4002() {
    let server = server(Limits::default());
    let client = client(Limits::default());
    let (mut dialed, accepted, _r) = connected(&server, &client);
    accepted.closer.replaced();
    let ended = dialed.receiver.recv();
    assert!(
        matches!(&ended, Err(rutis_bridge::channel::ChannelError::Closed { reason }) if reason == "replaced by a new connection"),
        "{ended:?}"
    );
}

#[test]
fn an_orderly_close_reads_as_a_normal_end() {
    let server = server(Limits::default());
    let client = client(Limits::default());
    let (mut dialed, accepted, _r) = connected(&server, &client);
    accepted.closer.close("bye");
    assert!(matches!(dialed.receiver.recv(), Ok(None)));
}

/// A TCP relay that can stop carrying bytes without closing anything: a
/// half-open connection, as a vanished network leaves it.
fn freezable_relay(target: SocketAddr) -> (SocketAddr, Arc<AtomicBool>) {
    let listener = std::net::TcpListener::bind(loopback()).unwrap();
    let address = listener.local_addr().unwrap();
    let frozen = Arc::new(AtomicBool::new(false));
    let freeze = frozen.clone();
    std::thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { return };
            let upstream = std::net::TcpStream::connect(target).unwrap();
            for (mut from, mut to) in [
                (client.try_clone().unwrap(), upstream.try_clone().unwrap()),
                (upstream, client),
            ] {
                let frozen = freeze.clone();
                std::thread::spawn(move || {
                    use std::io::{Read, Write};
                    let mut buffer = [0u8; 16 * 1024];
                    loop {
                        let Ok(n) = from.read(&mut buffer) else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        if frozen.load(Ordering::SeqCst) {
                            // Swallow: nothing more reaches the far side.
                            continue;
                        }
                        if to.write_all(&buffer[..n]).is_err() {
                            return;
                        }
                    }
                });
            }
        }
    });
    (address, frozen)
}

#[test]
fn heartbeats_find_a_half_open_connection() {
    let quick = Limits {
        ping: Duration::from_millis(100),
        timeout: Duration::from_millis(400),
        ..Limits::default()
    };
    let server = server(quick.clone());
    let client = client(quick);
    let (relay, frozen) = freezable_relay(server.local_addr("public").unwrap());
    let (_r, accepted) = accept(&server, "mac", "mac-token");
    let mut dialed =
        block_on(client.dial(&dial_as(format!("ws://{relay}/rutis"), "mac", "mac-token"))).unwrap();
    let mut accepted = accepted.recv_timeout(Duration::from_secs(5)).unwrap();
    // Alive while pings flow, longer than the timeout.
    std::thread::sleep(Duration::from_millis(800));
    dialed.sender.send(b"still here").unwrap();
    assert_eq!(accepted.receiver.recv().unwrap().unwrap(), b"still here");

    frozen.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    for channel in [&mut dialed, &mut accepted] {
        let ended = channel.receiver.recv();
        assert!(
            matches!(&ended, Err(rutis_bridge::channel::ChannelError::Closed { reason }) if reason.contains("heartbeat timeout")),
            "{ended:?}"
        );
    }
    assert!(started.elapsed() < Duration::from_secs(3));
}

/// Keep the real TCP peer alive but stop reading after the handshake. Its
/// small receive window makes one bounded 8 MiB frame stall the writer.
async fn stalled_writer(explicit_close: bool) {
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    let listener = tokio::net::TcpListener::bind(loopback()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let transport = client(Limits {
        ping: Duration::from_millis(50),
        timeout: if explicit_close {
            Duration::from_secs(30)
        } else {
            Duration::from_millis(500)
        },
        max_message: 8 * 1024 * 1024,
        ..Limits::default()
    });
    let handshake = async {
        let (tcp, _) = listener.accept().await.unwrap();
        socket2::SockRef::from(&tcp)
            .set_recv_buffer_size(4096)
            .unwrap();
        tokio_tungstenite::accept_hdr_async(tcp, |_: &Request, mut response: Response| {
            response
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", PROTOCOL.parse().unwrap());
            Ok(response)
        })
        .await
        .unwrap()
    };
    let dial = dial_as(format!("ws://{addr}/rutis"), "mac", "mac-token");
    let (socket, channel) = tokio::join!(handshake, transport.dial(&dial));
    let mut channel = channel.unwrap();
    let closer = channel.closer.clone();
    channel.sender.send(&vec![b'x'; 8 * 1024 * 1024]).unwrap();
    let (result_tx, result_rx) = mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let _ = result_tx.send(channel.receiver.recv());
    });
    // RAII ends the blocked receiver and socket even if an assertion fails.
    struct Cleanup {
        closer: Arc<dyn rutis_bridge::channel::Closer>,
        receiver: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.closer.close("test cleanup");
            if let Some(receiver) = self.receiver.take() {
                receiver.join().unwrap();
            }
        }
    }
    let _cleanup = Cleanup {
        closer: closer.clone(),
        receiver: Some(receiver),
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        result_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    let started = std::time::Instant::now();
    if explicit_close {
        closer.close("explicit close under backpressure");
    }
    let ended = result_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let expected = if explicit_close {
        "explicit close under backpressure"
    } else {
        "heartbeat timeout"
    };
    assert!(
        matches!(&ended, Err(rutis_bridge::channel::ChannelError::Closed { reason }) if reason.contains(expected)),
        "{ended:?}"
    );
    // Pipe closure alone is insufficient: the physical connection task must
    // finish rather than spend the entire wait limit in a blocked send/close.
    transport.closed(Duration::from_secs(2)).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(socket);
}

#[tokio::test]
async fn heartbeat_deadline_survives_socket_write_backpressure() {
    stalled_writer(false).await;
}

#[tokio::test]
async fn explicit_close_interrupts_socket_write_backpressure() {
    stalled_writer(true).await;
}

#[test]
fn local_receiver_backpressure_pauses_silence_but_not_explicit_close() {
    let limits = Limits {
        buffer: 1,
        ping: Duration::from_millis(100),
        timeout: Duration::from_millis(400),
        ..Limits::default()
    };
    for close_while_paused in [false, true] {
        let server = server(limits.clone());
        let client = client(limits.clone());
        let (mut dialed, mut accepted, _registered) = connected(&server, &client);
        dialed.sender.send(b"a").unwrap();
        dialed.sender.send(b"b").unwrap();
        // The first byte fills the pipe; the second blocks the reader.
        // Allow several complete silence periods, not a deadline-edge race.
        std::thread::sleep(Duration::from_millis(1600));
        accepted
            .sender
            .send(b"alive")
            .expect("local backpressure is not peer silence");
        let closer = accepted.closer.clone();
        if close_while_paused {
            let started = std::time::Instant::now();
            closer.close("close while inbound paused");
            block_on(server.closed(Duration::from_secs(2)));
            assert!(started.elapsed() < Duration::from_secs(1));
        } else {
            let (tx, rx) = mpsc::channel();
            let reader = std::thread::spawn(move || {
                let first = accepted.receiver.recv();
                let second = accepted.receiver.recv();
                let _ = tx.send((first, second));
            });
            struct Cleanup {
                closer: Arc<dyn rutis_bridge::channel::Closer>,
                reader: Option<std::thread::JoinHandle<()>>,
            }
            impl Drop for Cleanup {
                fn drop(&mut self) {
                    self.closer.close("test cleanup");
                    self.reader.take().unwrap().join().unwrap();
                }
            }
            let _cleanup = Cleanup {
                closer,
                reader: Some(reader),
            };
            let (first, second) = rx.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(first.unwrap().unwrap(), b"a");
            assert_eq!(second.unwrap().unwrap(), b"b");
            // Draining capacity resumes socket reads and healthy heartbeats.
            std::thread::sleep(Duration::from_millis(800));
            accepted.sender.send(b"resumed").unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_plugin_provides_the_transport_and_unloading_closes_everything() {
    let plugin = WebSocketPlugin::new(Config::new().listener(ListenerConfig::new(
        "public",
        loopback(),
        id("main"),
    )))
    .unwrap();
    let handle = plugin.clone();
    let root = Ctx::root().unwrap();
    let view = root.plugin(plugin);
    (&view).await.unwrap();
    let transport = root
        .get_as::<dyn Transport>(transport_key("websocket"))
        .expect("Transport#websocket is provided");
    assert_eq!(transport.kind(), "websocket");
    let running = handle.transport().unwrap();
    let address = address(&running);
    let (_registered, accepted) = accept(&running, "mac", "mac-token");
    let client = client(Limits::default());
    let mut dialed = client
        .dial(&dial_as(address.clone(), "mac", "mac-token"))
        .await
        .unwrap();
    let mut accepted = accepted.recv_timeout(Duration::from_secs(5)).unwrap();
    drop((transport, running));

    view.dispose().await.unwrap();
    assert!(root
        .get_as::<dyn Transport>(transport_key("websocket"))
        .is_none());
    assert!(handle.transport().is_none());
    // Its channels closed, and the far end saw it.
    assert!(!matches!(accepted.receiver.recv(), Ok(Some(_))));
    assert!(tokio::task::spawn_blocking(move || dialed.receiver.recv())
        .await
        .unwrap()
        .is_ok_and(|end| end.is_none()));
    // Nothing listens any more.
    let refused = client.dial(&dial_as(address, "mac", "mac-token")).await;
    assert!(
        matches!(refused, Err(ConnectError::Retryable { .. })),
        "{refused:?}"
    );
}
