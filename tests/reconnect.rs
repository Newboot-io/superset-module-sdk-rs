//! Reconnection after a protocol error: a small MQTT 5 broker stub over mutual TLS sends a
//! PUBLISH with an empty topic and an unknown topic alias. The MQTT library answers with a
//! DISCONNECT of its own; the client must then open a new connection and subscribe again.
mod support;

use std::time::{Duration, Instant};
use superset_module_sdk::{ModuleClient, ModuleConfig, RECONNECT_AFTER};
use support::Pki;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const CONNECT: u8 = 0x10;
const CONNACK: &[u8] = &[0x20, 0x03, 0x00, 0x00, 0x00]; // no session, success, no properties
const SUBSCRIBE: u8 = 0x82;
const DISCONNECT: u8 = 0xe0;
/// PUBLISH, QoS 0: an empty topic and the property "topic alias" (0x23) = 5, never defined.
const UNKNOWN_ALIAS: &[u8] = &[0x30, 0x06, 0x00, 0x00, 0x03, 0x23, 0x00, 0x05];
const STEP: Duration = Duration::from_secs(10);

/// One MQTT packet: its first header byte and its body.
async fn read_packet(stream: &mut (impl AsyncRead + Unpin)) -> (u8, Vec<u8>) {
    let kind = stream.read_u8().await.unwrap();
    let (mut length, mut shift) = (0usize, 0);
    loop {
        let byte = stream.read_u8().await.unwrap();
        length |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    (kind, body)
}

/// A variable byte integer and the bytes it took.
fn varint(bytes: &[u8]) -> (usize, usize) {
    let (mut value, mut used) = (0usize, 0);
    for (i, byte) in bytes.iter().enumerate() {
        value |= usize::from(byte & 0x7f) << (7 * i);
        used = i + 1;
        if byte & 0x80 == 0 {
            break;
        }
    }
    (value, used)
}

/// The SUBACK granting every filter of a SUBSCRIBE body, at QoS 1.
fn suback(subscribe: &[u8]) -> Vec<u8> {
    let (properties, used) = varint(&subscribe[2..]);
    let mut at = 2 + used + properties;
    let mut filters = 0u8;
    while at < subscribe.len() {
        let length = usize::from(u16::from_be_bytes([subscribe[at], subscribe[at + 1]]));
        at += 2 + length + 1; // the filter, then its options byte
        filters += 1;
    }
    let mut packet = vec![0x90, 3 + filters, subscribe[0], subscribe[1], 0x00];
    packet.extend(std::iter::repeat_n(0x01, usize::from(filters)));
    packet
}

async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let end = Instant::now() + STEP;
    while !condition() {
        assert!(Instant::now() < end, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_protocol_error_reconnects_and_subscribes_again() {
    let pki = Pki::new();
    let acceptor = pki.acceptor();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut config = pki.config_json(None, &[]);
    config["brokerUrl"] = format!("tcps://localhost:{port}").into();
    let client =
        ModuleClient::new(ModuleConfig::from_slice(config.to_string().as_bytes()).unwrap());
    client.connect().await.unwrap();

    // First connection: CONNECT, SUBSCRIBE, then ready.
    let accept = async {
        acceptor
            .accept(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    };
    let mut first = tokio::time::timeout(STEP, accept).await.unwrap();
    assert_eq!(read_packet(&mut first).await.0, CONNECT);
    first.write_all(CONNACK).await.unwrap();
    let (kind, subscribe) = read_packet(&mut first).await;
    assert_eq!(kind, SUBSCRIBE);
    first.write_all(&suback(&subscribe)).await.unwrap();
    wait_until("ready after the first SUBACK", || client.is_connected()).await;

    // The unknown alias: the library raises a DISCONNECT, and the client drops this connection
    // (the stub keeps its end open: the client must not keep reading it).
    first.write_all(UNKNOWN_ALIAS).await.unwrap();
    wait_until("not ready after the protocol error", || {
        !client.is_connected()
    })
    .await;
    let mut rest = Vec::new();
    let ended = tokio::time::timeout(STEP, first.read_to_end(&mut rest)).await;
    assert!(ended.is_ok(), "the client closed the first connection");
    assert!(rest.is_empty() || rest[0] == DISCONNECT, "{rest:?}");

    // A second connection, after the pause: CONNECT again, SUBSCRIBE again, ready again.
    let accept = async {
        acceptor
            .accept(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    };
    let mut second = tokio::time::timeout(RECONNECT_AFTER + STEP, accept)
        .await
        .expect("a second connection");
    assert_eq!(read_packet(&mut second).await.0, CONNECT);
    second.write_all(CONNACK).await.unwrap();
    let (kind, subscribe) = read_packet(&mut second).await;
    assert_eq!(kind, SUBSCRIBE);
    second.write_all(&suback(&subscribe)).await.unwrap();
    wait_until("ready after the second SUBACK", || client.is_connected()).await;

    client.close().await;
}
