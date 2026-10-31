//! Reconnection after a protocol error: a small MQTT 5 broker stub over mutual TLS sends a
//! PUBLISH with an empty topic and an unknown topic alias. The MQTT library answers with a
//! DISCONNECT of its own; the client must then open a new connection and subscribe again.
mod support;

use superset_module_sdk::{ModuleClient, ModuleConfig, RECONNECT_AFTER};
use support::{
    read_packet, suback, wait_until, Pki, CONNACK, CONNECT, DISCONNECT, STEP, SUBSCRIBE,
    UNKNOWN_ALIAS,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
