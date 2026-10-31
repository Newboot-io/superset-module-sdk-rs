//! Flow observations at the client's own boundaries, against the MQTT 5 broker stub: the
//! connection, messages from a linked module (`receive`, `process`), readings sent (`enqueue`),
//! and what is never counted (stored messages, commands).
mod support;

use superset_module_sdk::{FlowOperation, ModuleClient, ModuleConfig, Telemetry};
use support::{accept_and_subscribe, publish, wait_until, Pki};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

const COMMAND: &str = "input/directmethod/tenant-a/cluster-a/my-module/unicast/ping";
const INPUT: &str = "output/telemetry/tenant-a/node-a/sensor/v";
const SERIES: usize = 17;

/// The value of one rendered series, by its name and labels.
fn series(client: &ModuleClient, name: &str) -> String {
    let metrics = client.metrics();
    let line = metrics
        .lines()
        .find(|line| {
            line.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with(' '))
        })
        .unwrap_or_else(|| panic!("no {name} in\n{metrics}"));
    line[name.len() + 1..].to_owned()
}

fn count(client: &ModuleClient, operation: &str, outcome: &str) -> u64 {
    let name =
        format!("newboot_flow_operations_total{{operation=\"{operation}\",outcome=\"{outcome}\"}}");
    series(client, &name).parse().unwrap()
}

fn last(client: &ModuleClient, outcome: &str, operation: &str) -> f64 {
    let name =
        format!("newboot_flow_last_{outcome}_timestamp_seconds{{operation=\"{operation}\"}}");
    series(client, &name).parse().unwrap()
}

fn connected(client: &ModuleClient) -> String {
    series(client, "newboot_flow_mqtt_connected")
}

#[tokio::test]
async fn records_the_connection_inputs_and_readings_once_each() {
    let pki = Pki::new();
    let acceptor = pki.acceptor();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut config = pki.config_json(None, &[]);
    config["brokerUrl"] = format!("tcps://localhost:{port}").into();
    let client =
        ModuleClient::new(ModuleConfig::from_slice(config.to_string().as_bytes()).unwrap());
    client.on_telemetry(|input| {
        if input.variable_id == "boom" {
            panic!("handler failure");
        }
    });
    let metrics = client.metrics();
    assert_eq!(
        metrics
            .lines()
            .filter(|line| !line.starts_with('#'))
            .count(),
        SERIES
    );
    assert_eq!(connected(&client), "-1", "not yet observed");

    client.connect().await.unwrap();
    let mut broker = accept_and_subscribe(&acceptor, &listener).await;
    wait_until("ready", || client.is_connected()).await;
    assert_eq!(connected(&client), "1");

    // A valid input, one that breaks the payload rules, a stored one, a command and an input
    // whose handler panics: three are received, one of them processed.
    for (topic, payload, retain) in [
        (INPUT, &br#"{"value":1,"variableId":"v"}"#[..], false),
        (INPUT, b"not json", false),
        (INPUT, br#"{"value":1,"variableId":"v"}"#, true),
        (COMMAND, b"{}", false),
        (INPUT, br#"{"value":1,"variableId":"boom"}"#, false),
    ] {
        broker
            .write_all(&publish(topic, payload, retain))
            .await
            .unwrap();
    }
    // Messages are handled in order: once the last is counted, all of them are.
    wait_until("three inputs received", || {
        count(&client, "process", "error") == 2
    })
    .await;
    assert_eq!(count(&client, "receive", "success"), 3);
    assert_eq!(count(&client, "receive", "error"), 0);
    assert_eq!(count(&client, "process", "success"), 1);
    assert!(last(&client, "success", "receive") > 1e9, "Unix seconds");

    // A reading the connected client takes; one that breaks the payload rules never reaches
    // the client and is not counted.
    assert!(client.send_telemetry(Telemetry::new("v", 1.5)).unwrap());
    assert!(client.send_telemetry(Telemetry::new("a/b", 1.5)).is_err());
    assert_eq!(count(&client, "enqueue", "success"), 1);
    assert_eq!(count(&client, "enqueue", "error"), 0);

    // The connection drops: disconnected, and a reading now is not taken.
    drop(broker);
    drop(listener);
    wait_until("not ready after the drop", || !client.is_connected()).await;
    assert_eq!(connected(&client), "0");
    assert!(!client.send_telemetry(Telemetry::new("v", 1.5)).unwrap());
    assert_eq!(count(&client, "enqueue", "error"), 1);

    client.close().await;
    assert_eq!(connected(&client), "0");
    // The SDK never acquires: the module records it.
    assert_eq!(count(&client, "acquire", "success"), 0);
    assert_eq!(count(&client, "acquire", "error"), 0);
}

#[tokio::test]
async fn unreadable_certificates_are_observed_as_disconnected() {
    let pki = Pki::new();
    let client = ModuleClient::new(pki.config(None, &[]));
    std::fs::remove_file(&pki.ca).unwrap();
    assert_eq!(connected(&client), "-1");
    client.connect().await.unwrap();
    wait_until("disconnected", || connected(&client) == "0").await;
    client.close().await;
    assert_eq!(connected(&client), "0");
}

#[test]
fn a_module_records_acquire_on_the_clients_flow() {
    let pki = Pki::new();
    let client = ModuleClient::new(pki.config(None, &[]));
    client.flow().record(FlowOperation::Acquire, true);
    client.clone().flow().record(FlowOperation::Acquire, false);
    assert_eq!(count(&client, "acquire", "success"), 1);
    assert_eq!(count(&client, "acquire", "error"), 1);
    assert!(last(&client, "error", "acquire") > 1e9);
    let never = r#"newboot_flow_last_success_timestamp_seconds{operation="enqueue"}"#;
    assert_eq!(series(&client, never), "0", "zero before the first");
}
