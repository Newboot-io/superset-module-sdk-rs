//! The public `ModuleClient` API: config loading, twin fetch over mutual TLS, secrets, telemetry
//! and connection life cycle.
mod support;

use serde_json::json;
use superset_module_sdk::{Error, ModuleClient, ModuleConfig, SecretError, Telemetry, TwinError};
use support::{http, Pki, SECRET_VALUE};

fn twin_body() -> Vec<u8> {
    json!({"path": "/site/scope", "modules": {"my-module": {
        "serverport": 4840,
        "variables": {"variable-1": {"modelId": "dtmi:example:Variable;1", "name": "Boiler"}}
    }}})
    .to_string()
    .into_bytes()
}

async fn fetch(
    pki: &Pki,
    response: Vec<u8>,
) -> (Result<superset_module_sdk::ModuleTwin, Error>, String) {
    fetch_with(pki, response, None).await
}

async fn fetch_with(
    pki: &Pki,
    response: Vec<u8>,
    correlation_id: Option<&str>,
) -> (Result<superset_module_sdk::ModuleTwin, Error>, String) {
    let (endpoint, server) = pki.serve_once(response).await;
    let client = ModuleClient::new(pki.config(Some(&endpoint), &[]));
    let twin = client.get_twin(correlation_id).await;
    (twin, server.await.unwrap())
}

#[tokio::test]
async fn get_twin_fetches_this_modules_twin_with_the_client_certificate() {
    let pki = Pki::new();
    let (twin, request) = fetch(&pki, http("200 OK", &twin_body())).await;
    let twin = twin.unwrap();
    assert_eq!(twin.get("serverport"), Some(&json!(4840)));
    assert!(twin.variables().unwrap().contains_key("variable-1"));
    assert_eq!(twin.variable_name("variable-1"), "Boiler");
    assert!(request.starts_with("GET /twin HTTP/1.1\r\n"), "{request}");
    // No reload: no correlation id.
    assert!(!request
        .to_ascii_lowercase()
        .contains("x-newboot-correlation-id"));
}

#[tokio::test]
async fn get_twin_forwards_a_plain_correlation_id_only() {
    let pki = Pki::new();
    let (twin, request) = fetch_with(&pki, http("200 OK", &twin_body()), Some("reload-1")).await;
    twin.unwrap();
    assert!(
        request
            .to_ascii_lowercase()
            .contains("x-newboot-correlation-id: reload-1\r\n"),
        "{request}"
    );
    let (twin, request) = fetch_with(&pki, http("200 OK", &twin_body()), Some("a b")).await;
    twin.unwrap();
    assert!(!request
        .to_ascii_lowercase()
        .contains("x-newboot-correlation-id"));
}

#[tokio::test]
async fn get_twin_requests_the_endpoint_path_and_query_as_given() {
    let pki = Pki::new();
    let (endpoint, server) = pki.serve_once(http("200 OK", &twin_body())).await;
    let client = ModuleClient::new(pki.config(Some(&format!("{endpoint};p=1?scope=a&v=1")), &[]));
    client.get_twin(None).await.unwrap();
    let request = server.await.unwrap();
    assert!(
        request.starts_with("GET /twin;p=1?scope=a&v=1 HTTP/1.1\r\n"),
        "{request}"
    );
}

#[tokio::test]
async fn corrupt_certificates_are_twin_errors() {
    for corrupt in ["identity", "ca"] {
        let pki = Pki::new();
        let path = if corrupt == "identity" {
            &pki.identity
        } else {
            &pki.ca
        };
        std::fs::write(
            path,
            "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        let client = ModuleClient::new(pki.config(Some("https://localhost:1/twin"), &[]));
        let error = client.get_twin(None).await.unwrap_err();
        assert!(
            matches!(error, Error::Twin(TwinError::Failed(_))),
            "{corrupt}: {error}"
        );
    }
}

#[tokio::test]
async fn get_twin_refuses_an_error_status() {
    let pki = Pki::new();
    let (twin, _) = fetch(&pki, http("500 Internal Server Error", b"")).await;
    let error = twin.unwrap_err();
    assert!(matches!(error, Error::Twin(_)));
    assert!(error.to_string().contains("HTTP 500"), "{error}");
}

#[tokio::test]
async fn get_twin_does_not_follow_redirects() {
    let pki = Pki::new();
    let mut answer = b"HTTP/1.1 302 Found\r\nlocation: /twin\r\n".to_vec();
    answer.extend_from_slice(&http("200 OK", &twin_body())[b"HTTP/1.1 200 OK\r\n".len()..]);
    let (twin, _) = fetch(&pki, answer).await;
    assert!(twin.unwrap_err().to_string().contains("HTTP 302"));
}

#[tokio::test]
async fn get_twin_refuses_another_modules_twin() {
    let pki = Pki::new();
    let body = json!({"modules": {"other": {}}}).to_string();
    let (twin, _) = fetch(&pki, http("200 OK", body.as_bytes())).await;
    assert!(matches!(twin, Err(Error::Twin(_))));
    let body = json!({"modules": {"my-module": {}, "other": {}}}).to_string();
    let (twin, _) = fetch(&pki, http("200 OK", body.as_bytes())).await;
    assert!(matches!(twin, Err(Error::Twin(_))));
}

#[tokio::test]
async fn get_twin_stops_reading_past_2_mib() {
    let pki = Pki::new();
    let body = vec![b' '; 2 * 1024 * 1024 + 1];
    let (twin, _) = fetch(&pki, http("200 OK", &body)).await;
    assert!(twin.unwrap_err().to_string().contains("too large"));
}

#[tokio::test]
async fn get_twin_refuses_an_answer_shorter_than_announced() {
    let pki = Pki::new();
    let mut answer = http("200 OK", &twin_body());
    answer.truncate(answer.len() - 5);
    let (twin, _) = fetch(&pki, answer).await;
    assert!(matches!(twin, Err(Error::Twin(_))));
}

#[tokio::test]
async fn get_twin_needs_a_trusted_server_and_an_accepted_client() {
    let pki = Pki::new();
    pki.use_foreign_identity();
    let (twin, request) = fetch(&pki, http("200 OK", &twin_body())).await;
    assert!(twin.is_err());
    assert!(request.is_empty(), "the server must refuse the handshake");

    let pki = Pki::new();
    pki.trust_another_ca();
    let (twin, request) = fetch(&pki, http("200 OK", &twin_body())).await;
    assert!(twin.is_err());
    assert!(request.is_empty());
}

#[tokio::test]
async fn get_twin_rereads_the_certificates_on_every_call() {
    let pki = Pki::new();
    let (endpoint, server) = pki.serve_once(http("200 OK", &twin_body())).await;
    let client = ModuleClient::new(pki.config(Some(&endpoint), &[]));
    std::fs::remove_file(&pki.identity).unwrap();
    let error = client.get_twin(None).await.unwrap_err();
    assert!(
        matches!(error, Error::Twin(TwinError::Failed(_))),
        "{error}"
    );
    assert!(error.to_string().contains("client certificate"), "{error}");
    server.abort();
}

#[tokio::test]
async fn get_twin_without_a_twin_is_a_typed_error() {
    let pki = Pki::new();
    let client = ModuleClient::new(pki.config(None, &[]));
    let error = client.get_twin(None).await.unwrap_err();
    assert!(matches!(error, Error::Twin(TwinError::NotConfigured)));
    assert_eq!(error.to_string(), "this module has no module twin");
}

#[test]
fn get_secret_and_check_secrets_never_show_the_value() {
    let pki = Pki::new();
    let good = pki.secret("device-password", &format!("\"{SECRET_VALUE}\""));
    let client = ModuleClient::new(pki.config(None, std::slice::from_ref(&good)));
    assert_eq!(client.get_secret("device-password").unwrap(), SECRET_VALUE);
    client.check_secrets().unwrap();
    let error = client.get_secret("api-key").unwrap_err();
    assert!(matches!(
        error,
        Error::Secret {
            reason: SecretError::NotDeclared,
            ..
        }
    ));

    for contents in [
        format!("{{\"value\": \"{SECRET_VALUE}\"}}"),
        SECRET_VALUE.to_string(),
        format!("[\"{SECRET_VALUE}\""),
    ] {
        pki.secret("device-password", &contents);
        let error = client.get_secret("device-password").unwrap_err();
        for shown in [error.to_string(), format!("{error:?}")] {
            assert!(!shown.contains(SECRET_VALUE), "{shown}");
            assert!(shown.contains("device-password"), "{shown}");
        }
        assert!(client.check_secrets().is_err());
    }
    std::fs::remove_file(good).unwrap();
    let error = client.check_secrets().unwrap_err();
    assert_eq!(
        error.to_string(),
        "secret device-password is missing or unreadable"
    );
}

#[test]
fn from_env_reads_the_file_module_config_names() {
    let pki = Pki::new();
    let path = pki.dir.path().join("module.json");
    std::fs::write(&path, pki.config_json(None, &[]).to_string()).unwrap();
    // The only test touching MODULE_CONFIG: no other test reads it concurrently.
    std::env::set_var("MODULE_CONFIG", &path);
    let client = ModuleClient::from_env().unwrap();
    assert_eq!(client.target().module_id, "my-module");
    assert_eq!(client.config().broker_address().unwrap(), ("127.0.0.1", 1));
    std::env::set_var("MODULE_CONFIG", pki.dir.path().join("missing.json"));
    let error = ModuleClient::from_env().err().unwrap();
    assert!(matches!(error, Error::Config(_)));
    assert!(ModuleConfig::from_env().is_err());
}

#[tokio::test]
async fn send_telemetry_is_false_before_connect_and_errs_on_invalid_input() {
    let pki = Pki::new();
    let client = ModuleClient::new(pki.config(None, &[]));
    assert!(!client
        .send_telemetry(Telemetry::new("variable-1", 1.5))
        .unwrap());
    let error = client
        .send_telemetry(Telemetry::new("a/b", 1.5))
        .unwrap_err();
    assert!(matches!(error, Error::Telemetry(_)));
    assert!(error.to_string().contains("variableId"), "{error}");
    let error = client
        .send_telemetry(Telemetry::new("variable-1", json!({"nested": 1})))
        .unwrap_err();
    assert!(error.to_string().contains("value"), "{error}");
}

#[tokio::test]
async fn connect_never_fails_on_missing_certificate_files() {
    let pki = Pki::new();
    let client = ModuleClient::new(pki.config(None, &[]));
    std::fs::remove_file(&pki.ca).unwrap();
    // It starts, logs and retries; readiness tells the module it is not connected.
    client.connect().await.unwrap();
    assert!(!client.is_connected());
    let started = std::time::Instant::now();
    client.close().await;
    // close() wakes the task from its pause instead of waiting it out.
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn connect_starts_once_and_close_is_final() {
    let pki = Pki::new();
    let client = ModuleClient::new(pki.config(None, &[]));
    client.on_telemetry(|_| {});
    client.on_twin_change(|_| {});
    client.connect().await.unwrap();
    // The broker address is unreachable: never connected, so nothing is queued.
    assert!(!client.is_connected());
    assert!(!client
        .send_telemetry(Telemetry::new("variable-1", 1.5))
        .unwrap());
    // A second connect is a no-op.
    client.connect().await.unwrap();
    client.close().await;
    assert!(!client.is_connected());
    assert!(!client
        .send_telemetry(Telemetry::new("variable-1", 1.5))
        .unwrap());
    // Closed: connecting again is an error.
    let error = client.connect().await.unwrap_err();
    assert!(matches!(error, Error::Closed), "{error}");
    client.close().await;
}

#[test]
fn secret_names_lists_the_declared_secrets() {
    let pki = Pki::new();
    let files = [
        pki.secret("device-password", "\"x\""),
        pki.secret("api-key", "\"y\""),
    ];
    let client = ModuleClient::new(pki.config(None, &files));
    assert_eq!(client.secret_names(), ["device-password", "api-key"]);
}
