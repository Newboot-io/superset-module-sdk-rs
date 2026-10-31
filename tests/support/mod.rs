//! Shared test fixtures: a throwaway PKI, module configs on disk, a one-shot HTTPS server
//! that requires a client certificate, and the pieces of a small MQTT 5 broker stub.
#![allow(dead_code)]

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use superset_module_sdk::ModuleConfig;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

pub const SECRET_VALUE: &str = "e2e-unit-s3cret";

/// A CA, a server certificate for `localhost`, and a client identity file, all on disk.
pub struct Pki {
    pub dir: tempfile::TempDir,
    pub ca: PathBuf,
    pub identity: PathBuf,
    server_cert: CertificateDer<'static>,
    server_key: Vec<u8>,
    ca_cert: CertificateDer<'static>,
}

fn ca() -> (rcgen::Certificate, KeyPair) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    (params.self_signed(&key).unwrap(), key)
}

impl Pki {
    pub fn new() -> Self {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        let dir = tempfile::tempdir().unwrap();
        let (ca_cert, ca_key) = ca();

        let server_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_cert = params.signed_by(&server_key, &ca_cert, &ca_key).unwrap();

        let client_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_cert = params.signed_by(&client_key, &ca_cert, &ca_key).unwrap();

        let ca = dir.path().join("ca.crt");
        std::fs::write(&ca, ca_cert.pem()).unwrap();
        // One PEM per identity: certificate, chain, then key, like the files a Superset mounts.
        let identity = dir.path().join("client.pem");
        std::fs::write(
            &identity,
            format!(
                "{}{}{}",
                client_cert.pem(),
                ca_cert.pem(),
                client_key.serialize_pem()
            ),
        )
        .unwrap();
        Self {
            ca,
            ca_cert: ca_cert.der().clone(),
            dir,
            identity,
            server_cert: server_cert.der().clone(),
            server_key: server_key.serialize_der(),
        }
    }

    /// Replaces the client identity with one from another CA, which the server refuses.
    pub fn use_foreign_identity(&self) {
        let (other_ca, other_key) = ca();
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(Vec::<String>::new())
            .unwrap()
            .signed_by(&key, &other_ca, &other_key)
            .unwrap();
        std::fs::write(
            &self.identity,
            format!("{}{}", cert.pem(), key.serialize_pem()),
        )
        .unwrap();
    }

    /// Replaces the trusted CA with an unrelated one: the server is no longer trusted.
    pub fn trust_another_ca(&self) {
        std::fs::write(&self.ca, ca().0.pem()).unwrap();
    }

    /// Writes a secret file and returns its path.
    pub fn secret(&self, name: &str, contents: &str) -> String {
        let path = self.dir.path().join(format!("{name}.json"));
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().into()
    }

    /// A module config pointing at this PKI. `twin_endpoint`: `None` for a module without twin.
    pub fn config_json(&self, twin_endpoint: Option<&str>, secret_files: &[String]) -> Value {
        let mut config = json!({
            "brokerUrl": "tcps://127.0.0.1:1",
            "configuration": {"inputs": ["output/telemetry/tenant-a/node-a/sensor/v"],
                "secretFiles": secret_files},
            "mqtt": {"ca": self.ca, "identity": self.identity, "mode": "mutual-tls"},
            "target": {"clusterId": "cluster-a", "moduleId": "my-module", "nodeId": "node-a",
                "tenantConfigId": "tenant-a"}
        });
        if let Some(endpoint) = twin_endpoint {
            config["twin"] =
                json!({"ca": self.ca, "endpoint": endpoint, "identity": self.identity});
        }
        config
    }

    pub fn config(&self, twin_endpoint: Option<&str>, secret_files: &[String]) -> ModuleConfig {
        ModuleConfig::from_slice(
            self.config_json(twin_endpoint, secret_files)
                .to_string()
                .as_bytes(),
        )
        .unwrap()
    }

    /// Serves one HTTPS request on `localhost` with `response` (raw HTTP), requiring a client
    /// certificate from this CA. Returns the endpoint and the request the server received
    /// (empty when the handshake failed).
    pub async fn serve_once(&self, response: Vec<u8>) -> (String, tokio::task::JoinHandle<String>) {
        let acceptor = self.acceptor();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let Ok(mut tls) = acceptor.accept(tcp).await else {
                return String::new();
            };
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                match tls.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => request.extend_from_slice(&buffer[..n]),
                }
            }
            let _ = tls.write_all(&response).await;
            let _ = tls.shutdown().await;
            String::from_utf8_lossy(&request).into_owned()
        });
        (format!("https://localhost:{port}/twin"), server)
    }

    /// A TLS acceptor for `localhost` that requires a client certificate from this CA.
    pub fn acceptor(&self) -> TlsAcceptor {
        let mut roots = RootCertStore::empty();
        roots.add(self.ca_cert.clone()).unwrap();
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.server_key.clone()));
        let config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![self.server_cert.clone()], key)
            .unwrap();
        TlsAcceptor::from(Arc::new(config))
    }
}

/// A complete HTTP answer.
pub fn http(status: &str, body: &[u8]) -> Vec<u8> {
    let mut answer = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    answer.extend_from_slice(body);
    answer
}

// The MQTT 5 broker stub: just enough of the protocol to connect, subscribe and publish.

pub const CONNECT: u8 = 0x10;
pub const CONNACK: &[u8] = &[0x20, 0x03, 0x00, 0x00, 0x00]; // no session, success, no properties
pub const SUBSCRIBE: u8 = 0x82;
pub const DISCONNECT: u8 = 0xe0;
/// PUBLISH, QoS 0: an empty topic and the property "topic alias" (0x23) = 5, never defined.
pub const UNKNOWN_ALIAS: &[u8] = &[0x30, 0x06, 0x00, 0x00, 0x03, 0x23, 0x00, 0x05];
pub const STEP: Duration = Duration::from_secs(10);

/// One MQTT packet: its first header byte and its body.
pub async fn read_packet(stream: &mut (impl AsyncRead + Unpin)) -> (u8, Vec<u8>) {
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
pub fn varint(bytes: &[u8]) -> (usize, usize) {
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
pub fn suback(subscribe: &[u8]) -> Vec<u8> {
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

pub async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let end = Instant::now() + STEP;
    while !condition() {
        assert!(Instant::now() < end, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A variable byte integer.
pub fn encode_varint(mut value: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let byte = (value % 128) as u8;
        value /= 128;
        if value == 0 {
            bytes.push(byte);
            return bytes;
        }
        bytes.push(byte | 0x80);
    }
}

/// A PUBLISH at QoS 0, without properties.
pub fn publish(topic: &str, payload: &[u8], retain: bool) -> Vec<u8> {
    let mut body = (topic.len() as u16).to_be_bytes().to_vec();
    body.extend_from_slice(topic.as_bytes());
    body.push(0x00); // no properties
    body.extend_from_slice(payload);
    let mut packet = vec![0x30 | u8::from(retain)];
    packet.extend(encode_varint(body.len()));
    packet.extend(body);
    packet
}

/// Accepts the client's next connection and answers its CONNECT and SUBSCRIBE: once this
/// returns, the client is about to be ready.
pub async fn accept_and_subscribe(
    acceptor: &TlsAcceptor,
    listener: &TcpListener,
) -> TlsStream<TcpStream> {
    let accept = async {
        acceptor
            .accept(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    };
    let mut stream = tokio::time::timeout(STEP, accept).await.unwrap();
    assert_eq!(read_packet(&mut stream).await.0, CONNECT);
    stream.write_all(CONNACK).await.unwrap();
    let (kind, subscribe) = read_packet(&mut stream).await;
    assert_eq!(kind, SUBSCRIBE);
    stream.write_all(&suback(&subscribe)).await.unwrap();
    stream
}
