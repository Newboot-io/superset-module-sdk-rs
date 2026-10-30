//! Shared test fixtures: a throwaway PKI, module configs on disk, and a one-shot HTTPS server
//! that requires a client certificate.
#![allow(dead_code)]

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use superset_module_sdk::ModuleConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
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
        let acceptor = TlsAcceptor::from(Arc::new(config));
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
