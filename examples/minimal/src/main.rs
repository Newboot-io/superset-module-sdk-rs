//! A minimal module: sends one reading per variable of its module twin every few seconds, logs
//! the linked modules' inputs by name only, and serves /healthz and /readyz on port 8080.
use axum::{http::StatusCode, routing::get, Router};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use superset_module_sdk::{telemetry::now_ms, InputTelemetry, ModuleClient, ModuleTwin, Telemetry};

const HEALTH_PORT: u16 = 8080;
const RETRY_TWIN_AFTER: Duration = Duration::from_secs(3);
const SEND_EVERY: Duration = Duration::from_secs(5);

/// Read your machine or sensor here.
fn read_sensor(_variable_id: &str) -> f64 {
    20.0 + f64::from(now_ms() as u32 % 500) / 100.0
}

/// At start: keep trying until the twin arrives. Without it there is nothing to send.
async fn load_twin(client: ModuleClient, twin: Arc<Mutex<Option<ModuleTwin>>>) {
    loop {
        match client.get_twin(None).await {
            Ok(fetched) => {
                twin.lock().unwrap().get_or_insert(fetched);
                return;
            }
            Err(error) => eprintln!("twin: {error}"),
        }
        tokio::time::sleep(RETRY_TWIN_AFTER).await;
    }
}

/// One reading per variable; the SDK names each after its variable in the module twin.
fn send(client: &ModuleClient, twin: &Mutex<Option<ModuleTwin>>) {
    let twin = twin.lock().unwrap();
    let Some(twin) = twin.as_ref() else { return };
    for variable_id in twin.variable_ids() {
        // Ok(false): the connection dropped; the next round sends again once it is back.
        if let Err(error) =
            client.send_telemetry(Telemetry::new(variable_id, read_sensor(variable_id)))
        {
            eprintln!("send: {error}");
        }
    }
}

#[tokio::main]
async fn main() {
    let client = match ModuleClient::from_env() {
        Ok(client) => client,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };
    // Stop at start when a declared secret cannot be read. The error names the secret only.
    if let Err(error) = client.check_secrets() {
        eprintln!("{error}");
        std::process::exit(1);
    }

    let twin: Arc<Mutex<Option<ModuleTwin>>> = Arc::default();
    client.on_telemetry(|input: &InputTelemetry| {
        let name = input.name.as_deref().unwrap_or(&input.variable_id);
        eprintln!("input from {name}");
    });
    let changed = twin.clone();
    client.on_twin_change(move |new: &ModuleTwin| *changed.lock().unwrap() = Some(new.clone()));
    if client.config().twin.is_some() {
        tokio::spawn(load_twin(client.clone(), twin.clone()));
    } else {
        eprintln!("this module has no module twin: it has no variables, so it sends nothing");
    }

    // A Superset restarts the module when /healthz fails, and counts it as ready once /readyz
    // succeeds (any 2xx status).
    let probe = client.clone();
    let readyz = move || {
        let ready = probe.is_connected();
        async move {
            if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
    };
    let health = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(readyz));
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", HEALTH_PORT))
        .await
        .expect("health port");
    tokio::spawn(async move { axum::serve(listener, health).await });

    if let Err(error) = client.connect().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    let mut timer = tokio::time::interval(SEND_EVERY);
    loop {
        tokio::select! {
            _ = terminate.recv() => break,
            _ = timer.tick() => if client.is_connected() { send(&client, &twin) },
        }
    }
    client.close().await;
}
