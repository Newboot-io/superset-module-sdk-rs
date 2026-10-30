//! alarm-monitor: watches the smoothed temperature (`temp_avg`) a linked module sends and raises
//! an alarm when it leaves the band its twin sets. Sends `alarm` (0 or 1) and `alarm_count` on
//! every `temp_avg` it receives; serves /healthz and /readyz on port 8080.
mod alarm;

use alarm::{temp_avg, AlarmState, Limits, Owned, ALARM, ALARM_COUNT};
use axum::{http::StatusCode, routing::get, Router};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use superset_module_sdk::{Error, ModuleClient, ModuleTwin, TwinError};

const ALERT_TOKEN: &str = "alert-token";
const HEALTH_PORT: u16 = 8080;
const RETRY_TWIN_AFTER: Duration = Duration::from_secs(3);

/// What the module knows: its limits and variables (from the twin) and the alarm.
#[derive(Default)]
struct Monitor {
    limits: Limits,
    owned: Owned,
    state: AlarmState,
}

impl Monitor {
    /// Applies a twin, at start and on every reload. Logs field names, never values.
    fn apply(&mut self, twin: &ModuleTwin) {
        let (limits, defaulted) = Limits::from_twin(twin);
        for field in defaulted {
            eprintln!("twin: {field} is missing or invalid, using its default");
        }
        self.limits = limits;
        self.owned = Owned::from_twin(twin);
        for (name, id) in [
            (ALARM, &self.owned.alarm),
            (ALARM_COUNT, &self.owned.alarm_count),
        ] {
            match id {
                Some(id) => eprintln!("twin: {name} is variable {id}"),
                None => eprintln!("twin: no variable named {name}: it is not sent"),
            }
        }
    }
}

fn fail(error: impl std::fmt::Display) -> ! {
    eprintln!("{error}");
    std::process::exit(1);
}

#[tokio::main]
async fn main() {
    let client = ModuleClient::from_env().unwrap_or_else(|error| fail(error));
    // The alert token would authenticate a webhook; it is checked, never printed.
    client.check_secrets().unwrap_or_else(|error| fail(error));
    match client.get_secret(ALERT_TOKEN) {
        Ok(token) if !token.is_empty() => {}
        Ok(_) => fail(format!("secret {ALERT_TOKEN} is empty")),
        Err(error) => fail(error),
    }

    let health = {
        let probe = client.clone();
        Router::new()
            .route("/healthz", get(|| async { StatusCode::OK }))
            .route(
                "/readyz",
                get(move || {
                    let ready = probe.is_connected();
                    async move {
                        if ready {
                            StatusCode::OK
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        }
                    }
                }),
            )
    };
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", HEALTH_PORT))
        .await
        .unwrap_or_else(|error| fail(format!("health port: {error}")));
    tokio::spawn(async move { axum::serve(listener, health).await });

    // Without its twin the module knows neither its limits nor its variables: wait for it.
    let twin = loop {
        match client.get_twin(None).await {
            Ok(twin) => break twin,
            // Deployed without a twin: waiting cannot help.
            Err(error @ Error::Twin(TwinError::NotConfigured)) => fail(error),
            Err(error) => eprintln!("{error}"),
        }
        tokio::time::sleep(RETRY_TWIN_AFTER).await;
    };
    let monitor = Arc::new(Mutex::new(Monitor::default()));
    monitor.lock().unwrap().apply(&twin);

    let changed = monitor.clone();
    client.on_twin_change(move |twin| changed.lock().unwrap().apply(twin));
    let (watching, sender) = (monitor.clone(), client.clone());
    client.on_telemetry(move |input| {
        let Some(value) = temp_avg(input) else { return };
        let mut monitor = watching.lock().unwrap();
        let limits = monitor.limits;
        if monitor.state.observe(value, &limits) {
            eprintln!(
                "alarm raised ({} so far): alert would be sent",
                monitor.state.count
            );
        }
        for reading in monitor.state.readings(&monitor.owned) {
            if let Err(error) = sender.send_telemetry(reading) {
                eprintln!("send: {error}");
            }
        }
    });
    client.connect().await.unwrap_or_else(|error| fail(error));

    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .unwrap_or_else(|error| fail(format!("SIGTERM handler: {error}")));
    terminate.recv().await;
    client.close().await;
}
