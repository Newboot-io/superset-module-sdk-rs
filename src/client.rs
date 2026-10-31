//! [`ModuleClient`]: the module's connection to its Superset.
use crate::command::{
    error_answer, parse_command, reloaded, Command, INVALID_PAYLOAD, RELOAD, RELOAD_FAILED,
};
use crate::config::{ModuleConfig, Mqtt, Target};
use crate::error::{Error, Result};
use crate::flow::{self, FlowOperation};
use crate::secrets;
use crate::telemetry::{now_ms, parse_input, InputTelemetry, Telemetry};
use crate::twin::{fetch_twin, ModuleTwin};
use rumqttc::v5::mqttbytes::v5::{Filter, Packet, Publish, RetainForwardRule, SubscribeReasonCode};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions};
use rumqttc::{Outgoing, TlsConfiguration, Transport};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Pause before reconnecting after the connection drops or the certificates cannot be read.
pub const RECONNECT_AFTER: Duration = Duration::from_secs(5);
/// The maximum packet size announced to the broker on CONNECT: 2 MiB + 1 KiB (2098176 bytes).
/// A Superset broker forwards messages up to 2 MiB; with room for a packet's topic and
/// properties, any of them is read and dropped by the 8 KiB limits, never a reconnect. rumqttc
/// also refuses a bigger packet from its header, before reading it.
pub const MAX_PACKET_BYTES: u32 = 2 * 1024 * 1024 + 1024;
const CLOSE_WITHIN: Duration = Duration::from_secs(2);
const REQUEST_QUEUE: usize = 64;

type TelemetryHandler = Arc<dyn Fn(&InputTelemetry) + Send + Sync>;
type TwinHandler = Arc<dyn Fn(&ModuleTwin) + Send + Sync>;

/// A module's client: its module twin, its secrets, and its telemetry over MQTT.
///
/// Cheap to clone; every clone shares one connection.
#[derive(Clone)]
pub struct ModuleClient {
    inner: Arc<Inner>,
}

struct Inner {
    /// Set by `close()`: the client cannot connect again.
    closed: AtomicBool,
    config: ModuleConfig,
    /// What `metrics()` renders: `receive`, `process`, `enqueue`, and the connection, which is
    /// also the readiness (`set_connected`): `/metrics` and `is_connected()` never disagree.
    flow: flow::Flow,
    /// The last twin fetched, by `get_twin` or a reload: it names the variables sent.
    last_twin: Mutex<Option<ModuleTwin>>,
    mqtt: Mutex<Option<AsyncClient>>,
    on_telemetry: RwLock<Vec<TelemetryHandler>>,
    on_twin_change: RwLock<Vec<TwinHandler>>,
    reloading: tokio::sync::Mutex<()>,
    task: Mutex<Option<JoinHandle<()>>>,
    /// Wakes the connection's task from a pause when the client closes.
    wake: Notify,
}

impl ModuleClient {
    /// Reads the module config from `MODULE_CONFIG` (default `/etc/newboot/module.json`).
    ///
    /// Errors: [`Error::Config`], or [`Error::UnsupportedMode`] when `mqtt.mode` is not
    /// `mutual-tls`.
    pub fn from_env() -> Result<Self> {
        Ok(Self::new(ModuleConfig::from_env()?))
    }

    /// A client for an already parsed config.
    pub fn new(config: ModuleConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                closed: AtomicBool::new(false),
                config,
                flow: flow::Flow::new(),
                last_twin: Mutex::new(None),
                mqtt: Mutex::new(None),
                on_telemetry: RwLock::new(Vec::new()),
                on_twin_change: RwLock::new(Vec::new()),
                reloading: tokio::sync::Mutex::new(()),
                task: Mutex::new(None),
                wake: Notify::new(),
            }),
        }
    }

    /// The module config.
    pub fn config(&self) -> &ModuleConfig {
        &self.inner.config
    }

    /// The module's identity in its Superset.
    pub fn target(&self) -> &Target {
        &self.inner.config.target
    }

    /// Fetches this module's twin. Certificates are read again on every call. A correlation id
    /// (letters, digits and `-`) is passed to the endpoint for tracing; others are not sent.
    /// The twin is kept: [`send_telemetry`](Self::send_telemetry) names readings after its
    /// variables.
    ///
    /// Errors: [`Error::Twin`], with [`TwinError::NotConfigured`](crate::TwinError) when the
    /// module has no twin.
    pub async fn get_twin(&self, correlation_id: Option<&str>) -> Result<ModuleTwin> {
        self.inner.fetch_twin(correlation_id).await
    }

    /// Reads one secret by name; the file is read again on every call. Never log the value.
    ///
    /// Errors: [`Error::Secret`], naming the secret only.
    pub fn get_secret(&self, name: &str) -> Result<String> {
        secrets::read_secret(&self.inner.config.configuration, name)
    }

    /// The names of the declared secrets.
    pub fn secret_names(&self) -> Vec<String> {
        secrets::secret_names(&self.inner.config.configuration)
    }

    /// Reads every declared secret once: call it at start to stop early when one is missing.
    ///
    /// Errors: [`Error::Secret`] naming the first secret that cannot be read.
    pub fn check_secrets(&self) -> Result<()> {
        secrets::check_secrets(&self.inner.config.configuration)
    }

    /// Adds a handler for every valid message from a linked module. Handlers run in the order
    /// they were added, on the connection's task: keep them short and never block. A handler
    /// that panics is reported on stderr as "panicked" only, and the next one runs.
    pub fn on_telemetry(&self, handler: impl Fn(&InputTelemetry) + Send + Sync + 'static) {
        self.inner
            .on_telemetry
            .write()
            .unwrap()
            .push(Arc::new(handler));
    }

    /// Adds a handler for the new twin after each successful `reload`, called in the order they
    /// were added and before the SDK answers. If one panics, the reload answers `reload_failed`
    /// and the handlers after it are skipped.
    pub fn on_twin_change(&self, handler: impl Fn(&ModuleTwin) + Send + Sync + 'static) {
        self.inner
            .on_twin_change
            .write()
            .unwrap()
            .push(Arc::new(handler));
    }

    /// Starts the MQTT connection on the current Tokio runtime and returns at once: it reads
    /// the certificates, connects, subscribes to the linked modules' topics and the module's
    /// commands, and on any failure (certificate files missing or unreadable included) logs
    /// `mqtt: <reason>` to stderr, waits [`RECONNECT_AFTER`] and tries again with the files read
    /// again. Readiness is [`is_connected`](Self::is_connected). A second call is a no-op.
    ///
    /// Errors: [`Error::Closed`] after [`close`](Self::close).
    pub async fn connect(&self) -> Result<()> {
        // A no-op when the application installed its own provider first.
        let _ = rustls::crypto::ring::default_provider().install_default();
        // `close()` marks the client closed before it takes this lock: checked under the lock, a
        // connect racing a close either starts first (and close stops it) or sees it closed.
        let mut slot = self.inner.mqtt.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if slot.is_some() {
            return Ok(());
        }
        let (client, events) = AsyncClient::new(mqtt_options(&self.inner.config)?, REQUEST_QUEUE);
        *slot = Some(client);
        // Stored before the lock is released, so a racing close() always finds the task.
        let task = tokio::spawn(run(self.inner.clone(), events));
        *self.inner.task.lock().unwrap() = Some(task);
        drop(slot);
        Ok(())
    }

    /// True once connected and every subscription is granted; false before [`connect`](Self::connect),
    /// while reconnecting and after [`close`](Self::close). This is what a module's `/readyz`
    /// should answer.
    pub fn is_connected(&self) -> bool {
        self.inner.flow.is_connected()
    }

    /// Publishes one reading on the module's telemetry topic, QoS 0, not retained. Without a
    /// name, the reading takes its variable's `name` from the last fetched twin, if any.
    ///
    /// Returns `Ok(true)` when the reading was handed to the connected client, `Ok(false)` when
    /// the client is not connected (nothing is queued while disconnected) or its local request
    /// queue is full. Either is one `enqueue` in [`metrics`](Self::metrics), a success for
    /// `Ok(true)` only.
    ///
    /// Errors: [`Error::Telemetry`] naming the field that breaks the payload rules. The reading
    /// never reaches the MQTT client: no `enqueue` is recorded.
    pub fn send_telemetry(&self, telemetry: Telemetry) -> Result<bool> {
        let payload = self.inner.telemetry_payload(&telemetry)?;
        let topic = self.inner.config.target.telemetry_topic();
        let sent = self.inner.publish(topic, payload);
        self.inner.flow.record(FlowOperation::Enqueue, sent);
        Ok(sent)
    }

    /// The module's flow observations in the Prometheus text format, for the Newboot console's
    /// data-flow view: serve them on `GET /metrics` (port 8080) with
    /// [`METRICS_CONTENT_TYPE`](crate::METRICS_CONTENT_TYPE). These are the 17 series of the
    /// common module flow contract: the MQTT connection (`-1` until first observed), and per
    /// operation its success and error counts and its last success and last error. The client
    /// records `receive` (a message from a linked module), `process` (it was decoded and every
    /// handler returned, or not), `enqueue` ([`send_telemetry`](Self::send_telemetry)) and the
    /// connection; commands and reloads are not counted. Counters restart from zero with the
    /// process.
    pub fn metrics(&self) -> String {
        self.inner.flow.render()
    }

    /// The flow observations [`metrics`](Self::metrics) renders, shared by every clone. Record
    /// [`FlowOperation::Acquire`] here when the module reads its machine or sensor.
    pub fn flow(&self) -> &flow::Flow {
        &self.inner.flow
    }

    /// Disconnects and stops the connection's task, returning within about 4 s even if a handler
    /// blocks it (that handler then finishes in the background). The client cannot connect again.
    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.set_connected(false);
        self.inner.wake.notify_one();
        if let Some(client) = self.inner.mqtt.lock().unwrap().take() {
            let _ = client.try_disconnect();
        }
        let task = self.inner.task.lock().unwrap().take();
        if let Some(mut task) = task {
            if tokio::time::timeout(CLOSE_WITHIN, &mut task).await.is_err() {
                task.abort();
                // Bounded too: a handler blocking the task cannot hang close().
                let _ = tokio::time::timeout(CLOSE_WITHIN, task).await;
            }
        }
        // A SUBACK handled while closing must not leave the client ready.
        self.inner.set_connected(false);
    }
}

impl Inner {
    /// Readiness, which is the flow's connection.
    fn set_connected(&self, connected: bool) {
        self.flow.connection(connected);
    }

    async fn fetch_twin(&self, correlation_id: Option<&str>) -> Result<ModuleTwin> {
        let twin = self
            .config
            .twin
            .as_ref()
            .ok_or(Error::Twin(crate::TwinError::NotConfigured))?;
        let twin = fetch_twin(twin, &self.config.target.module_id, correlation_id).await?;
        *self.last_twin.lock().unwrap() = Some(twin.clone());
        Ok(twin)
    }

    /// A reading's payload, named after its variable in the last fetched twin when it has no
    /// name of its own.
    fn telemetry_payload(&self, telemetry: &Telemetry) -> Result<String> {
        let last_twin = self.last_twin.lock().unwrap();
        let name = match telemetry.name {
            Some(_) => None,
            None => last_twin
                .as_ref()
                .and_then(|twin| twin.declared_name(&telemetry.variable_id)),
        };
        telemetry.payload(name)
    }

    /// Hands a message to the client only while connected and subscribed: nothing is queued
    /// for later. False when not connected or the client's queue is full.
    fn publish(&self, topic: String, payload: String) -> bool {
        if !self.flow.is_connected() {
            return false;
        }
        let client = self.mqtt.lock().unwrap();
        client.as_ref().is_some_and(|client| {
            client
                .try_publish(topic, QoS::AtMostOnce, false, payload)
                .is_ok()
        })
    }

    /// Answers a command while connected and before it expires; otherwise nothing is sent.
    fn reply(&self, command: &Command, answer: serde_json::Value) -> bool {
        command
            .reply(answer, now_ms())
            .is_some_and(|(topic, body)| self.publish(topic, body))
    }

    /// Every telemetry handler, in order; a panicking one is reported and skipped. False when
    /// one panicked.
    fn deliver(&self, input: &InputTelemetry) -> bool {
        let handlers = self.on_telemetry.read().unwrap().clone();
        let mut handled = true;
        for handler in handlers {
            if catch_unwind(AssertUnwindSafe(|| handler(input))).is_err() {
                eprintln!("telemetry handler panicked");
                handled = false;
            }
        }
        handled
    }

    /// Every twin-change handler, in order; false as soon as one panics.
    fn twin_changed(&self, twin: &ModuleTwin) -> bool {
        let handlers = self.on_twin_change.read().unwrap().clone();
        handlers
            .iter()
            .all(|handler| catch_unwind(AssertUnwindSafe(|| handler(twin))).is_ok())
    }

    fn subscribe(&self) {
        let topics = std::iter::once(self.config.target.command_filter())
            .chain(self.config.configuration.inputs.iter().cloned());
        // A Superset broker sends stored messages with the retain flag set, and they are dropped;
        // live ones arrive with the flag cleared. On other brokers these options skip stored
        // messages.
        let filters = topics.map(|path| Filter {
            path,
            preserve_retain: true,
            qos: QoS::AtLeastOnce,
            retain_forward_rule: RetainForwardRule::Never,
            ..Default::default()
        });
        if let Some(client) = self.mqtt.lock().unwrap().as_ref() {
            let _ = client.try_subscribe_many(filters);
        }
    }

    fn receive(self: &Arc<Self>, message: Publish) {
        let Ok(topic) = std::str::from_utf8(&message.topic) else {
            return;
        };
        let command = match route(
            &self.config,
            topic,
            &message.payload,
            message.retain,
            now_ms(),
        ) {
            Route::Ignore => return,
            // One `receive` and one `process` per message from a linked module.
            Route::Input(input) => {
                self.flow.record(FlowOperation::Receive, true);
                let handled = self.deliver(&input);
                self.flow.record(FlowOperation::Process, handled);
                return;
            }
            Route::Invalid => {
                self.flow.record(FlowOperation::Receive, true);
                self.flow.record(FlowOperation::Process, false);
                return;
            }
            Route::Answer(command, answer) => {
                self.reply(&command, answer);
                return;
            }
            Route::Reload(command) => command,
        };
        let inner = self.clone();
        tokio::spawn(async move {
            // One reload at a time.
            let _one_at_a_time = inner.reloading.lock().await;
            let answer = match inner.fetch_twin(Some(&command.correlation_id)).await {
                Ok(twin) if inner.twin_changed(&twin) => reloaded(),
                Ok(_) => {
                    eprintln!("reload: twin-change handler panicked");
                    error_answer(RELOAD_FAILED)
                }
                Err(error) => {
                    eprintln!("reload: {error}");
                    error_answer(RELOAD_FAILED)
                }
            };
            inner.reply(&command, answer);
        });
    }

    /// Waits before the next attempt; false when the client closed meanwhile.
    async fn pause(&self) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        tokio::select! {
            _ = tokio::time::sleep(RECONNECT_AFTER) => !self.closed.load(Ordering::Acquire),
            _ = self.wake.notified() => false,
        }
    }
}

/// What to do with one incoming message.
#[derive(Debug, PartialEq)]
enum Route {
    /// Answer a command at once.
    Answer(Command, serde_json::Value),
    /// Drop it: not addressed here, off time, malformed, a command this release ignores, or a
    /// linked module's stored message.
    Ignore,
    /// A valid message from a linked module.
    Input(InputTelemetry),
    /// Drop it: a message from a linked module that breaks the payload rules.
    Invalid,
    /// Fetch the twin again, then answer.
    Reload(Command),
}

fn route(config: &ModuleConfig, topic: &str, payload: &[u8], retain: bool, now: u64) -> Route {
    if config
        .configuration
        .inputs
        .iter()
        .any(|input| input == topic)
    {
        // A stored message, sent again on every subscription: not an input received now.
        if retain {
            return Route::Ignore;
        }
        return parse_input(topic, payload, retain).map_or(Route::Invalid, Route::Input);
    }
    match parse_command(topic, payload, retain, &config.target, now) {
        // v0.1 answers reload only; any other command is left unanswered.
        Some(command) if command.command != RELOAD => Route::Ignore,
        Some(command) if !command.payload.is_object() => {
            Route::Answer(command, error_answer(INVALID_PAYLOAD))
        }
        Some(command) => Route::Reload(command),
        None => Route::Ignore,
    }
}

/// The host name when `HOSTNAME` is unset or empty (Linux).
const KERNEL_HOSTNAME: &str = "/proc/sys/kernel/hostname";

/// The first non-empty of `HOSTNAME` and the kernel's host name; empty when neither is set.
fn pick_hostname(env: Option<&str>, kernel: Option<&str>) -> String {
    [env, kernel]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|name| !name.is_empty())
        .unwrap_or_default()
        .to_owned()
}

/// The connection options, without the TLS transport (read before every connection).
fn mqtt_options(config: &ModuleConfig) -> Result<MqttOptions> {
    let (host, port) = config.broker_address()?;
    let hostname = pick_hostname(
        std::env::var("HOSTNAME").ok().as_deref(),
        std::fs::read_to_string(KERNEL_HOSTNAME).ok().as_deref(),
    );
    let client_id = format!("{}-{hostname}", config.target.module_id);
    let mut options = MqttOptions::new(client_id, host, port);
    // rumqttc drops the connection over a message above its limit (10 KiB unless set): raise it
    // to what the broker forwards. The broker is told on CONNECT.
    options.set_max_packet_size(Some(MAX_PACKET_BYTES));
    Ok(options)
}

/// Certificates rotate: read the files again before every connection.
fn transport(mqtt: &Mqtt) -> std::io::Result<Transport> {
    let identity = std::fs::read(&mqtt.identity)?; // certificate, chain and key in one file
    Ok(Transport::tls_with_config(TlsConfiguration::Simple {
        alpn: None,
        ca: std::fs::read(&mqtt.ca)?,
        client_auth: Some((identity.clone(), identity)),
    }))
}

/// What the connection's task does after one event.
#[derive(Debug, PartialEq)]
enum Flow {
    /// Poll the next event.
    Next,
    /// Wait [`RECONNECT_AFTER`], read the certificates again and reconnect.
    Reconnect,
    /// The client closed: end the task.
    Stop,
}

fn handle(inner: &Arc<Inner>, event: Event) -> Flow {
    match event {
        Event::Incoming(Packet::ConnAck(..)) => inner.subscribe(),
        Event::Incoming(Packet::SubAck(ack)) => {
            let granted = ack
                .return_codes
                .iter()
                .all(|code| matches!(code, SubscribeReasonCode::Success(_)));
            if !granted {
                eprintln!("mqtt: subscription refused");
            }
            // Never ready again once closed.
            inner.set_connected(granted && !inner.closed.load(Ordering::Acquire));
        }
        Event::Incoming(Packet::Publish(message)) => inner.receive(message),
        // A DISCONNECT goes out on close(), and also when rumqttc meets a protocol error:
        // only close() ends the task.
        Event::Outgoing(Outgoing::Disconnect) => {
            inner.set_connected(false);
            if inner.closed.load(Ordering::Acquire) {
                return Flow::Stop;
            }
            eprintln!("mqtt: disconnected");
            return Flow::Reconnect;
        }
        _ => {}
    }
    Flow::Next
}

/// Not ready once the connection's task ends, however it ends (aborted, or a panic included).
struct NotReadyOnExit(Arc<Inner>);

impl Drop for NotReadyOnExit {
    fn drop(&mut self) {
        self.0.set_connected(false);
    }
}

async fn run(inner: Arc<Inner>, mut events: EventLoop) {
    let _not_ready_on_exit = NotReadyOnExit(inner.clone());
    'connection: loop {
        match transport(&inner.config.mqtt) {
            Ok(transport) => {
                events.options.set_transport(transport);
            }
            Err(error) => {
                eprintln!("mqtt: cannot read the certificates: {error}");
                inner.set_connected(false);
                if inner.pause().await {
                    continue;
                }
                break;
            }
        }
        loop {
            match events.poll().await {
                Ok(event) => match handle(&inner, event) {
                    Flow::Next => {}
                    Flow::Stop => break 'connection,
                    Flow::Reconnect => {
                        // rumqttc keeps its socket after a DISCONNECT it raised itself (a
                        // protocol error): drop it, so the next poll opens a new connection,
                        // with the certificates read again, and subscribes again.
                        events.clean();
                        if inner.pause().await {
                            continue 'connection;
                        }
                        break 'connection;
                    }
                },
                Err(error) => {
                    inner.set_connected(false);
                    if inner.closed.load(Ordering::Acquire) {
                        break 'connection;
                    }
                    eprintln!("mqtt: {error}");
                    if inner.pause().await {
                        continue 'connection;
                    }
                    break 'connection;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    const NOW: u64 = 1_700_000_000_000;
    const INPUT: &str = "output/telemetry/tenant-a/node-a/sensor/v";

    fn config() -> ModuleConfig {
        ModuleConfig::from_slice(
            json!({
                "brokerUrl": "tcps://broker:8883",
                "configuration": {"inputs": [INPUT]},
                "mqtt": {"ca": "/ca.crt", "identity": "/id.pem", "mode": "mutual-tls"},
                "target": {"clusterId": "cluster-a", "moduleId": "my-module", "nodeId": "node-a",
                    "tenantConfigId": "tenant-a"}
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap()
    }

    fn command(name: &str, payload: Value) -> (String, Vec<u8>) {
        command_at(name, payload, NOW)
    }

    fn command_at(name: &str, payload: Value, now: u64) -> (String, Vec<u8>) {
        let target = config().target;
        let body = json!({
            "command": name, "correlationId": "c-1", "delivery": "unicast",
            "expiresAtUnixMs": now + 5_000, "issuedAtUnixMs": now, "payload": payload,
            "replyTopic": target.reply_topic("c-1"), "target": target.scope(),
        });
        (
            format!("{}/unicast/{name}", target.command_topic()),
            body.to_string().into_bytes(),
        )
    }

    fn input() -> InputTelemetry {
        parse_input(INPUT, br#"{"value":1,"variableId":"v"}"#, false).unwrap()
    }

    #[test]
    fn telemetry_handlers_run_in_order_and_survive_a_panic() {
        let client = ModuleClient::new(config());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = calls.clone();
        client.on_telemetry(move |input| {
            first
                .lock()
                .unwrap()
                .push(format!("1:{}", input.variable_id))
        });
        client.on_telemetry(|_| panic!("handler failure"));
        let third = calls.clone();
        client.on_telemetry(move |input| {
            third
                .lock()
                .unwrap()
                .push(format!("3:{}", input.variable_id))
        });
        assert!(!client.inner.deliver(&input()), "a handler panicked");
        assert!(!client.inner.deliver(&input()), "a handler panicked");
        assert_eq!(*calls.lock().unwrap(), ["1:v", "3:v", "1:v", "3:v"]);
    }

    #[test]
    fn a_panicking_twin_handler_fails_the_reload_and_skips_the_rest() {
        let client = ModuleClient::new(config());
        let twin = ModuleTwin::default();
        assert!(client.inner.twin_changed(&twin), "no handler: reloaded");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = calls.clone();
        client.on_twin_change(move |_| first.lock().unwrap().push(1));
        assert!(client.inner.twin_changed(&twin));
        client.on_twin_change(|_| panic!("handler failure"));
        let third = calls.clone();
        client.on_twin_change(move |_| third.lock().unwrap().push(3));
        assert!(!client.inner.twin_changed(&twin));
        assert_eq!(*calls.lock().unwrap(), [1, 1]);
    }

    #[test]
    fn readings_take_their_name_from_the_last_twin() {
        let client = ModuleClient::new(config());
        let payload = |telemetry: Telemetry| -> Value {
            serde_json::from_str(&client.inner.telemetry_payload(&telemetry).unwrap()).unwrap()
        };
        assert!(payload(Telemetry::new("v", 1)).get("name").is_none());
        let twin = crate::twin::parse_twin_answer(
            br#"{"modules":{"my-module":{"variables":{"v":{"name":"Boiler"}}}}}"#,
            "my-module",
        )
        .unwrap();
        *client.inner.last_twin.lock().unwrap() = Some(twin);
        assert_eq!(payload(Telemetry::new("v", 1))["name"], "Boiler");
        assert_eq!(
            payload(Telemetry::new("v", 1).with_name("Own"))["name"],
            "Own"
        );
        assert!(payload(Telemetry::new("w", 1)).get("name").is_none());
    }

    #[test]
    fn send_telemetry_returns_false_until_connected() {
        let client = ModuleClient::new(config());
        assert!(!client.send_telemetry(Telemetry::new("v", 1)).unwrap());
        assert!(matches!(
            client.send_telemetry(Telemetry::new("a/b", 1)),
            Err(Error::Telemetry(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_connect_racing_a_close_never_leaves_a_task_running() {
        for _ in 0..200 {
            let client = ModuleClient::new(config());
            let (connecting, closing) = (client.clone(), client.clone());
            let connect = tokio::spawn(async move { connecting.connect().await });
            let close = tokio::spawn(async move { closing.close().await });
            let connected = connect.await.unwrap();
            close.await.unwrap();
            // Either connect won (and close stopped it) or it saw the client closed.
            assert!(matches!(connected, Ok(()) | Err(Error::Closed)));
            assert!(client.inner.mqtt.lock().unwrap().is_none());
            assert!(client.inner.task.lock().unwrap().is_none());
            assert!(matches!(client.connect().await, Err(Error::Closed)));
        }
    }

    fn suback(return_codes: Vec<SubscribeReasonCode>) -> Event {
        Event::Incoming(Packet::SubAck(rumqttc::v5::mqttbytes::v5::SubAck {
            pkid: 1,
            return_codes,
            properties: None,
        }))
    }

    #[test]
    fn a_disconnect_reconnects_unless_the_client_closed() {
        let client = ModuleClient::new(config());
        client.inner.set_connected(true);
        let disconnect = || Event::Outgoing(Outgoing::Disconnect);
        // A protocol error makes rumqttc send DISCONNECT: the task reconnects.
        assert_eq!(handle(&client.inner, disconnect()), Flow::Reconnect);
        assert!(!client.is_connected());
        assert!(connected_series(&client, "0"));
        client.inner.closed.store(true, Ordering::Release);
        assert_eq!(handle(&client.inner, disconnect()), Flow::Stop);
    }

    /// The flow's connection series has this value.
    fn connected_series(client: &ModuleClient, value: &str) -> bool {
        let line = format!("newboot_flow_mqtt_connected {value}");
        client.metrics().lines().any(|l| l == line)
    }

    #[test]
    fn ready_only_when_every_subscription_is_granted() {
        let client = ModuleClient::new(config());
        assert!(connected_series(&client, "-1"), "not yet observed");
        let granted = SubscribeReasonCode::Success(QoS::AtLeastOnce);
        assert_eq!(
            handle(&client.inner, suback(vec![granted, granted])),
            Flow::Next
        );
        assert!(client.is_connected());
        assert!(connected_series(&client, "1"));
        let refused = suback(vec![granted, SubscribeReasonCode::NotAuthorized]);
        assert_eq!(handle(&client.inner, refused), Flow::Next);
        assert!(!client.is_connected());
        assert!(connected_series(&client, "0"));
        assert_eq!(
            handle(&client.inner, Event::Outgoing(Outgoing::PingReq)),
            Flow::Next
        );
    }

    #[test]
    fn a_suback_after_close_leaves_the_client_not_ready() {
        let client = ModuleClient::new(config());
        client.inner.closed.store(true, Ordering::Release);
        let granted = SubscribeReasonCode::Success(QoS::AtLeastOnce);
        assert_eq!(handle(&client.inner, suback(vec![granted])), Flow::Next);
        assert!(!client.is_connected());
        assert!(connected_series(&client, "0"));
    }

    fn enqueues(client: &ModuleClient, outcome: &str) -> bool {
        let line = format!(
            "newboot_flow_operations_total{{operation=\"enqueue\",outcome=\"{outcome}\"}} 1"
        );
        client.metrics().lines().any(|l| l == line)
    }

    #[tokio::test]
    async fn a_full_request_queue_is_an_enqueue_error() {
        let client = ModuleClient::new(config());
        // The event loop is never polled: the client's request queue only fills.
        let (mqtt, _events) = AsyncClient::new(mqtt_options(&config()).unwrap(), 1);
        *client.inner.mqtt.lock().unwrap() = Some(mqtt);
        client.inner.set_connected(true);
        assert!(client.send_telemetry(Telemetry::new("v", 1)).unwrap());
        assert!(enqueues(&client, "success"));
        assert!(
            !client.send_telemetry(Telemetry::new("v", 1)).unwrap(),
            "queue full"
        );
        assert!(enqueues(&client, "error"));
    }

    #[test]
    fn the_client_id_falls_back_to_the_kernel_host_name() {
        assert_eq!(pick_hostname(Some("pod-1"), Some("node\n")), "pod-1");
        assert_eq!(pick_hostname(Some(""), Some("node\n")), "node");
        assert_eq!(pick_hostname(None, Some(" node \n")), "node");
        assert_eq!(pick_hostname(Some("  "), None), "");
        assert_eq!(pick_hostname(None, None), "");
    }

    #[test]
    fn announces_the_max_packet_size_on_connect() {
        let options = mqtt_options(&config()).unwrap();
        assert_eq!(options.max_packet_size(), Some(MAX_PACKET_BYTES));
        assert_eq!(MAX_PACKET_BYTES, 2_098_176);
        assert_eq!(options.broker_address(), ("broker".to_string(), 8883));
        assert!(options.client_id().starts_with("my-module-"));
    }

    #[test]
    fn replies_and_readings_go_out_only_while_connected() {
        let client = ModuleClient::new(config());
        let now = now_ms();
        let (topic, body) = command_at(RELOAD, json!({}), now);
        let Route::Reload(command) = route(&config(), &topic, &body, false, now) else {
            panic!("a reload");
        };
        // Not connected (and no client at all): nothing is queued.
        assert!(!client.inner.reply(&command, reloaded()));
        assert!(!client.send_telemetry(Telemetry::new("v", 1)).unwrap());
        // Marked connected without a client: still nothing sent, never a panic.
        client.inner.set_connected(true);
        assert!(!client.inner.reply(&command, reloaded()));
    }

    #[test]
    fn routes_linked_inputs_to_the_handler() {
        let route = route(
            &config(),
            INPUT,
            br#"{"value":1,"variableId":"v"}"#,
            false,
            NOW,
        );
        assert!(matches!(route, Route::Input(ref input) if input.variable_id == "v"));
    }

    #[test]
    fn drops_invalid_inputs_and_unknown_topics() {
        let config = config();
        assert_eq!(
            route(&config, INPUT, b"not json", false, NOW),
            Route::Invalid
        );
        assert_eq!(
            route(
                &config,
                INPUT,
                br#"{"value":1,"variableId":"v"}"#,
                true,
                NOW
            ),
            Route::Ignore
        );
        assert_eq!(
            route(
                &config,
                "output/telemetry/other/v",
                br#"{"value":1,"variableId":"v"}"#,
                false,
                NOW
            ),
            Route::Ignore
        );
    }

    #[test]
    fn reloads_on_a_valid_reload() {
        let (topic, body) = command(RELOAD, json!({}));
        assert!(matches!(
            route(&config(), &topic, &body, false, NOW),
            Route::Reload(ref command) if command.correlation_id == "c-1"
        ));
    }

    #[test]
    fn answers_invalid_payload_to_a_reload_without_an_object() {
        let (topic, body) = command(RELOAD, json!("text"));
        let route = route(&config(), &topic, &body, false, NOW);
        assert!(
            matches!(route, Route::Answer(_, ref answer) if answer["error"] == INVALID_PAYLOAD)
        );
    }

    #[test]
    fn ignores_other_commands_and_late_reloads() {
        let config = config();
        for name in ["getValue", "ping"] {
            let (topic, body) = command(name, json!({"variableId": "v"}));
            assert_eq!(
                route(&config, &topic, &body, false, NOW),
                Route::Ignore,
                "{name}"
            );
        }
        let (topic, body) = command(RELOAD, json!({}));
        assert_eq!(
            route(&config, &topic, &body, false, NOW + 5_000),
            Route::Ignore
        );
        assert_eq!(route(&config, &topic, &body, true, NOW), Route::Ignore);
    }
}
