//! Flow observations: what the Newboot console's data-flow view shows for a running module.
//!
//! A [`Flow`] counts four operations, each by outcome, with the time of its last success and
//! last failure, and the MQTT connection state. [`Flow::render`] gives the 17 series of the
//! common module flow contract in the Prometheus text format, served on the module's `/metrics`
//! with [`METRICS_CONTENT_TYPE`]. Counters live in the process: they restart from zero when the
//! module restarts. Labels are fixed: no topic, variable id or value ever becomes one.
//!
//! [`ModuleClient`](crate::ModuleClient) owns one and records `receive`, `process`, `enqueue`
//! and the connection itself; a module records `acquire` on
//! [`ModuleClient::flow`](crate::ModuleClient::flow).
//!
//! Ported from Newboot's reference implementation of the module flow-metrics contract and
//! contributed by Newboot under Apache-2.0; conformance vector at revision 589be4b6, identical in
//! every SDK.
use crate::telemetry::now_ms;
use std::fmt::{self, Write};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// The content type to serve [`Flow::render`] with: Prometheus text format 0.0.4.
pub const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

const CONNECTED: &str = "newboot_flow_mqtt_connected";
const LAST_ERROR: &str = "newboot_flow_last_error_timestamp_seconds";
const LAST_SUCCESS: &str = "newboot_flow_last_success_timestamp_seconds";
const OPERATIONS: &str = "newboot_flow_operations_total";
const MILLIS_PER_SECOND: f64 = 1000.0;
/// The connection before it was first observed.
const UNKNOWN: i64 = -1;

/// One of the four operations of the contract; the set is fixed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FlowOperation {
    /// A read from the machine or sensor completed or failed; a connection failure that
    /// prevents reading counts as a failure. Recorded by the module, never by the SDK.
    Acquire,
    /// A message from a linked module arrived (commands and reloads are not counted).
    Receive,
    /// That message was decoded and handled, or it broke the payload rules or a handler
    /// panicked.
    Process,
    /// The local MQTT client took a reading to send, or it was not taken. Never a claim that
    /// the broker, a subscriber or the cloud received it.
    Enqueue,
}

impl FlowOperation {
    /// Every operation, in the order they are rendered.
    pub const ALL: [Self; 4] = [Self::Acquire, Self::Receive, Self::Process, Self::Enqueue];

    /// The `operation` label: `acquire`, `receive`, `process` or `enqueue`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Acquire => "acquire",
            Self::Receive => "receive",
            Self::Process => "process",
            Self::Enqueue => "enqueue",
        }
    }
}

/// One operation's counts, and its last success and failure in ms since the Unix epoch.
struct Counters {
    error: AtomicU64,
    last_error: AtomicU64,
    last_success: AtomicU64,
    success: AtomicU64,
}

impl Counters {
    const fn new() -> Self {
        Self {
            error: AtomicU64::new(0),
            last_error: AtomicU64::new(0),
            last_success: AtomicU64::new(0),
            success: AtomicU64::new(0),
        }
    }
}

/// A process's flow observations; thread-safe, every method takes `&self`.
///
/// Use the one [`ModuleClient::flow`](crate::ModuleClient::flow) gives: one per process, so
/// the client's own observations and the module's are served together. Its connection gauge is
/// the client's readiness itself: [`ModuleClient::is_connected`](crate::ModuleClient::is_connected)
/// and the rendered `newboot_flow_mqtt_connected` never disagree.
pub struct Flow {
    connected: AtomicI64,
    operations: [Counters; FlowOperation::ALL.len()],
}

impl Default for Flow {
    /// Same as [`Flow::new`]: a separately created `Flow` is never served.
    fn default() -> Self {
        Self::new()
    }
}

impl Flow {
    /// Every counter at zero, the connection not yet observed (`-1`). A `Flow` created here is
    /// never served on the module's `/metrics` and never sees the connection: record `acquire` on
    /// [`ModuleClient::flow`](crate::ModuleClient::flow) instead.
    pub const fn new() -> Self {
        Self {
            connected: AtomicI64::new(UNKNOWN),
            operations: [const { Counters::new() }; FlowOperation::ALL.len()],
        }
    }

    /// Sets the MQTT connection: ready (`1`) or not (`0`). The client's readiness is this state.
    pub(crate) fn connection(&self, connected: bool) {
        self.connected
            .store(i64::from(connected), Ordering::Release);
    }

    /// True once the connection was set ready, until it is set not ready.
    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire) == 1
    }

    /// Records one finished operation, now. Call it once per operation, after it succeeded or
    /// failed; log the failure's details as usual, never in a label.
    pub fn record(&self, operation: FlowOperation, success: bool) {
        self.record_at(operation, success, now_ms());
    }

    fn record_at(&self, operation: FlowOperation, success: bool, timestamp_ms: u64) {
        let counters = &self.operations[operation as usize];
        let (count, last) = if success {
            (&counters.success, &counters.last_success)
        } else {
            (&counters.error, &counters.last_error)
        };
        count.fetch_add(1, Ordering::Relaxed);
        last.fetch_max(timestamp_ms, Ordering::Relaxed);
    }

    /// The 17 series in the Prometheus text format: the connection, then per operation its
    /// success and error counts, its last success and its last failure (Unix seconds, 0 before
    /// the first). Byte for byte what Newboot's reference implementation renders.
    pub fn render(&self) -> String {
        let mut text = format!(
            "# HELP {CONNECTED} MQTT connection: 1 connected, 0 disconnected, -1 unknown.\n\
             # TYPE {CONNECTED} gauge\n{CONNECTED} {}\n",
            self.connected.load(Ordering::Acquire)
        );
        for (name, kind, help) in [
            (
                OPERATIONS,
                "counter",
                "Application operations by outcome; enqueue means local queue acceptance.",
            ),
            (
                LAST_SUCCESS,
                "gauge",
                "Unix timestamp of last successful operation; zero means never.",
            ),
            (
                LAST_ERROR,
                "gauge",
                "Unix timestamp of last failed operation; zero means never.",
            ),
        ] {
            // Writing to a String cannot fail.
            let _ = writeln!(text, "# HELP {name} {help}\n# TYPE {name} {kind}");
            for operation in FlowOperation::ALL {
                let counters = &self.operations[operation as usize];
                let label = operation.label();
                if name == OPERATIONS {
                    for (outcome, count) in
                        [("success", &counters.success), ("error", &counters.error)]
                    {
                        let _ = writeln!(
                            text,
                            "{name}{{operation=\"{label}\",outcome=\"{outcome}\"}} {}",
                            count.load(Ordering::Relaxed)
                        );
                    }
                } else {
                    let last = if name == LAST_SUCCESS {
                        &counters.last_success
                    } else {
                        &counters.last_error
                    };
                    // Seconds as the shortest decimal: `3`, `1761900000.123`.
                    let _ = writeln!(
                        text,
                        "{name}{{operation=\"{label}\"}} {}",
                        last.load(Ordering::Relaxed) as f64 / MILLIS_PER_SECOND
                    );
                }
            }
        }
        text
    }
}

impl fmt::Debug for Flow {
    /// The connection and each operation's success and error counts.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Flow");
        debug.field("connected", &self.connected.load(Ordering::Acquire));
        for operation in FlowOperation::ALL {
            let counters = &self.operations[operation as usize];
            let counts = (
                counters.success.load(Ordering::Relaxed),
                counters.error.load(Ordering::Relaxed),
            );
            debug.field(operation.label(), &counts);
        }
        debug.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn series(text: &str) -> Vec<&str> {
        text.lines().filter(|line| !line.starts_with('#')).collect()
    }

    /// Ported from Newboot's reference implementation of the module flow-metrics contract and
    /// contributed by Newboot under Apache-2.0; conformance vector at revision 589be4b6,
    /// identical in every SDK.
    #[test]
    fn conforms_to_the_shared_contract() {
        let contract: Value =
            serde_json::from_str(include_str!("../tests/flow-conformance.json")).unwrap();
        let flow = Flow::new();
        for event in contract["events"].as_array().unwrap() {
            let operation = FlowOperation::ALL
                .into_iter()
                .find(|operation| operation.label() == event["operation"].as_str().unwrap())
                .unwrap();
            flow.record_at(
                operation,
                event["success"].as_bool().unwrap(),
                event["timestampSeconds"].as_u64().unwrap() * 1000,
            );
        }
        let text = flow.render();
        for (name, value) in contract["expected"].as_object().unwrap() {
            let line = format!("{name} {}", value.as_str().unwrap());
            assert!(text.lines().any(|l| l == line), "{line} in\n{text}");
        }
        assert_eq!(
            series(&text).len() as u64,
            contract["seriesCount"].as_u64().unwrap()
        );
    }

    #[test]
    fn renders_exactly_what_the_platform_renders() {
        let flow = Flow::new();
        flow.record_at(FlowOperation::Receive, true, 1_761_900_000_123);
        flow.record_at(FlowOperation::Receive, false, 1_761_900_001_000);
        flow.connection(true);
        let expected = "\
# HELP newboot_flow_mqtt_connected MQTT connection: 1 connected, 0 disconnected, -1 unknown.
# TYPE newboot_flow_mqtt_connected gauge
newboot_flow_mqtt_connected 1
# HELP newboot_flow_operations_total Application operations by outcome; enqueue means local queue acceptance.
# TYPE newboot_flow_operations_total counter
newboot_flow_operations_total{operation=\"acquire\",outcome=\"success\"} 0
newboot_flow_operations_total{operation=\"acquire\",outcome=\"error\"} 0
newboot_flow_operations_total{operation=\"receive\",outcome=\"success\"} 1
newboot_flow_operations_total{operation=\"receive\",outcome=\"error\"} 1
newboot_flow_operations_total{operation=\"process\",outcome=\"success\"} 0
newboot_flow_operations_total{operation=\"process\",outcome=\"error\"} 0
newboot_flow_operations_total{operation=\"enqueue\",outcome=\"success\"} 0
newboot_flow_operations_total{operation=\"enqueue\",outcome=\"error\"} 0
# HELP newboot_flow_last_success_timestamp_seconds Unix timestamp of last successful operation; zero means never.
# TYPE newboot_flow_last_success_timestamp_seconds gauge
newboot_flow_last_success_timestamp_seconds{operation=\"acquire\"} 0
newboot_flow_last_success_timestamp_seconds{operation=\"receive\"} 1761900000.123
newboot_flow_last_success_timestamp_seconds{operation=\"process\"} 0
newboot_flow_last_success_timestamp_seconds{operation=\"enqueue\"} 0
# HELP newboot_flow_last_error_timestamp_seconds Unix timestamp of last failed operation; zero means never.
# TYPE newboot_flow_last_error_timestamp_seconds gauge
newboot_flow_last_error_timestamp_seconds{operation=\"acquire\"} 0
newboot_flow_last_error_timestamp_seconds{operation=\"receive\"} 1761900001
newboot_flow_last_error_timestamp_seconds{operation=\"process\"} 0
newboot_flow_last_error_timestamp_seconds{operation=\"enqueue\"} 0
";
        assert_eq!(flow.render(), expected);
    }

    #[test]
    fn the_connection_is_unknown_until_observed() {
        let flow = Flow::new();
        assert!(series(&flow.render()).contains(&"newboot_flow_mqtt_connected -1"));
        flow.connection(true);
        assert!(series(&flow.render()).contains(&"newboot_flow_mqtt_connected 1"));
        flow.connection(false);
        assert!(series(&flow.render()).contains(&"newboot_flow_mqtt_connected 0"));
    }

    #[test]
    fn the_last_timestamps_never_go_back() {
        let flow = Flow::new();
        flow.record_at(FlowOperation::Enqueue, true, 5_000);
        flow.record_at(FlowOperation::Enqueue, true, 4_000);
        let text = flow.render();
        assert!(series(&text)
            .contains(&"newboot_flow_last_success_timestamp_seconds{operation=\"enqueue\"} 5"));
        assert!(series(&text).contains(
            &"newboot_flow_operations_total{operation=\"enqueue\",outcome=\"success\"} 2"
        ));
    }

    #[test]
    fn debug_shows_the_connection_and_the_counts() {
        let flow = Flow::new();
        flow.record_at(FlowOperation::Enqueue, false, 1);
        assert_eq!(
            format!("{flow:?}"),
            "Flow { connected: -1, acquire: (0, 0), receive: (0, 0), process: (0, 0), \
             enqueue: (0, 1) }"
        );
    }

    #[test]
    fn concurrent_records_are_all_counted() {
        let flow = Flow::new();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        flow.record(FlowOperation::Receive, true);
                    }
                });
            }
        });
        let text = flow.render();
        assert!(series(&text).contains(
            &"newboot_flow_operations_total{operation=\"receive\",outcome=\"success\"} 8000"
        ));
        assert_eq!(series(&text).len(), 17);
    }
}
