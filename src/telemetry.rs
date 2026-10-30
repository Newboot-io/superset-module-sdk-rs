//! Telemetry: what a module sends on its own topic and receives from its linked modules.
use crate::error::{Error, Result};
use crate::topics::valid_segment;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

/// The largest data message a module takes from its linked modules, in bytes.
pub const MAX_INPUT_BYTES: usize = 8192;
/// The largest timestamp, 2^53 - 1 ms: every integer up to it is exact as a double.
pub const MAX_TIMESTAMP_MS: u64 = (1 << 53) - 1;

/// One reading to send.
#[derive(Clone, Debug, PartialEq)]
pub struct Telemetry {
    /// A key of `variables` in the module twin.
    pub variable_id: String,
    /// The reading: a boolean, a number, a string, or an array of them.
    pub value: Value,
    /// The variable's display name. When absent, [`ModuleClient::send_telemetry`] uses the
    /// variable's `name` from the last fetched twin, or omits it.
    ///
    /// [`ModuleClient::send_telemetry`]: crate::ModuleClient::send_telemetry
    pub name: Option<String>,
    /// Milliseconds since the Unix epoch; now when absent.
    pub timestamp: Option<u64>,
}

impl Telemetry {
    /// A reading for one variable, without a name, timestamped when sent.
    pub fn new(variable_id: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            variable_id: variable_id.into(),
            value: value.into(),
            name: None,
            timestamp: None,
        }
    }

    /// Sets the display name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the timestamp, in milliseconds since the Unix epoch; at most [`MAX_TIMESTAMP_MS`],
    /// or sending fails.
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// The JSON message: `variableId`, `value`, `timestamp` (now when unset) and `name` when set.
    ///
    /// Errors with [`Error::Telemetry`] naming the field that breaks the payload rules:
    /// `variableId` a valid topic level, `value` a boolean, finite number, string or array of
    /// them, `timestamp` at most [`MAX_TIMESTAMP_MS`].
    pub fn to_payload(&self) -> Result<String> {
        self.payload(None)
    }

    /// The JSON message, named `default_name` when the reading has no name of its own.
    pub(crate) fn payload(&self, default_name: Option<&str>) -> Result<String> {
        if !valid_segment(&self.variable_id) {
            return Err(Error::Telemetry("variableId is not a valid id"));
        }
        if !kept_value(&self.value) {
            return Err(Error::Telemetry(
                "value is not a boolean, number, string or array of them",
            ));
        }
        let timestamp = self.timestamp.unwrap_or_else(now_ms);
        if timestamp > MAX_TIMESTAMP_MS {
            return Err(Error::Telemetry(
                "timestamp is not an integer from 0 to 2^53-1",
            ));
        }
        let mut message = json!({"timestamp": timestamp,
            "value": self.value, "variableId": self.variable_id});
        if let Some(name) = self.name.as_deref().or(default_name) {
            message["name"] = json!(name);
        }
        Ok(message.to_string())
    }
}

/// A message from a linked module.
#[derive(Clone, Debug, PartialEq)]
pub struct InputTelemetry {
    /// The linked module's telemetry topic, one of `configuration.inputs`.
    pub topic: String,
    /// The linked module's variable id.
    pub variable_id: String,
    /// A boolean, a number, a string, or an array of them.
    pub value: Value,
    /// The variable's display name, when the sender set one.
    pub name: Option<String>,
    /// The sender's timestamp, when it set one.
    pub timestamp: Option<u64>,
}

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A boolean, a finite number or a string. serde_json refuses NaN, infinities and numbers past
/// the double range when parsing; the check stays explicit.
fn is_scalar(value: &Value) -> bool {
    match value {
        Value::Bool(_) | Value::String(_) => true,
        Value::Number(number) => number.as_f64().is_some_and(f64::is_finite),
        _ => false,
    }
}

/// A timestamp: an integer from 0 to 2^53-1, written as an integer or as an integral number
/// such as `1.7e12`.
fn timestamp(value: &Value) -> Option<u64> {
    let ms = match value.as_u64() {
        Some(ms) => ms,
        None => {
            let ms = value.as_f64()?;
            if !(ms.is_finite() && ms >= 0.0 && ms.fract() == 0.0 && ms <= MAX_TIMESTAMP_MS as f64)
            {
                return None;
            }
            ms as u64
        }
    };
    (ms <= MAX_TIMESTAMP_MS).then_some(ms)
}

/// A scalar or an array of scalars, as an OPC UA array variable sends; anything nested is not.
fn kept_value(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().all(is_scalar),
        scalar => is_scalar(scalar),
    }
}

/// Parses a linked module's message. Drops retained messages, messages over 8 KiB, non-JSON
/// (including NaN and numbers too large for a double), and anything that breaks the payload
/// rules: a valid variable id, a scalar or array-of-scalars value, an optional string `name`
/// and an optional integer `timestamp` >= 0.
pub fn parse_input(topic: &str, payload: &[u8], retain: bool) -> Option<InputTelemetry> {
    if retain || payload.len() > MAX_INPUT_BYTES {
        return None;
    }
    let mut input: Value = serde_json::from_slice(payload).ok()?;
    let variable_id = input
        .get("variableId")?
        .as_str()
        .filter(|id| valid_segment(id))?
        .to_owned();
    // Optional fields, but when present they must be a string and an integer >= 0.
    let name = match input.get("name") {
        None => None,
        Some(name) => Some(name.as_str()?.to_owned()),
    };
    let timestamp = match input.get("timestamp") {
        None => None,
        Some(value) => Some(timestamp(value)?),
    };
    let value = input.get_mut("value")?.take();
    if !kept_value(&value) {
        return None;
    }
    Some(InputTelemetry {
        topic: topic.into(),
        variable_id,
        value,
        name,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC: &str = "output/telemetry/tenant-a/node-a/sensor/v";

    #[test]
    fn payload_has_exactly_the_four_fields() {
        let payload = Telemetry::new("variable-1", 21.5)
            .with_name("Boiler")
            .with_timestamp(1_700_000_000_000)
            .to_payload()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&payload).unwrap(),
            json!({"name": "Boiler", "timestamp": 1_700_000_000_000u64, "value": 21.5,
                "variableId": "variable-1"})
        );
    }

    #[test]
    fn name_is_omitted_or_defaulted_and_timestamp_is_now() {
        let before = now_ms();
        let payload: Value =
            serde_json::from_str(&Telemetry::new("v", true).to_payload().unwrap()).unwrap();
        assert!(payload.get("name").is_none(), "{payload}");
        let named: Value =
            serde_json::from_str(&Telemetry::new("v", true).payload(Some("Boiler")).unwrap())
                .unwrap();
        assert_eq!(named["name"], "Boiler");
        let own: Value = serde_json::from_str(
            &Telemetry::new("v", true)
                .with_name("Own")
                .payload(Some("Boiler"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(own["name"], "Own");
        let timestamp = payload["timestamp"].as_u64().unwrap();
        assert!(timestamp >= before && timestamp <= now_ms());
    }

    #[test]
    fn payload_refuses_bad_ids_and_nested_values() {
        assert!(Telemetry::new("a/b", 1).to_payload().is_err());
        assert!(Telemetry::new("", 1).to_payload().is_err());
        assert!(Telemetry::new("v", json!({"a": 1})).to_payload().is_err());
        assert!(Telemetry::new("v", json!([[1]])).to_payload().is_err());
        assert!(Telemetry::new("v", Value::Null).to_payload().is_err());
        assert!(Telemetry::new("v", json!([1, "a", false]))
            .to_payload()
            .is_ok());
    }

    #[test]
    fn timestamps_are_integers_up_to_2_pow_53_minus_1() {
        let parsed = |raw: &str| {
            let body = format!(r#"{{"timestamp": {raw}, "value": 1, "variableId": "v"}}"#);
            parse_input(TOPIC, body.as_bytes(), false).and_then(|input| input.timestamp)
        };
        assert_eq!(parsed("0"), Some(0));
        assert_eq!(parsed("1.7e12"), Some(1_700_000_000_000));
        assert_eq!(parsed("1700000000000.0"), Some(1_700_000_000_000));
        assert_eq!(parsed("9007199254740991"), Some(MAX_TIMESTAMP_MS));
        for bad in ["9007199254740992", "9.1e15", "1.5", "-1", "-0.5", "1e400"] {
            assert_eq!(parsed(bad), None, "{bad}");
        }
        let send = |ms| Telemetry::new("v", 1).with_timestamp(ms).to_payload();
        assert!(send(MAX_TIMESTAMP_MS).is_ok());
        let error = send(MAX_TIMESTAMP_MS + 1).unwrap_err();
        assert!(error.to_string().contains("timestamp"), "{error}");
    }

    #[test]
    fn numbers_must_be_finite_doubles() {
        // An integer past the double range is not JSON serde_json accepts: dropped.
        let huge = format!(r#"{{"value": 1{}, "variableId": "v"}}"#, "0".repeat(400));
        assert!(parse_input(TOPIC, huge.as_bytes(), false).is_none());
        let big = br#"{"value": 18446744073709551615, "variableId": "v"}"#;
        assert!(
            parse_input(TOPIC, big, false).is_some(),
            "u64::MAX is a finite double"
        );
        assert!(Telemetry::new("v", u64::MAX).to_payload().is_ok());
        assert!(Telemetry::new("v", -1.5e308).to_payload().is_ok());
        // serde_json turns a non-finite f64 into null, which is refused.
        assert!(Telemetry::new("v", f64::NAN).to_payload().is_err());
        assert!(Telemetry::new("v", f64::INFINITY).to_payload().is_err());
    }

    #[test]
    fn parses_a_linked_value() {
        let input = parse_input(
            TOPIC,
            br#"{"name":"Flow","timestamp":5,"value":[1,2.5],"variableId":"variable-up"}"#,
            false,
        )
        .unwrap();
        assert_eq!(input.topic, TOPIC);
        assert_eq!(input.variable_id, "variable-up");
        assert_eq!(input.value, json!([1, 2.5]));
        assert_eq!(input.name.as_deref(), Some("Flow"));
        assert_eq!(input.timestamp, Some(5));
    }

    #[test]
    fn drops_what_the_samples_drop() {
        let good = br#"{"value":1.5,"variableId":"v"}"#;
        assert!(parse_input(TOPIC, good, false).is_some());
        assert!(parse_input(TOPIC, good, true).is_none(), "retained");
        let big = format!(
            r#"{{"value":"{}","variableId":"v"}}"#,
            "x".repeat(MAX_INPUT_BYTES)
        );
        assert!(
            parse_input(TOPIC, big.as_bytes(), false).is_none(),
            "over 8 KiB"
        );
        for bad in [
            &b"not json"[..],
            br#"{"value": NaN, "variableId": "v"}"#,
            br#"{"value": 1e400, "variableId": "v"}"#,
            br#"{"value": {"a": [1]}, "variableId": "v"}"#,
            br#"{"value": [[1]], "variableId": "v"}"#,
            br#"{"value": null, "variableId": "v"}"#,
            br#"{"variableId": "v"}"#,
            br#"{"value": 1, "variableId": "a/b"}"#,
            br#"{"value": 1}"#,
            br#"{"name": 5, "value": 1, "variableId": "v"}"#,
            br#"{"timestamp": -1, "value": 1, "variableId": "v"}"#,
            br#"{"timestamp": 1.5, "value": 1, "variableId": "v"}"#,
            br#"{"timestamp": "now", "value": 1, "variableId": "v"}"#,
            br#"{"timestamp": 9007199254740992, "value": 1, "variableId": "v"}"#,
            br#"{"timestamp": 1e300, "value": 1, "variableId": "v"}"#,
            br#"{"timestamp": null, "value": 1, "variableId": "v"}"#,
            br#"{"name": null, "value": 1, "variableId": "v"}"#,
            b"5",
        ] {
            assert!(
                parse_input(TOPIC, bad, false).is_none(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }
}
