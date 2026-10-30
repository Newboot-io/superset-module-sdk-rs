//! Commands from the Superset: which ones a module answers, and the reply's shape.
use crate::config::Target;
use crate::topics::valid_segment;
use serde::Deserialize;
use serde_json::{json, Value};

/// The largest command, in bytes.
pub const MAX_COMMAND_BYTES: usize = 8192;
/// Clock skew allowed on a command's issue time.
pub const MAX_CLOCK_SKEW_MS: u64 = 1_000;
/// The longest a command may live.
pub const MAX_TTL_MS: u64 = 30_000;
/// The command that tells a module its twin changed.
pub const RELOAD: &str = "reload";
/// The short error code a failed reload answers.
pub const RELOAD_FAILED: &str = "reload_failed";
/// The short error code for a payload that is not an object.
pub const INVALID_PAYLOAD: &str = "invalid_payload";
const DELIVERIES: [&str; 2] = ["broadcast", "unicast"];

/// A command addressed to this module.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    /// The command's name, the last level of its topic.
    pub command: String,
    /// The id the reply repeats and its topic ends with.
    pub correlation_id: String,
    /// `broadcast` or `unicast`.
    pub delivery: String,
    /// When nobody waits for the answer any more, in ms since the Unix epoch.
    pub expires_at_unix_ms: u64,
    /// When the command was sent, in ms since the Unix epoch.
    pub issued_at_unix_ms: u64,
    #[serde(default)]
    /// The command's arguments, an object.
    pub payload: Value,
    /// Where the answer goes.
    pub reply_topic: String,
    /// The module the command is addressed to.
    pub target: Value,
}

impl Command {
    /// Issued now (1 s of clock skew allowed), valid at most 30 s, not yet expired.
    pub fn on_time(&self, now_ms: u64) -> bool {
        let (issued, expires) = (self.issued_at_unix_ms, self.expires_at_unix_ms);
        issued <= now_ms.saturating_add(MAX_CLOCK_SKEW_MS)
            && expires > now_ms
            && expires > issued
            && expires - issued <= MAX_TTL_MS
    }

    /// The reply to send, or `None` once the command expired: nobody waits for a late answer.
    /// `answer` is `{"result": …}` or `{"error": "<code>"}`; the reply adds exactly
    /// `correlationId` and `target`.
    pub fn reply(&self, mut answer: Value, now_ms: u64) -> Option<(String, String)> {
        if now_ms >= self.expires_at_unix_ms {
            return None;
        }
        answer["correlationId"] = json!(self.correlation_id);
        answer["target"] = self.target.clone();
        Some((self.reply_topic.clone(), answer.to_string()))
    }
}

/// Parses a message on this module's command topic. Returns the command only when it is
/// addressed to this module (topic, reply topic and target all match exactly) and on time;
/// retained, oversized and malformed messages are dropped. Unknown keys are ignored.
pub fn parse_command(
    topic: &str,
    payload: &[u8],
    retain: bool,
    target: &Target,
    now_ms: u64,
) -> Option<Command> {
    if retain || payload.len() > MAX_COMMAND_BYTES {
        return None;
    }
    let command: Command = serde_json::from_slice(payload).ok()?;
    let addressed = valid_segment(&command.correlation_id)
        && valid_segment(&command.command)
        && DELIVERIES.contains(&command.delivery.as_str())
        && topic
            == format!(
                "{}/{}/{}",
                target.command_topic(),
                command.delivery,
                command.command
            )
        && command.reply_topic == target.reply_topic(&command.correlation_id)
        && command.target == target.scope();
    (addressed && command.on_time(now_ms)).then_some(command)
}

/// The answer to a successful reload.
pub fn reloaded() -> Value {
    json!({"result": {"reloaded": true}})
}

/// A short error answer.
pub fn error_answer(code: &str) -> Value {
    json!({"error": code})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topics::tests::target;

    const NOW: u64 = 1_700_000_000_000;

    fn envelope(name: &str, id: &str) -> Value {
        let target = target();
        json!({
            "command": name, "correlationId": id, "delivery": "unicast",
            "expiresAtUnixMs": NOW + 5_000, "issuedAtUnixMs": NOW, "payload": {},
            "replyTopic": target.reply_topic(id), "target": target.scope(),
        })
    }

    fn topic(delivery: &str, name: &str) -> String {
        format!("{}/{delivery}/{name}", target().command_topic())
    }

    fn parse(topic: &str, body: &Value) -> Option<Command> {
        parse_command(topic, body.to_string().as_bytes(), false, &target(), NOW)
    }

    #[test]
    fn accepts_an_addressed_command_on_time() {
        let command = parse(&topic("unicast", "reload"), &envelope("reload", "r-1")).unwrap();
        assert_eq!(command.command, RELOAD);
        assert_eq!(command.correlation_id, "r-1");
        let mut broadcast = envelope("reload", "r-2");
        broadcast["delivery"] = json!("broadcast");
        broadcast["extra"] = json!("ignored");
        assert!(parse(&topic("broadcast", "reload"), &broadcast).is_some());
    }

    #[test]
    fn drops_commands_not_addressed_here() {
        let good = envelope("reload", "r-1");
        let cases: Vec<(&str, Value)> = vec![
            (
                "target",
                json!({"clusterId": "cluster-a", "moduleId": "other", "tenantConfigId": "tenant-a"}),
            ),
            ("replyTopic", json!(target().reply_topic("elsewhere"))),
            ("delivery", json!("multicast")),
            ("correlationId", json!("a/b")),
            ("expiresAtUnixMs", json!("soon")),
        ];
        for (key, value) in cases {
            let mut body = good.clone();
            body[key] = value;
            assert!(parse(&topic("unicast", "reload"), &body).is_none(), "{key}");
        }
        let mut no_deadline = good.clone();
        no_deadline
            .as_object_mut()
            .unwrap()
            .remove("expiresAtUnixMs");
        assert!(parse(&topic("unicast", "reload"), &no_deadline).is_none());
        // The topic must name the same delivery and command as the body.
        assert!(parse(&topic("broadcast", "reload"), &good).is_none());
        assert!(parse(&topic("unicast", "getValue"), &good).is_none());
    }

    #[test]
    fn drops_late_early_and_long_lived_commands() {
        for (issued, expires) in [
            (NOW - 5_000, NOW - 1),      // expired
            (NOW, NOW),                  // expires now
            (NOW + 5_000, NOW + 10_000), // issued in the future
            (NOW, NOW + MAX_TTL_MS + 1), // lives too long
            (NOW + 500, NOW + 400),      // expires before issued
        ] {
            let mut body = envelope("reload", "r-1");
            body["issuedAtUnixMs"] = json!(issued);
            body["expiresAtUnixMs"] = json!(expires);
            assert!(
                parse(&topic("unicast", "reload"), &body).is_none(),
                "{issued} {expires}"
            );
        }
        let mut skewed = envelope("reload", "r-1");
        skewed["issuedAtUnixMs"] = json!(NOW + MAX_CLOCK_SKEW_MS);
        assert!(parse(&topic("unicast", "reload"), &skewed).is_some());
    }

    #[test]
    fn drops_retained_oversized_and_malformed_messages() {
        let body = envelope("reload", "r-1").to_string();
        let topic = topic("unicast", "reload");
        assert!(parse_command(&topic, body.as_bytes(), true, &target(), NOW).is_none());
        let mut big = envelope("reload", "r-1");
        big["pad"] = json!("x".repeat(MAX_COMMAND_BYTES));
        assert!(parse(&topic, &big).is_none());
        for junk in ["5", "[1]", "null", "not json", "\"text\"", ""] {
            assert!(parse_command(&topic, junk.as_bytes(), false, &target(), NOW).is_none());
        }
    }

    #[test]
    fn replies_with_exactly_three_keys_until_expiry() {
        let command = parse(&topic("unicast", "reload"), &envelope("reload", "r-1")).unwrap();
        let (topic, body) = command.reply(reloaded(), NOW).unwrap();
        assert_eq!(topic, target().reply_topic("r-1"));
        assert_eq!(
            serde_json::from_str::<Value>(&body).unwrap(),
            json!({"correlationId": "r-1", "result": {"reloaded": true}, "target": target().scope()})
        );
        let (_, body) = command.reply(error_answer(RELOAD_FAILED), NOW).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&body).unwrap()["error"],
            RELOAD_FAILED
        );
        assert!(command.reply(reloaded(), NOW + 5_000).is_none());
    }
}
