//! The topics a module publishes and subscribes to, all built from its [`Target`].
use crate::config::Target;
use serde_json::{json, Value};

/// The longest topic level or id, in bytes.
pub const MAX_SEGMENT_BYTES: usize = 200;

/// A topic level or id: non-empty, at most 200 bytes, no control character, `/`, `+` or `#`.
pub fn valid_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SEGMENT_BYTES
        && !value
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '+' | '#'))
}

impl Target {
    /// Where this module publishes its telemetry.
    pub fn telemetry_topic(&self) -> String {
        let Self {
            module_id,
            node_id,
            tenant_config_id,
            ..
        } = self;
        format!("output/telemetry/{tenant_config_id}/{node_id}/{module_id}/v")
    }

    /// The prefix of this module's commands: `<prefix>/<delivery>/<command>`.
    pub fn command_topic(&self) -> String {
        let Self {
            cluster_id,
            module_id,
            tenant_config_id,
            ..
        } = self;
        format!("input/directmethod/{tenant_config_id}/{cluster_id}/{module_id}")
    }

    /// The subscription that receives every command of this module.
    pub fn command_filter(&self) -> String {
        format!("{}/+/+", self.command_topic())
    }

    /// Where the answer to one command goes.
    pub fn reply_topic(&self, correlation_id: &str) -> String {
        let Self {
            cluster_id,
            module_id,
            tenant_config_id,
            ..
        } = self;
        format!(
            "output/directmethod/{tenant_config_id}/{cluster_id}/{module_id}/res/{correlation_id}"
        )
    }

    /// What a command's `target` must say, and every reply repeats.
    pub fn scope(&self) -> Value {
        let Self {
            cluster_id,
            module_id,
            tenant_config_id,
            ..
        } = self;
        json!({"clusterId": cluster_id, "moduleId": module_id, "tenantConfigId": tenant_config_id})
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn target() -> Target {
        Target {
            cluster_id: "cluster-a".into(),
            module_id: "my-module".into(),
            node_id: "node-a".into(),
            tenant_config_id: "tenant-a".into(),
        }
    }

    #[test]
    fn builds_the_module_topics() {
        let target = target();
        assert_eq!(
            target.telemetry_topic(),
            "output/telemetry/tenant-a/node-a/my-module/v"
        );
        assert_eq!(
            target.command_filter(),
            "input/directmethod/tenant-a/cluster-a/my-module/+/+"
        );
        assert_eq!(
            target.reply_topic("abc"),
            "output/directmethod/tenant-a/cluster-a/my-module/res/abc"
        );
        assert_eq!(
            target.scope(),
            json!({"clusterId": "cluster-a", "moduleId": "my-module", "tenantConfigId": "tenant-a"})
        );
    }

    #[test]
    fn segments_refuse_wildcards_separators_and_controls() {
        assert!(valid_segment("variable-0c6e3d1a"));
        assert!(valid_segment(&"x".repeat(MAX_SEGMENT_BYTES)));
        for bad in ["", "a/b", "a+", "#", "a\u{7f}", "tab\t"] {
            assert!(!valid_segment(bad), "{bad:?}");
        }
        assert!(!valid_segment(&"x".repeat(MAX_SEGMENT_BYTES + 1)));
        // Bytes, not characters: 100 two-byte characters fit, 101 do not.
        assert!(valid_segment(&"é".repeat(100)));
        assert!(!valid_segment(&"é".repeat(101)));
    }
}
