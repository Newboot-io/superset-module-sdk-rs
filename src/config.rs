//! `module.json`: what the Superset mounts into every module.
use crate::error::{Error, Result};
use crate::topics::valid_segment;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::path::Path;

/// Where the Superset mounts the module config, unless `MODULE_CONFIG` names another file.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/newboot/module.json";
/// The environment variable that overrides [`DEFAULT_CONFIG_PATH`].
pub const CONFIG_ENV: &str = "MODULE_CONFIG";
/// The only MQTT mode this release supports.
pub const MUTUAL_TLS: &str = "mutual-tls";
/// The broker URL schemes accepted; both mean MQTT over TLS.
pub const BROKER_SCHEMES: [&str; 2] = ["tcps://", "mqtts://"];
/// The broker port when `brokerUrl` names none.
pub const DEFAULT_BROKER_PORT: u16 = 8883;
const SECRET_SUFFIX: &str = ".json";

/// The module config, as the Superset renders it.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModuleConfig {
    /// `tcps://host[:port]` or `mqtts://host[:port]` of the Superset broker (port 8883 when
    /// absent).
    pub broker_url: String,
    /// The module's own configuration, with its linked inputs and secret files.
    #[serde(default)]
    pub configuration: Configuration,
    /// How to connect to the broker.
    pub mqtt: Mqtt,
    /// Who this module is: its ids name every topic.
    pub target: Target,
    /// Where to fetch the module twin; absent when the module has none.
    pub twin: Option<Twin>,
}

/// The module's configuration block.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Configuration {
    /// Telemetry topics of the modules the Superset links to this one.
    #[serde(default)]
    pub inputs: Vec<String>,
    /// One file per secret, `<name>.json`, each holding the value as a JSON string.
    #[serde(default)]
    pub secret_files: Vec<String>,
    /// Any other field of the configuration.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

/// The broker connection.
#[derive(Clone, Debug, Deserialize)]
pub struct Mqtt {
    /// The CA bundle that signs the broker's certificate.
    pub ca: String,
    /// One PEM file: the client certificate, its chain and its key.
    pub identity: String,
    /// The authentication mode; only `mutual-tls` is supported.
    pub mode: String,
}

/// The module twin endpoint.
#[derive(Clone, Debug, Deserialize)]
pub struct Twin {
    /// The CA bundle that signs the endpoint's certificate.
    pub ca: String,
    /// The `https://` URL of the module twin.
    pub endpoint: String,
    /// One PEM file: the client certificate, its chain and its key.
    pub identity: String,
}

/// The module's identity in its Superset.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    /// The cluster the module runs on.
    pub cluster_id: String,
    /// The module's id.
    pub module_id: String,
    /// The node the module belongs to.
    pub node_id: String,
    /// The tenant configuration the module belongs to.
    pub tenant_config_id: String,
}

impl ModuleConfig {
    /// Reads the file `MODULE_CONFIG` names, or [`DEFAULT_CONFIG_PATH`].
    pub fn from_env() -> Result<Self> {
        let path = std::env::var(CONFIG_ENV).unwrap_or_else(|_| DEFAULT_CONFIG_PATH.into());
        Self::from_path(path)
    }

    /// Reads and validates one config file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)
            .map_err(|error| Error::Config(format!("cannot read {}: {error}", path.display())))?;
        Self::from_slice(&bytes)
    }

    /// Parses and validates a config. `mqtt.mode` is checked first, on the raw JSON: a config
    /// for another mode is an [`Error::UnsupportedMode`] even when its other fields differ (no
    /// `identity`, a `brokerUrl` without scheme).
    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let not_config =
            |error: serde_json::Error| Error::Config(format!("not a module config: {error}"));
        let raw: Value = serde_json::from_slice(bytes).map_err(not_config)?;
        match raw.get("mqtt").and_then(|mqtt| mqtt.get("mode")) {
            Some(Value::String(mode)) if mode == MUTUAL_TLS => {}
            Some(Value::String(mode)) => return Err(Error::UnsupportedMode(mode.clone())),
            Some(other) => return Err(Error::UnsupportedMode(other.to_string())),
            None => return Err(Error::UnsupportedMode("no mode".into())),
        }
        let config: Self = serde_json::from_value(raw).map_err(not_config)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        self.broker_address()?;
        let mut paths = vec![
            ("mqtt.ca", &self.mqtt.ca),
            ("mqtt.identity", &self.mqtt.identity),
        ];
        if let Some(twin) = &self.twin {
            paths.push(("twin.ca", &twin.ca));
            paths.push(("twin.identity", &twin.identity));
        }
        paths.extend(
            self.configuration
                .secret_files
                .iter()
                .map(|file| ("configuration.secretFiles", file)),
        );
        if let Some((field, _)) = paths.iter().find(|(_, path)| path.is_empty()) {
            return Err(Error::Config(format!("{field} holds an empty path")));
        }
        let Target {
            cluster_id,
            module_id,
            node_id,
            tenant_config_id,
        } = &self.target;
        for (field, value) in [
            ("clusterId", cluster_id),
            ("moduleId", module_id),
            ("nodeId", node_id),
            ("tenantConfigId", tenant_config_id),
        ] {
            if !valid_segment(value) {
                return Err(Error::Config(format!(
                    "target.{field} is not a topic level"
                )));
            }
        }
        if let Some(input) = self.configuration.inputs.iter().find(|input| {
            input.is_empty() || input.contains(['+', '#']) || input.chars().any(char::is_control)
        }) {
            return Err(Error::Config(format!(
                "configuration.inputs holds {input:?}, not a topic"
            )));
        }
        if let Some(file) = self
            .configuration
            .secret_files
            .iter()
            .find(|file| !file.ends_with(SECRET_SUFFIX))
        {
            return Err(Error::Config(format!(
                "configuration.secretFiles holds {file:?}, not a .json file"
            )));
        }
        if let Some(twin) = &self.twin {
            if !twin.endpoint.starts_with("https://") {
                return Err(Error::Config("twin.endpoint is not https".into()));
            }
        }
        Ok(())
    }

    /// The broker's host and port, from `tcps://host[:port]` or `mqtts://host[:port]`; see
    /// [`parse_broker_url`]. An IPv6 literal is returned without its brackets.
    pub fn broker_address(&self) -> Result<(&str, u16)> {
        parse_broker_url(&self.broker_url).ok_or_else(|| {
            Error::Config("brokerUrl is not tcps:// or mqtts:// then host[:port]".into())
        })
    }
}

/// The broker URL rule shared by the SDKs: exactly `scheme://host[:port]`, scheme `tcps` or
/// `mqtts`; host `[A-Za-z0-9.-]+` or a bracketed IPv6 literal `[0-9A-Fa-f:.]+`; port 1 to 5 ASCII
/// digits with a value from 1 to 65535, [`DEFAULT_BROKER_PORT`] when absent. Nothing else: no
/// user info, path, query or fragment.
pub fn parse_broker_url(url: &str) -> Option<(&str, u16)> {
    let address = BROKER_SCHEMES
        .iter()
        .find_map(|scheme| url.strip_prefix(scheme))?;
    let (host, rest) = match address.strip_prefix('[') {
        Some(bracketed) => {
            let (host, rest) = bracketed.split_once(']')?;
            let ipv6 = |c: char| c.is_ascii_hexdigit() || matches!(c, ':' | '.');
            (!host.is_empty() && host.chars().all(ipv6)).then_some((host, rest))?
        }
        None => {
            let end = address.find(':').unwrap_or(address.len());
            let (host, rest) = address.split_at(end);
            let name = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-');
            (!host.is_empty() && host.chars().all(name)).then_some((host, rest))?
        }
    };
    let port = match rest {
        "" => DEFAULT_BROKER_PORT,
        _ => {
            let digits = rest.strip_prefix(':')?;
            if digits.is_empty() || digits.len() > 5 || !digits.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            let port: u32 = digits.parse().ok()?;
            u16::try_from(port).ok().filter(|port| *port != 0)?
        }
    };
    Some((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn sample() -> Value {
        json!({
            "brokerUrl": "tcps://broker:8883",
            "configuration": {
                "inputs": ["output/telemetry/tenant-a/node-a/sensor/v"],
                "secretFiles": ["/vault/material/device-password.json"],
                "interval": 5
            },
            "mqtt": {"ca": "/ca.crt", "identity": "/id.pem", "mode": "mutual-tls"},
            "target": {"clusterId": "cluster-a", "moduleId": "my-module", "nodeId": "node-a",
                "tenantConfigId": "tenant-a"},
            "twin": {"ca": "/twin-ca.crt", "endpoint": "https://twin/twin", "identity": "/t.pem"}
        })
    }

    fn parse(value: &Value) -> Result<ModuleConfig> {
        ModuleConfig::from_slice(value.to_string().as_bytes())
    }

    #[test]
    fn parses_what_a_superset_renders() {
        let config = parse(&sample()).unwrap();
        assert_eq!(config.broker_address().unwrap(), ("broker", 8883));
        assert_eq!(config.configuration.inputs.len(), 1);
        assert_eq!(config.configuration.other["interval"], 5);
        assert_eq!(config.target.module_id, "my-module");
        assert_eq!(config.twin.unwrap().endpoint, "https://twin/twin");
    }

    #[test]
    fn configuration_and_twin_are_optional() {
        let mut value = sample();
        let object = value.as_object_mut().unwrap();
        object.remove("configuration");
        object.remove("twin");
        let config = parse(&value).unwrap();
        assert!(config.configuration.inputs.is_empty());
        assert!(config.twin.is_none());
    }

    #[test]
    fn refuses_another_mqtt_mode() {
        let mut value = sample();
        value["mqtt"]["mode"] = json!("token");
        let error = parse(&value).unwrap_err();
        assert!(matches!(error, Error::UnsupportedMode(ref mode) if mode == "token"));
        assert!(error.to_string().contains("mutual-tls only"));
    }

    /// The shared broker URL table, verbatim.
    const BROKER_ACCEPT: [&str; 7] = [
        "tcps://broker",
        "mqtts://broker:8883",
        "tcps://mq.example.com:1",
        "tcps://10.0.0.1:65535",
        "tcps://[::1]:8883",
        "tcps://[::1]",
        "tcps://[1.2.3.4]",
    ];
    const BROKER_REFUSE: [&str; 22] = [
        "tcp://broker:8883",
        "tcps://broker:",
        "tcps://broker:0",
        "tcps://broker:65536",
        "tcps://broker:+8883",
        "tcps://broker:8883/",
        "tcps://broker:8883/x",
        "tcps://u:p@broker:8883",
        "tcps://broker:8883?x=1",
        "tcps://broker:8883#f",
        "tcps://a_b:8883",
        "tcps://b;x:8883",
        "tcps://h%41:8883",
        "tcps://bröker:8883",
        "tcps://broker\u{0000}:8883",
        "tcps://:8883",
        "tcps://[::1:8883",
        "broker:8883",
        "tcps://broker:8883\n",
        "tcps://bro\tker:8883",
        " tcps://broker",
        "tcps://broker:8883 ",
    ];

    #[test]
    fn broker_urls_follow_the_shared_table() {
        let expected = [
            ("broker", 8883),
            ("broker", 8883),
            ("mq.example.com", 1),
            ("10.0.0.1", 65535),
            ("::1", 8883),
            ("::1", 8883),
            ("1.2.3.4", 8883),
        ];
        for (url, address) in BROKER_ACCEPT.into_iter().zip(expected) {
            assert_eq!(parse_broker_url(url), Some(address), "{url}");
            let mut value = sample();
            value["brokerUrl"] = json!(url);
            assert_eq!(parse(&value).unwrap().broker_address().unwrap(), address);
        }
        for url in BROKER_REFUSE {
            assert_eq!(parse_broker_url(url), None, "{url:?}");
            let mut value = sample();
            value["brokerUrl"] = json!(url);
            assert!(matches!(parse(&value), Err(Error::Config(_))), "{url:?}");
        }
    }

    #[test]
    fn refuses_more_broker_urls() {
        for url in [
            "TCPS://broker",
            "tcps://",
            "tcps://broker:8883:1",
            "tcps://broker:123456",
            "tcps://broker:00008883",
            "tcps://bro ker",
            "tcps://[]:8883",
            "tcps://[::1]x",
            "tcps://[::g]",
            "tcps://broker:8883\n",
        ] {
            assert_eq!(parse_broker_url(url), None, "{url:?}");
        }
        assert_eq!(
            parse_broker_url("tcps://broker:08883"),
            Some(("broker", 8883))
        );
    }

    #[test]
    fn checks_the_mode_first_on_the_platforms_real_shape() {
        // A config for another mode: no scheme in brokerUrl, no identity.
        let raw = json!({
            "brokerUrl": "other-broker:18884",
            "mqtt": {"ca": "/etc/newboot/ca/mqtt-ca.crt", "mode": "token"},
            "target": {"clusterId": "cluster-a", "moduleId": "my-module", "nodeId": "node-a",
                "tenantConfigId": "tenant-a"}
        });
        let error = ModuleConfig::from_slice(raw.to_string().as_bytes()).unwrap_err();
        assert!(
            matches!(error, Error::UnsupportedMode(ref mode) if mode == "token"),
            "{error}"
        );
        let mut not_a_string = raw.clone();
        not_a_string["mqtt"]["mode"] = json!(5);
        assert!(matches!(
            ModuleConfig::from_slice(not_a_string.to_string().as_bytes()),
            Err(Error::UnsupportedMode(_))
        ));
        let mut no_mode = sample();
        no_mode["mqtt"].as_object_mut().unwrap().remove("mode");
        assert!(matches!(parse(&no_mode), Err(Error::UnsupportedMode(_))));
        let mut no_mqtt = sample();
        no_mqtt.as_object_mut().unwrap().remove("mqtt");
        assert!(matches!(parse(&no_mqtt), Err(Error::UnsupportedMode(_))));
    }

    #[test]
    fn refuses_empty_paths() {
        for pointer in ["/mqtt/ca", "/mqtt/identity", "/twin/ca", "/twin/identity"] {
            let mut value = sample();
            *value.pointer_mut(pointer).unwrap() = json!("");
            let error = parse(&value).unwrap_err();
            assert!(
                error.to_string().contains("empty path"),
                "{pointer}: {error}"
            );
        }
        let mut value = sample();
        value["configuration"]["secretFiles"] = json!([""]);
        assert!(matches!(parse(&value), Err(Error::Config(_))));
    }

    #[test]
    fn refuses_wildcard_or_empty_inputs() {
        for bad in ["output/telemetry/+/v", "output/#", "", "a\nb"] {
            let mut value = sample();
            value["configuration"]["inputs"] = json!(["output/ok/v", bad]);
            let error = parse(&value).unwrap_err();
            assert!(
                error.to_string().contains("configuration.inputs"),
                "{error}"
            );
        }
    }

    #[test]
    fn refuses_secret_files_that_are_not_json() {
        for bad in [
            "/vault/material/password",
            "/vault/material/password.txt",
            "",
        ] {
            let mut value = sample();
            value["configuration"]["secretFiles"] = json!([bad]);
            let error = parse(&value).unwrap_err();
            assert!(error.to_string().contains("secretFiles"), "{error}");
        }
    }

    #[test]
    fn refuses_a_target_that_is_not_a_topic_level() {
        for bad in ["", "a/b", "a+", "a#", "a\nb"] {
            let mut value = sample();
            value["target"]["nodeId"] = json!(bad);
            assert!(matches!(parse(&value), Err(Error::Config(_))), "{bad:?}");
        }
    }

    #[test]
    fn a_null_twin_means_no_twin() {
        let mut value = sample();
        value["twin"] = Value::Null;
        assert!(parse(&value).unwrap().twin.is_none());
    }

    #[test]
    fn refuses_a_plain_http_twin() {
        let mut value = sample();
        value["twin"]["endpoint"] = json!("http://twin/twin");
        assert!(matches!(parse(&value), Err(Error::Config(_))));
    }

    #[test]
    fn refuses_missing_fields_and_non_json() {
        let mut value = sample();
        value.as_object_mut().unwrap().remove("target");
        assert!(matches!(parse(&value), Err(Error::Config(_))));
        assert!(matches!(
            ModuleConfig::from_slice(b"nope"),
            Err(Error::Config(_))
        ));
    }
}
