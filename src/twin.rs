//! The module twin: the fields saved in the console and the module's variables.
use crate::config::Twin;
use crate::error::{Error, Result};
use serde_json::{Map, Value};
use std::time::Duration;

/// The largest module twin answer the SDK reads.
pub const MAX_TWIN_BYTES: usize = 2 * 1024 * 1024;
/// One deadline for a whole fetch, connection to last byte: within the 15 s a Superset gives a
/// reload.
pub const TWIN_TIMEOUT: Duration = Duration::from_secs(12);
/// The header that carries a reload's correlation id to the module twin endpoint.
pub const CORRELATION_HEADER: &str = "x-newboot-correlation-id";
const VARIABLES: &str = "variables";

/// This module's twin, exactly as the endpoint answered it: its fields plus an optional
/// `variables` object (by variable id).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ModuleTwin(Map<String, Value>);

impl ModuleTwin {
    /// One field of the twin, `variables` included.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    /// The module's variables, by variable id; `None` when the twin lists none.
    pub fn variables(&self) -> Option<&Map<String, Value>> {
        self.0.get(VARIABLES).and_then(Value::as_object)
    }

    /// The display name of a variable, or its id when it has none.
    pub fn variable_name<'a>(&'a self, variable_id: &'a str) -> &'a str {
        self.declared_name(variable_id).unwrap_or(variable_id)
    }

    /// The `name` the twin gives a variable, if any.
    pub fn declared_name(&self, variable_id: &str) -> Option<&str> {
        self.variables()
            .and_then(|variables| variables.get(variable_id))
            .and_then(|variable| variable.get("name"))
            .and_then(Value::as_str)
    }

    /// The ids of the module's variables.
    pub fn variable_ids(&self) -> impl Iterator<Item = &str> {
        self.variables()
            .into_iter()
            .flat_map(|variables| variables.keys().map(String::as_str))
    }

    /// The whole twin.
    pub fn as_map(&self) -> &Map<String, Value> {
        &self.0
    }

    /// The whole twin, owned.
    pub fn into_map(self) -> Map<String, Value> {
        self.0
    }
}

/// Checks a module twin answer: it holds exactly one module, this one, and its variables (when
/// present) are an object.
pub fn parse_twin_answer(body: &[u8], module_id: &str) -> Result<ModuleTwin> {
    let not_ours = || Error::twin("the answer is not this module's");
    let mut answer: Value = serde_json::from_slice(body).map_err(|_| not_ours())?;
    let modules = answer.get_mut("modules").and_then(Value::as_object_mut);
    let ours = match modules {
        Some(modules) if modules.len() == 1 => modules.remove(module_id),
        _ => None,
    };
    match ours {
        Some(Value::Object(twin)) if twin.get(VARIABLES).is_none_or(Value::is_object) => {
            Ok(ModuleTwin(twin))
        }
        _ => Err(not_ours()),
    }
}

/// A correlation id the SDK forwards: letters, digits and `-` only.
pub(crate) fn plain_correlation_id(id: &str) -> bool {
    !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// Fetches the module twin over HTTPS with the module's client certificate. The certificate
/// and CA files are read again for every request: they rotate.
pub(crate) async fn fetch_twin(
    twin: &Twin,
    module_id: &str,
    correlation_id: Option<&str>,
) -> Result<ModuleTwin> {
    let http_error = |error: reqwest::Error| Error::twin(error.without_url().to_string());
    let read = |path: &str, what: &str| {
        std::fs::read(path).map_err(|error| Error::twin(format!("cannot read the {what}: {error}")))
    };
    // The identity file holds the certificate, its chain and the key.
    let identity = read(&twin.identity, "client certificate")?;
    let mut http = reqwest::Client::builder()
        .https_only(true)
        .identity(reqwest::Identity::from_pem(&identity).map_err(http_error)?)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(TWIN_TIMEOUT)
        .tls_built_in_root_certs(false);
    let bundle = read(&twin.ca, "CA bundle")?;
    for ca in reqwest::Certificate::from_pem_bundle(&bundle).map_err(http_error)? {
        http = http.add_root_certificate(ca);
    }
    let mut request = http.build().map_err(http_error)?.get(&twin.endpoint);
    if let Some(id) = correlation_id.filter(|id| plain_correlation_id(id)) {
        request = request.header(CORRELATION_HEADER, id);
    }
    let mut response = request.send().await.map_err(http_error)?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(Error::twin(format!("HTTP {}", response.status())));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(http_error)? {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_TWIN_BYTES {
            return Err(Error::twin("answer too large"));
        }
    }
    parse_twin_answer(&body, module_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(value: Value) -> Result<ModuleTwin> {
        parse_twin_answer(value.to_string().as_bytes(), "my-module")
    }

    #[test]
    fn returns_this_modules_fields_and_variables() {
        let twin = parse(json!({"path": "/site/scope", "modules": {"my-module": {
            "serverport": 4840,
            "variables": {"variable-1": {"modelId": "dtmi:example:Variable;1", "name": "Boiler"}}
        }}}))
        .unwrap();
        assert_eq!(twin.get("serverport"), Some(&json!(4840)));
        assert!(twin.get("variables").is_some(), "kept as received");
        assert_eq!(twin.as_map().len(), 2);
        assert_eq!(twin.clone().into_map()["serverport"], 4840);
        assert_eq!(twin.variable_name("variable-1"), "Boiler");
        assert_eq!(twin.variable_name("variable-2"), "variable-2");
        assert_eq!(twin.declared_name("variable-1"), Some("Boiler"));
        assert_eq!(twin.declared_name("variable-2"), None);
        assert_eq!(twin.variable_ids().collect::<Vec<_>>(), ["variable-1"]);
    }

    #[test]
    fn variables_are_optional() {
        let twin = parse(json!({"modules": {"my-module": {"active": true}}})).unwrap();
        assert!(twin.variables().is_none());
        assert_eq!(twin.variable_ids().count(), 0);
    }

    #[test]
    fn refuses_anything_but_exactly_this_module() {
        for answer in [
            json!([1]),
            json!({}),
            json!({"modules": {}}),
            json!({"modules": []}),
            json!({"modules": {"other": {}}}),
            json!({"modules": {"my-module": {}, "other": {}}}),
            json!({"modules": {"my-module": 5}}),
            json!({"modules": {"my-module": {"variables": [1]}}}),
        ] {
            assert!(
                matches!(parse(answer.clone()), Err(Error::Twin(_))),
                "{answer}"
            );
        }
    }

    #[test]
    fn refuses_non_json_and_deep_nesting_without_overflowing() {
        assert!(parse_twin_answer(b"{", "my-module").is_err());
        let deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
        assert!(parse_twin_answer(deep.as_bytes(), "my-module").is_err());
    }

    #[test]
    fn a_non_finite_number_anywhere_makes_the_answer_invalid() {
        for body in [
            r#"{"modules": {"my-module": {"limit": NaN}}}"#,
            r#"{"modules": {"my-module": {"limit": 1e400}}}"#,
            r#"{"modules": {"my-module": {"variables": {"v": {"max": -Infinity}}}}}"#,
            r#"{"path": 1e999, "modules": {"my-module": {}}}"#,
        ] {
            assert!(
                matches!(
                    parse_twin_answer(body.as_bytes(), "my-module"),
                    Err(Error::Twin(_))
                ),
                "{body}"
            );
        }
    }

    #[test]
    fn forwards_plain_correlation_ids_only() {
        assert!(plain_correlation_id("reload-1"));
        assert!(!plain_correlation_id(""));
        assert!(!plain_correlation_id("a b"));
        assert!(!plain_correlation_id("a\r\nx: y"));
    }
}
