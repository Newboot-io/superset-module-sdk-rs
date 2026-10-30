//! Secrets: one file per secret, `<name>.json`, holding the value as a JSON string.
use crate::config::Configuration;
use crate::error::{Error, Result, SecretError};

/// The name of the secret a file holds: its base name without `.json`.
pub fn secret_name(path: &str) -> &str {
    let file = path.rsplit('/').next().unwrap_or(path);
    file.strip_suffix(".json").unwrap_or(file)
}

/// Reads one secret, by name. The file is read again on every call: a new value replaces it
/// while the module runs. Errors name the secret only, never its value.
pub fn read_secret(configuration: &Configuration, name: &str) -> Result<String> {
    let fail = |reason| Error::Secret {
        name: name.into(),
        reason,
    };
    let file = configuration
        .secret_files
        .iter()
        .find(|path| path.rsplit('/').next() == Some(format!("{name}.json").as_str()))
        .ok_or_else(|| fail(SecretError::NotDeclared))?;
    let bytes = std::fs::read(file).map_err(|_| fail(SecretError::Unreadable))?;
    // The parser's own message can quote the file: never pass it on.
    serde_json::from_slice::<String>(&bytes).map_err(|_| fail(SecretError::NotAString))
}

/// The names of the declared secrets, in `configuration.secretFiles` order.
pub fn secret_names(configuration: &Configuration) -> Vec<String> {
    configuration
        .secret_files
        .iter()
        .map(|file| secret_name(file).to_owned())
        .collect()
}

/// Reads every declared secret once, and returns the first that cannot be read. Call it at
/// start: a module without its secrets should stop.
pub fn check_secrets(configuration: &Configuration) -> Result<()> {
    for file in &configuration.secret_files {
        read_secret(configuration, secret_name(file))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALUE: &str = "s3cret-value";

    fn configuration(dir: &tempfile::TempDir, files: &[(&str, &str)]) -> Configuration {
        let mut configuration = Configuration::default();
        for (name, contents) in files {
            let path = dir.path().join(name);
            std::fs::write(&path, contents).unwrap();
            configuration
                .secret_files
                .push(path.to_str().unwrap().into());
        }
        configuration
    }

    #[test]
    fn reads_a_json_string_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let config = configuration(&dir, &[("device-password.json", &format!("\"{VALUE}\""))]);
        assert_eq!(read_secret(&config, "device-password").unwrap(), VALUE);
        check_secrets(&config).unwrap();
    }

    #[test]
    fn rereads_the_file_on_every_call() {
        let dir = tempfile::tempdir().unwrap();
        let config = configuration(&dir, &[("token.json", "\"one\"")]);
        assert_eq!(read_secret(&config, "token").unwrap(), "one");
        std::fs::write(&config.secret_files[0], "\"two\"").unwrap();
        assert_eq!(read_secret(&config, "token").unwrap(), "two");
    }

    #[test]
    fn matches_the_whole_file_name_only() {
        let dir = tempfile::tempdir().unwrap();
        let config = configuration(&dir, &[("my-token.json", "\"x\"")]);
        let error = read_secret(&config, "token").unwrap_err();
        assert!(matches!(
            error,
            Error::Secret {
                reason: SecretError::NotDeclared,
                ..
            }
        ));
    }

    #[test]
    fn errors_name_the_secret_never_the_value() {
        let dir = tempfile::tempdir().unwrap();
        for contents in [
            format!("{{\"value\": \"{VALUE}\"}}"),
            VALUE.to_string(),
            format!("\"{VALUE}"),
        ] {
            let config = configuration(&dir, &[("device-password.json", &contents)]);
            let error = read_secret(&config, "device-password").unwrap_err();
            let message = error.to_string();
            assert_eq!(message, "secret device-password is not a JSON string");
            assert!(!format!("{error:?}").contains(VALUE));
            assert!(check_secrets(&config).is_err());
        }
    }

    #[test]
    fn a_missing_file_is_unreadable() {
        let config = Configuration {
            secret_files: vec!["/nonexistent/dir/api-key.json".into()],
            ..Default::default()
        };
        let error = check_secrets(&config).unwrap_err();
        assert_eq!(error.to_string(), "secret api-key is missing or unreadable");
    }

    #[test]
    fn lists_the_declared_names() {
        let configuration = Configuration {
            secret_files: vec!["/v/device-password.json".into(), "/v/api-key.json".into()],
            ..Default::default()
        };
        assert_eq!(secret_names(&configuration), ["device-password", "api-key"]);
        assert!(secret_names(&Configuration::default()).is_empty());
    }

    #[test]
    fn names_come_from_the_file() {
        assert_eq!(
            secret_name("/vault/material/device-password.json"),
            "device-password"
        );
        assert_eq!(secret_name("plain"), "plain");
    }
}
