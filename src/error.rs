//! The one error type of the SDK.
use std::fmt;

/// Why an operation failed, the base error of the SDK: a config, secret, twin or telemetry error
/// (the same set as the TypeScript and Python SDKs), or use after close. Messages never quote a secret's value or a file's contents.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// `module.json` is missing, unreadable or not what a Superset renders.
    Config(String),
    /// A config error: `module.json` asks for an MQTT mode this release does not support.
    UnsupportedMode(String),
    /// A secret is not declared, cannot be read or is not a JSON string. Holds its name only.
    Secret {
        /// The secret's name.
        name: String,
        /// Why it could not be read.
        reason: SecretError,
    },
    /// The module twin is not configured, could not be fetched, or is not this module's.
    Twin(TwinError),
    /// A reading breaks the payload rules; the message names the field, never its value.
    Telemetry(&'static str),
    /// [`connect`](crate::ModuleClient::connect) was called after
    /// [`close`](crate::ModuleClient::close): create a new client.
    Closed,
}

/// Why a secret could not be read. Never carries the value or a parser message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretError {
    /// No file of `configuration.secretFiles` is named `<name>.json`.
    NotDeclared,
    /// The file is missing or unreadable.
    Unreadable,
    /// The file is not a JSON string.
    NotAString,
}

/// Why the module twin could not be fetched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TwinError {
    /// The module was deployed without a module twin (`twin` absent or `null`).
    NotConfigured,
    /// A certificate file, the connection, the status, the size or the shape of the answer.
    Failed(String),
}

impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotDeclared => "is not declared",
            Self::Unreadable => "is missing or unreadable",
            Self::NotAString => "is not a JSON string",
        })
    }
}

impl fmt::Display for TwinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotConfigured => f.write_str("this module has no module twin"),
            Self::Failed(why) => write!(f, "module twin: {why}"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(why) => write!(f, "module config: {why}"),
            Self::UnsupportedMode(mode) => write!(
                f,
                "this SDK connects with {} only, not {mode}",
                crate::config::MUTUAL_TLS
            ),
            Self::Secret { name, reason } => write!(f, "secret {name} {reason}"),
            Self::Twin(error) => write!(f, "{error}"),
            Self::Telemetry(why) => write!(f, "telemetry: {why}"),
            Self::Closed => f.write_str("the client was closed: create a new one"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    pub(crate) fn twin(why: impl Into<String>) -> Self {
        Self::Twin(TwinError::Failed(why.into()))
    }
}

/// The SDK's result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_error_reads_as_one_line() {
        let cases = [
            (Error::Config("x".into()), "module config: x"),
            (
                Error::UnsupportedMode("token".into()),
                "this SDK connects with mutual-tls only, not token",
            ),
            (
                Error::Twin(TwinError::NotConfigured),
                "this module has no module twin",
            ),
            (Error::twin("HTTP 500"), "module twin: HTTP 500"),
            (
                Error::Secret {
                    name: "api-key".into(),
                    reason: SecretError::NotDeclared,
                },
                "secret api-key is not declared",
            ),
            (Error::Telemetry("bad"), "telemetry: bad"),
            (Error::Closed, "the client was closed: create a new one"),
        ];
        for (error, shown) in cases {
            assert_eq!(error.to_string(), shown);
        }
    }
}
