//! Write a module for a Newboot Superset.
//!
//! A module reads its module twin, its secrets, sends telemetry on its own topic and receives
//! telemetry from the modules its Superset links to it. [`ModuleClient`] does all of it from the
//! `module.json` the Superset mounts:
//!
//! ```no_run
//! # async fn run() -> superset_module_sdk::Result<()> {
//! use superset_module_sdk::{ModuleClient, Telemetry};
//!
//! let client = ModuleClient::from_env()?;
//! client.check_secrets()?;
//! let twin = client.get_twin(None).await?;
//! client.on_telemetry(|input| eprintln!("input from {}", input.variable_id));
//! client.connect().await?;
//! // Later, on a timer: Ok(false) while not connected, nothing queued.
//! for variable_id in twin.variable_ids() {
//!     client.send_telemetry(Telemetry::new(variable_id, 21.5))?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The README's quick start is a complete module and is compiled with the doc tests.
//!
//! The SDK answers the Superset's `reload` command itself; other commands are left unanswered
//! in this release.
//!
//! [`ModuleClient::metrics`] renders the module's [flow observations](flow) for the Newboot
//! console: serve it on `GET /metrics` (port 8080) with [`METRICS_CONTENT_TYPE`].
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod client;
pub mod command;
pub mod config;
mod error;
pub mod flow;
pub mod secrets;
pub mod telemetry;
pub mod topics;
pub mod twin;

pub use client::{ModuleClient, MAX_PACKET_BYTES, RECONNECT_AFTER};
pub use config::ModuleConfig;
pub use error::{Error, Result, SecretError, TwinError};
pub use flow::{Flow, FlowOperation, METRICS_CONTENT_TYPE};
pub use telemetry::{InputTelemetry, Telemetry};
pub use twin::ModuleTwin;

/// Compiles the README's code blocks with the doc tests.
#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
