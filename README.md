# superset-module-sdk (Rust)

Write a module for a Newboot Superset in Rust. The SDK reads the `module.json` a Superset mounts
into every module and gives you its four operations without re-implementing the wire contract:

- **fetch the module twin**: the fields saved in the console and the module's variables;
- **read a secret** by name;
- **send telemetry** on the module's own topic;
- **receive telemetry** from the modules your Superset links to this one;

and it answers the Superset's **`reload`** command for you (the module twin changed: fetch it again).

The same API exists for TypeScript and Python:
[superset-module-sdk-ts](https://github.com/Newboot-io/superset-module-sdk-ts) and
[superset-module-sdk-py](https://github.com/Newboot-io/superset-module-sdk-py). This repository is
[superset-module-sdk-rs](https://github.com/Newboot-io/superset-module-sdk-rs).

## Requirements

- Rust 1.85 or later, edition 2021.
- A [Tokio](https://tokio.rs) multi-thread runtime (the MQTT connection and reloads run on it).
- A Superset that deploys the module with `mqtt.mode` `mutual-tls` (see [Compatibility](#compatibility)).

TLS uses [rustls](https://github.com/rustls/rustls) with the `ring` provider; no OpenSSL. MQTT 5 is
[rumqttc](https://github.com/bytebeamio/rumqtt), HTTPS is [reqwest](https://github.com/seanmonstar/reqwest).

## Install

From GitHub for now (crates.io later), at the release tag:

```toml
[dependencies]
superset-module-sdk = { git = "https://github.com/Newboot-io/superset-module-sdk-rs", tag = "v0.1.0" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "time"] }
```

## Quick start

```rust,no_run
use std::sync::{Arc, Mutex};
use std::time::Duration;
use superset_module_sdk::{ModuleClient, Telemetry};

/// Read your machine or sensor here.
fn read_sensor(_variable_id: &str) -> f64 {
    21.5
}

#[tokio::main]
async fn main() -> superset_module_sdk::Result<()> {
    let client = ModuleClient::from_env()?; // MODULE_CONFIG or /etc/newboot/module.json
    client.check_secrets()?; // stop at start when a declared secret is missing
    let twin = Arc::new(Mutex::new(client.get_twin(None).await?));

    client.on_telemetry(|input| eprintln!("input from {}", input.variable_id));
    // A `reload` from the Superset fetches the twin again and calls this before answering.
    let changed = twin.clone();
    client.on_twin_change(move |new| *changed.lock().unwrap() = new.clone());
    client.connect().await?;

    // Send every 5 s once connected: send_telemetry returns Ok(false) and queues nothing
    // while the client is not connected (before the first connection, while reconnecting).
    let mut every = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = every.tick() => {
                if !client.is_connected() {
                    continue;
                }
                let twin = twin.lock().unwrap();
                for variable_id in twin.variable_ids() {
                    // No name given: the SDK uses the variable's name from the twin.
                    let reading = Telemetry::new(variable_id, read_sensor(variable_id));
                    client.send_telemetry(reading)?;
                }
            }
        }
    }
    client.close().await;
    Ok(())
}
```

[`examples/minimal`](examples/minimal) is a complete module: it sends one reading per variable every
5 s, logs linked inputs by name only, follows reloads, and serves `/healthz` and `/readyz` on port
8080. Build its image from the repository root:

```sh
docker build -f examples/minimal/Dockerfile -t minimal-module .
```

## Samples

[`samples/alarm-monitor`](samples/alarm-monitor) is a sample module with real logic: it raises an
alarm when the smoothed temperature from a linked module leaves the band its twin sets. It is the
last stage of a three-module pipeline built with the three SDKs: `sensor-sim` (TypeScript), then
`smoother` (Python), then `alarm-monitor` (Rust). Its README describes the settings, variables,
secret and how to run it.

## API

Everything is re-exported from the crate root. Errors are one enum, `Error`, with the same set as
the TypeScript and Python SDKs: `Config` (with `UnsupportedMode` for an unsupported `mqtt.mode`),
`Secret`, `Twin` and `Telemetry`, plus `Closed` for `connect()` after `close()`. `Error` is the
base type (the other SDKs' `SdkError`). Messages never quote a secret's value or a file's contents.

### `ModuleClient::from_env() -> Result<ModuleClient>`

Reads the file named by `MODULE_CONFIG` (default `/etc/newboot/module.json`) and validates it.
`ModuleClient::new(ModuleConfig)` takes a config you parsed yourself (`ModuleConfig::from_path`,
`ModuleConfig::from_slice`).

Errors: `Error::UnsupportedMode` when `mqtt.mode` is not the string `mutual-tls` (missing or not a
string included). The mode is checked first,
on the raw JSON, so a config rendered for another mode (other fields, no `identity`) reports the
mode, not a missing field. `Error::Config` for anything else that is not what a Superset renders:

- the file is unreadable, not JSON, or a field is missing;
- `brokerUrl`, matched on the raw string (no trimming or normalisation), is not exactly
  `tcps://host[:port]` or `mqtts://host[:port]`: host `[A-Za-z0-9.-]+`
  or a bracketed IPv6 literal such as `[::1]`; port 1 to 5 digits, 1-65535, 8883 when absent
  (`host:` with no port is refused); no user info, path, query or fragment;
- `mqtt.ca`, `mqtt.identity`, `twin.ca`, `twin.identity` or a `secretFiles` entry is an empty path;
- a `target` id is empty, over 200 bytes, or holds `/`, `+`, `#` or a control character;
- a `configuration.inputs` topic is empty or holds `+`, `#` or a control character;
- a `configuration.secretFiles` entry does not end in `.json`;
- the twin endpoint is not `https://`. `"twin": null` means no twin, like an absent `twin`.

`config()` and `target()` give the parsed config and the module's identity (cluster, module, node and
tenant configuration ids). `config().configuration.other` holds your module's own configuration fields.

### `get_twin(correlation_id: Option<&str>).await -> Result<ModuleTwin>`

`GET` on `twin.endpoint` over HTTPS, with the `twin.identity` client certificate (one PEM file:
certificate, chain, key) and only the `twin.ca` bundle trusted. Both files are read again on every
call, so rotated certificates are picked up. A correlation id made of letters, digits and `-` is
sent in the `x-newboot-correlation-id` header (others are not sent). The endpoint is requested with
its path (`;params` included) and query string as given. One 12 s deadline covers the
whole request; redirects are not followed; answers above 2 MiB are refused. The answer must hold
exactly one module, this one. The SDK keeps the last twin fetched (here or by a reload) to name
the readings you send.

Returns this module's twin exactly as received, a `ModuleTwin` wrapping a
`serde_json::Map<String, Value>`: `get(key)` reads one field, `variables()` the optional variables
object (by variable id); `variable_ids()`, `variable_name(id)` (its `name`, or the id),
`as_map()` and `into_map()` help further.

Errors: `Error::Twin(TwinError::NotConfigured)` when the module was deployed without a module twin;
`Error::Twin(TwinError::Failed(..))` when a certificate file cannot be read or is corrupt, or on
TLS, timeout (the message carries the cause chain, without the URL), a status other than 200, a body too large or cut short, an answer that is not JSON
(a non-finite number anywhere included), or one that is not this module's.

### `get_secret(name) -> Result<String>`, `secret_names()` and `check_secrets()`

`get_secret` finds `<name>.json` among `configuration.secretFiles`, reads it (again on every call:
a new value replaces the file while the module runs) and parses it as a JSON string.
`secret_names()` lists the declared secrets. `check_secrets()` reads every declared secret once;
call it at start so a module without its secrets stops early.

Errors: `Error::Secret { name, reason }` with `SecretError::NotDeclared`, `Unreadable` or
`NotAString`, naming the first secret that fails. The message names the secret only, never its
value nor the parser's message (which could quote the file).

### `connect().await -> Result<()>`

Starts the MQTT 5 connection over TLS to `brokerUrl` with `mqtt.identity` and `mqtt.ca`, on the
current Tokio runtime, and returns at once: it does not wait for the connection. Client id:
`<moduleId>-<host name>`, the host name being `HOSTNAME`, or the kernel's host name
(`/proc/sys/kernel/hostname`) when `HOSTNAME` is unset or empty. On every connection it subscribes,
at QoS 1 without retained messages, to `configuration.inputs` and to the module's command topic; a
refused subscription logs `mqtt: subscription refused` and leaves the client not ready. It never
fails because of the certificate files: when they are missing or unreadable, or the connection
drops (a DISCONNECT the MQTT library sends on a protocol error included), it logs `mqtt: <reason>`
to stderr, waits 5 s, reads the files again and retries; only `close()` stops it. Readiness is
`is_connected()`.

A second `connect()` while started is a no-op and returns `Ok(())`. A `connect()` racing a
`close()` either starts first and is stopped by it, or fails with `Error::Closed`.

Errors: `Error::Closed` when called after `close()`: create a new client instead.

### `is_connected() -> bool`

True once connected **and** every subscription is granted; false before `connect()`, while
reconnecting and after `close()`. This is what a module's `/readyz` should answer. Health endpoints
are the module's job: the example serves `/healthz` and `/readyz` on port 8080.

### `send_telemetry(Telemetry) -> Result<bool>`

Publishes `{"name"?, "timestamp", "value", "variableId"}` on
`output/telemetry/<tenantConfigId>/<nodeId>/<moduleId>/v`, QoS 0, not retained.
`Telemetry::new(variable_id, value)`, plus `.with_name(..)` and `.with_timestamp(ms)` (default: now).
Without a name, the reading takes its variable's `name` from the last fetched twin, or has none.
`variableId` should be a key of `variables` in the module twin.

Returns `Ok(true)` when the reading was handed to the connected client, `Ok(false)` when the client
is not connected (before `connect()`, while reconnecting, after `close()`): nothing is queued while
disconnected, so send again on your next round.

Errors: `Error::Telemetry` naming the field:

- `variableId` empty, over 200 bytes, or holding `/`, `+`, `#` or a control character;
- `value` not a boolean, a number finite as a double, a string, or an array of those (no object,
  no nested array);
- `timestamp` above 2^53-1 ms.

### `on_telemetry(handler)`

Adds `handler: Fn(&InputTelemetry)`; several handlers may be added and run in the order they were
added. Each valid message from a linked module carries `topic`, `variable_id`, `value`, and the
sender's `name` and `timestamp` when present. Received messages follow the same payload rules as
sent ones; dropped: retained messages, messages over 8 KiB, non-JSON (including `NaN` and numbers
too large for a double), a missing or invalid `variableId`, a nested `value` (an object, an array of
arrays), a `name` that is not a string, a `timestamp` that is not an integer from 0 to 2^53-1 (an
integral number such as `1.7e12` counts). Handlers run on the connection's task: keep them short.

### `on_twin_change(handler)` and the `reload` command

Adds `handler: Fn(&ModuleTwin)`; several handlers run in the order they were added. When the
Superset sends this module `reload` (the module twin changed), the SDK fetches the twin again with
the command's correlation id, calls every handler with it, then answers
`{"result": {"reloaded": true}}`. If the fetch fails, or a handler panics (the handlers after it
are skipped), it answers `{"error": "reload_failed"}` and logs `reload: <reason>`; a payload that
is not an object answers `{"error": "invalid_payload"}`. Reloads run one at a time. The answer goes
to the command's reply topic and holds exactly `correlationId`, `target` and the result or error.
Answers are sent only while connected and subscribed, never queued for later.

A command is answered only when it is addressed to this module (topic, reply topic and target all
match), at most 8 KiB, not retained, issued at most 1 s in the future, valid at most 30 s and not
expired; no answer is sent once it expired.

**Other commands** (such as `getValue` or your own) are not handled in this release: the SDK ignores
them and sends no answer.

### Handlers that panic

A panicking handler never stops the client: a telemetry handler's panic is logged as
`telemetry handler panicked` and the next handler runs; a twin-change handler's panic answers the
reload `reload_failed`. The SDK logs that a handler panicked, never the panic message, which may
quote values. Rust's default panic hook, however, prints the panic message to stderr: if your
handlers can panic with values in the message, install a quiet hook at start, for example

```rust,no_run
std::panic::set_hook(Box::new(|info| {
    let at = info.location().map(|l| format!(" at {}:{}", l.file(), l.line()));
    eprintln!("panicked{}", at.unwrap_or_default());
}));
```

### `close().await`

Disconnects and stops the connection's task. `is_connected()` is false afterwards; `connect()`
then fails with `Error::Closed` (create a new client).

## Security notes

- Certificates and keys are read from the files the Superset mounts, again before every connection
  and every twin fetch: rotated certificates are used without a restart.
- Only the CA bundles named in `module.json` are trusted; the system roots are not.
- Secrets are read on demand and never logged. Error messages name the secret, never its value.
- What a module may publish and read is decided by the broker's per-module access rules, which come
  from the Superset; the SDK only uses the module's own topics and its configured inputs.
- Run the module as the example's Dockerfile does: non-root user `10001`, read-only root file system.

## Compatibility

| SDK   | `mqtt.mode`  | MQTT | Rust  |
|-------|--------------|------|-------|
| 0.1.x | `mutual-tls` | 5    | 1.85+ |

Any other `mqtt.mode` is refused by `from_env()` with `Error::UnsupportedMode`.

**Maximum packet size.** The client announces a maximum packet size of 2 MiB + 1 KiB (2098176
bytes) to the broker on CONNECT, so a Superset broker never sends it more. In this SDK the MQTT
library also checks each incoming packet's size from its header and refuses a bigger one before
reading it. The TypeScript and Python MQTT libraries enforce the limit only after a packet is read,
so those SDKs rely on the broker (trusted, reached over mutual TLS) honouring it.

## Testing

Unit and integration tests for the SDK, the example and the sample (no network: an in-process HTTPS server with a throwaway CA stands in
for the module twin endpoint):

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
cargo audit                                                   # honours .cargo/audit.toml
cargo llvm-cov --package superset-module-sdk --summary-only   # coverage, needs cargo-llvm-cov
```

End-to-end, Docker only: builds the example module, runs it the way a Superset does (uid 10001,
read-only root, no capabilities) against a Mosquitto broker with default-deny access rules and a
module twin server, on an internal Docker network with a throwaway CA, then checks telemetry,
linked inputs, health probes, reloads (including failing and slow twin answers) and silence for
every command the SDK must not answer. It then runs the alarm-monitor sample the same way and checks
its alarms. Everything it creates is labelled and removed on exit.

```sh
e2e/run.sh
```

## Contributing

Issues and pull requests are welcome. Keep changes small, add tests with them, and make sure
`cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo fmt --all --check`, `cargo audit` and
`e2e/run.sh` pass. The minimum supported Rust version (1.85) is checked in CI.
The API stays aligned with the TypeScript and Python SDKs: propose contract changes for all three.

## License

Apache-2.0: see [LICENSE](LICENSE).
