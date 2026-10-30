# alarm-monitor (sample module)

A Newboot Superset module written with the Rust SDK. It watches the smoothed temperature a linked
module sends (`temp_avg`) and raises an alarm while it is outside the band its module twin sets.
On every `temp_avg` it receives it sends two readings: `alarm` (0 or 1) and `alarm_count` (how many
times the alarm went from 0 to 1 since the module started). Each time the alarm is raised it logs
`alert would be sent`: a real module would call a webhook with its `alert-token` secret here.

It uses only the SDK's public API and serves `/healthz` and `/readyz` (ready once connected and
subscribed) on port 8080.

## The sample pipeline

alarm-monitor is the last of three sample modules, one per SDK, linked in one Superset:

```text
sensor-sim (TypeScript) --temperature, pressure--> smoother (Python) --temp_avg, temp_f--> alarm-monitor (Rust) --alarm, alarm_count--> cloud
```

- [sensor-sim](https://github.com/Newboot-io/superset-module-sdk-ts/tree/main/samples/sensor-sim):
  simulates a temperature and a pressure sensor.
- [smoother](https://github.com/Newboot-io/superset-module-sdk-py/tree/main/samples/smoother): sends the
  moving average of the temperature (`temp_avg`) and its value in °F (`temp_f`).
- [alarm-monitor](https://github.com/Newboot-io/superset-module-sdk-rs/tree/main/samples/alarm-monitor):
  this module.

In the Superset, link alarm-monitor's input to the smoother. Linked messages are matched by their
`name` (`temp_avg`), not by variable id: `temp_f` and any other reading are ignored, and so is a
`temp_avg` whose value is not a number.

## Module twin settings

Read at start with `get_twin` and applied again on every reload (`on_twin_change`).

| Field       | Type   | Default | Meaning                                  |
|-------------|--------|---------|------------------------------------------|
| `highLimit` | number | 30      | Alarm while `temp_avg` is above it.      |
| `lowLimit`  | number | 10      | Alarm while `temp_avg` is below it.      |

A missing or invalid field falls back to its default, and the module logs the field's name. A
`lowLimit` that is not below `highLimit` resets both to their defaults. The limits themselves are
inside the band.

## Variables

The module finds its own variables by their `name` among the twin's `variables`; their ids are
whatever the Superset assigns. A variable missing from the twin is not sent (the module logs it).

| Name          | Value                                             |
|---------------|---------------------------------------------------|
| `alarm`       | 1 while the last `temp_avg` is outside the band, else 0 |
| `alarm_count` | number of 0 to 1 transitions since start          |

## Secrets

| Name          | Use                                                                 |
|---------------|---------------------------------------------------------------------|
| `alert-token` | Would authenticate the alert webhook. Checked at start (it must be declared and not empty); never printed. |

The module stops at start, naming the secret, when it is missing, unreadable or empty.

## Build

From the repository root (the image is linux/amd64, runs as uid 10001 and works on a read-only
root file system):

```sh
docker build -f samples/alarm-monitor/Dockerfile -t alarm-monitor .
```

In this repository the sample builds against the SDK next to it (a path dependency). In your own
module, depend on the release:

```toml
superset-module-sdk = { git = "https://github.com/Newboot-io/superset-module-sdk-rs", tag = "v0.1.0" }
```

## Test

The alarm state machine and the twin reading are unit tested:

```sh
cargo test -p alarm-monitor
```

The SDK's end-to-end test runs this sample too, after the example module: `e2e/run.sh` starts it
against a throwaway broker and module twin server (its twin profile `alarm-monitor`: limits 10..30,
lowered to 25 by a reload), sends it `temp_avg` readings as the smoother would, and checks its
`alarm` and `alarm_count` readings, its silence on other inputs, the limits after a reload, one
`alert would be sent` per alarm, and that the alert token never appears in its log.

## Run it in a Superset

Push the image to a registry your Superset can pull from, add the module with the two variables
named `alarm` and `alarm_count`, the twin settings above and the secret `alert-token`, then link its
input to the smoother sample. The Superset mounts the module's `module.json`, certificates and
secret; the module needs nothing else.
