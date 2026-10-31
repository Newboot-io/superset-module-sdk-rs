"""Drives the running example module over the test broker and asserts what the SDK promises:
telemetry per variable of the module twin, linked-module input, health probes, flow metrics,
reload answers, and silence for every command the SDK must not answer.

Linked inputs are checked from the module's log by e2e/run.sh: the last line this script prints
before ALL CHECKS PASSED lists the input names the module must log ("KEPT INPUTS: …"); every other
input sent here is named "dropped-…" and must never appear."""

import json
import os
import queue
import ssl
import time
import urllib.request

import paho.mqtt.client as mqtt
from fixtures import (
    ARRAY_RELOAD,
    CLUSTER,
    CUT_RELOAD,
    DEEP_RELOAD,
    DRIP_BODY_RELOAD,
    DRIP_HEADERS_RELOAD,
    FAILING_RELOAD,
    FLOW_OPERATIONS,
    FLOW_SERIES,
    METRICS_CONTENT_TYPE,
    MODULE,
    NODE,
    REDIRECT_RELOAD,
    RELOADED_VARIABLE,
    SHORT_RELOAD,
    STARTUP_VARIABLE,
    STARTUP_VARIABLE_NAME,
    TENANT,
    flow_series,
    operations,
    reloaded_name,
)
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.subscribeoptions import SubscribeOptions

# Above the SDK's 8 KiB limits. Mosquitto (this harness) honours the largest packet a client
# announces; a Superset broker forwards up to 2 MiB, so a module must take both sizes without
# dropping its connection.
BIG_MESSAGE_BYTES = (20 * 1024, 100 * 1024)
COMMAND_PREFIX = f"input/directmethod/{TENANT}/{CLUSTER}/{MODULE}"
COMMAND_TOPIC = f"{COMMAND_PREFIX}/unicast"
INPUT_TOPIC = f"output/telemetry/{TENANT}/{NODE}/sensor/v"
MAX_COMMAND_BYTES = 8192
MAX_SEGMENT_BYTES = 200
MAX_TTL_MS = 30_000
MODULE_HTTP = "http://module:8080"
REPLY_KEYS = 3
REPLY_PREFIX = f"output/directmethod/{TENANT}/{CLUSTER}/{MODULE}/res/"
# Longer than this whole script runs: a last-operation time within it is recent.
RUN_S = 600
SCOPE = {"clusterId": CLUSTER, "moduleId": MODULE, "tenantConfigId": TENANT}
SEND_EVERY_S = 5
SILENCE_S = 3
STARTUP_S = 90
TELEMETRY_TOPIC = f"output/telemetry/{TENANT}/{NODE}/{MODULE}/v"
TTL_MS = 5000
WAIT_S = 15

telemetry: "queue.Queue[dict]" = queue.Queue()
replies: "dict[str, queue.Queue[dict]]" = {}
# Inputs sent without the retain flag: each is one receive and one process in the flow metrics.
live_inputs = 0


def fail(what):
    print(f"FAIL {what}", flush=True)
    os._exit(1)  # also from paho's thread: a failure never turns into a timeout


def check(condition, what):
    if not condition:
        fail(what)
    print(f"PASS {what}", flush=True)


def reject_constant(name):
    raise ValueError(f"{name} is not JSON")


def on_message(_client, _userdata, message):
    """Collect what the module sends; the checker's own messages to it are skipped."""
    if message.topic != TELEMETRY_TOPIC and not message.topic.startswith(REPLY_PREFIX):
        return
    try:
        body = json.loads(message.payload, parse_constant=reject_constant)
    except ValueError:
        fail(f"non-JSON message on {message.topic}: {message.payload[:80]!r}")
    if message.qos != 0 or message.retain:
        fail(f"{message.topic} sent at QoS {message.qos}, retain {message.retain}")
    if message.topic == TELEMETRY_TOPIC:
        if set(body) != {"name", "timestamp", "value", "variableId"}:
            fail(f"telemetry has other keys than name, timestamp, value, variableId: {body}")
        telemetry.put(body)
    else:
        replies.setdefault(message.topic.removeprefix(REPLY_PREFIX), queue.Queue()).put(body)


def valid_segment(value):
    return (
        isinstance(value, str)
        and 0 < len(value.encode()) <= MAX_SEGMENT_BYTES
        and not any(c in "/+#" or ord(c) < 32 or ord(c) == 127 for c in value)
    )


def next_telemetry(deadline_s, variable_id):
    """The next telemetry message for this variable before the deadline."""
    end = time.monotonic() + deadline_s
    while (left := end - time.monotonic()) > 0:
        try:
            message = telemetry.get(timeout=left)
        except queue.Empty:
            break
        if message.get("variableId") == variable_id:
            return message
    return None


def publish(topic, payload, retain=False):
    client.publish(topic, payload, qos=1, retain=retain).wait_for_publish()


def envelope(name, correlation_id, payload, **override):
    now = int(time.time() * 1000)
    fields = {
        "command": name,
        "correlationId": correlation_id,
        "delivery": "unicast",
        "expiresAtUnixMs": now + TTL_MS,
        "issuedAtUnixMs": now,
        "payload": payload,
        "replyTopic": REPLY_PREFIX + correlation_id,
        "target": SCOPE,
    }
    fields.update(override)
    return {key: value for key, value in fields.items() if value is not None}


def command(name, correlation_id, payload, retain=False, delivery="unicast", **override):
    replies.setdefault(correlation_id, queue.Queue())
    body = envelope(name, correlation_id, payload, delivery=delivery, **override)
    publish(f"{COMMAND_PREFIX}/{delivery}/{name}", json.dumps(body), retain)
    return correlation_id


def reply_to(correlation_id, wait_s=WAIT_S):
    """The module's reply, checked against the envelope the cloud accepts."""
    try:
        answer = replies[correlation_id].get(timeout=wait_s)
    except queue.Empty:
        return None
    well_formed = (
        len(answer) == REPLY_KEYS
        and answer.get("correlationId") == correlation_id
        and answer.get("target") == SCOPE
        and (("result" in answer) != ("error" in answer))
        and ("error" not in answer or valid_segment(answer["error"]))
    )
    check(well_formed, f"reply {correlation_id} has exactly correlationId, target, result|error")
    return answer


def http_status(path):
    try:
        with urllib.request.urlopen(MODULE_HTTP + path, timeout=2) as response:
            return response.status
    except OSError as error:
        return getattr(error, "code", None)


def metrics():
    """The module's flow metrics, checked against the common contract: exactly its 17 series."""
    try:
        with urllib.request.urlopen(MODULE_HTTP + "/metrics", timeout=2) as response:
            content_type = response.headers.get("Content-Type")
            count, series = flow_series(response.read().decode())
    except OSError as error:
        fail(f"GET /metrics: {error}")
    check(content_type == METRICS_CONTENT_TYPE, "GET /metrics answers in Prometheus text format")
    check(
        count == len(FLOW_SERIES) and set(series) == FLOW_SERIES,
        "GET /metrics serves exactly the 17 flow series",
    )
    return series


def send_input(name, value, retain=False, raw=None):
    global live_inputs
    body = raw or json.dumps({"name": name, "value": value, "variableId": f"variable-{name}"})
    publish(INPUT_TOPIC, body, retain)
    live_inputs += not retain


tls = ssl.create_default_context(cafile="/certs/ca.crt")
tls.load_cert_chain("/certs/checker.pem")
client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="checker", protocol=mqtt.MQTTv5)
client.tls_set_context(tls)
client.on_message = on_message
client.connect("broker", 8883)
# Retain as published: the checker sees the flag the module set.
client.subscribe([("output/#", SubscribeOptions(qos=1, retainAsPublished=True))])
client.loop_start()

# Sending: one message per variable of the module twin fetched at start.
first = next_telemetry(STARTUP_S, STARTUP_VARIABLE)
check(first is not None, "telemetry for the variable listed in the module twin at start")
check(first.get("name") == STARTUP_VARIABLE_NAME, "telemetry is named after the variable")
check(isinstance(first.get("value"), (int, float)), "telemetry has a numeric value")
check(abs(first["timestamp"] - time.time() * 1000) < 60_000, "telemetry timestamp is epoch ms")

end = time.monotonic() + STARTUP_S
while http_status("/readyz") != 200 and time.monotonic() < end:
    time.sleep(0.5)
check(http_status("/readyz") == 200, "GET /readyz answers 200 once subscribed")
check(http_status("/healthz") == 200, "GET /healthz answers 200")
series = metrics()
check(series["newboot_flow_mqtt_connected"] == 1, "newboot_flow_mqtt_connected is 1 once ready")
check(operations(series, "enqueue", "success") > 0, "the telemetry sent counts as enqueues")

# Receiving: kept inputs reach the module's handler (which logs their names), dropped ones never.
kept = ["kept-scalar", "kept-array", "kept-text"]
send_input("kept-scalar", 1.5)
send_input("kept-array", [1, 2.5, 3.25])
send_input("kept-text", "on")
send_input("dropped-retained", 1.5, retain=True)
publish(INPUT_TOPIC, b"", retain=True)  # clear it
send_input("dropped-nested", {"a": [1]})
send_input("dropped-nested-array", [[1]])
send_input("dropped-null", None)
send_input(None, None, raw='{"name": "dropped-nan", "value": NaN, "variableId": "variable-nan"}')
send_input(None, None, raw='{"name": "dropped-huge", "value": 1e400, "variableId": "v-huge"}')
send_input(None, None, raw='{"name": "dropped-bad-id", "value": 1, "variableId": "a/b"}')
send_input(None, None, raw="not json dropped-not-json")
# Big messages on the input and command topics must not cut the module's connection (run.sh
# checks its log for reconnects), and an input over 8 KiB is dropped.
for size in BIG_MESSAGE_BYTES:
    send_input("dropped-big", "x" * size)
    publish(f"{COMMAND_TOPIC}/reload", json.dumps(envelope("reload", "big", {}, pad="x" * size)))
    answer = reply_to(command("reload", f"after-big-{size}", {}))
    check(answer and "result" in answer, f"after {size}-byte messages, reload is still answered")

# Messages that are not command objects must not stop the module.
for raw in ("5", "[1]", "null", "not json", '"text"'):
    publish(f"{COMMAND_TOPIC}/reload", raw)
answer = reply_to(command("reload", "after-junk", {}))
check(answer and answer.get("result") == {"reloaded": True}, f"after junk, reload: {answer}")
answer = reply_to(command("reload", "reload-broadcast", {}, delivery="broadcast"))
check(answer and answer.get("result") == {"reloaded": True}, f"broadcast reload: {answer}")
answer = reply_to(command("reload", "reload-bad-payload", "not an object"))
check(answer and answer.get("error") == "invalid_payload", f"invalid payload: {answer}")

# Commands the SDK must leave unanswered: other commands (out of scope in this release), and
# reloads that are not addressed to this module or not on time.
now = int(time.time() * 1000)
silent = {
    "another-target": {"target": {**SCOPE, "moduleId": "other"}},
    "expired": {"expiresAtUnixMs": now - 1, "issuedAtUnixMs": now - TTL_MS},
    "future-issue": {"expiresAtUnixMs": now + 2 * TTL_MS, "issuedAtUnixMs": now + TTL_MS},
    "long-ttl": {"expiresAtUnixMs": now + MAX_TTL_MS + TTL_MS},
    "no-deadline": {"expiresAtUnixMs": None},
    "text-deadline": {"expiresAtUnixMs": "soon"},
    "wrong-reply-topic": {"replyTopic": REPLY_PREFIX + "elsewhere"},
}
for correlation_id, override in silent.items():
    command("reload", correlation_id, {}, **override)
command("getValue", "get-value", {"variableId": STARTUP_VARIABLE})
command("ping", "custom-command", {})
command("reload", "retained", {}, retain=True)
publish(f"{COMMAND_TOPIC}/reload", b"", retain=True)  # clear it
replies.setdefault("oversized", queue.Queue())
publish(
    f"{COMMAND_TOPIC}/reload",
    json.dumps(envelope("reload", "oversized", {}, pad="x" * MAX_COMMAND_BYTES)),
)
time.sleep(SILENCE_S)
quiet = [*silent, "get-value", "custom-command", "retained", "oversized", "elsewhere", "big"]
for correlation_id in quiet:
    check(
        replies.get(correlation_id, queue.Queue()).empty(),
        f"no reply to the {correlation_id} command",
    )

# A failed fetch (HTTP 500, an answer cut mid-body, one shorter than announced, a JSON array, a
# redirect, JSON nested too deep) answers a short code and leaves the module working.
for failing in (
    FAILING_RELOAD,
    CUT_RELOAD,
    SHORT_RELOAD,
    ARRAY_RELOAD,
    REDIRECT_RELOAD,
    DEEP_RELOAD,
):
    answer = reply_to(command("reload", failing, {}))
    check(answer and answer.get("error") == "reload_failed", f"failed reload {failing}: {answer}")
# A module twin server that answers too slowly: the SDK gives up at its deadline and still
# answers within the command's longest life (30 s).
for slow in (DRIP_HEADERS_RELOAD, DRIP_BODY_RELOAD):
    issued = int(time.time() * 1000)
    longest = {"expiresAtUnixMs": issued + MAX_TTL_MS, "issuedAtUnixMs": issued}
    answer = reply_to(command("reload", slow, {}, **longest), MAX_TTL_MS / 1000)
    check(answer and answer.get("error") == "reload_failed", f"slow reload {slow}: {answer}")

# Then a reload brings the new variables, through the module's twin-change handler.
answer = reply_to(command("reload", "reload-1", {}))
check(answer and answer.get("result") == {"reloaded": True}, f"reload: {answer}")
# Earlier successful reloads (after-big-…, after-junk) served the same variable under other
# names: wait for the name this reload brought.
end = time.monotonic() + WAIT_S
after = None
while after is None and (left := end - time.monotonic()) > 0:
    message = next_telemetry(left, RELOADED_VARIABLE)
    if message is None:
        break
    if message.get("name") == reloaded_name("reload-1"):
        after = message
check(after is not None, "after reload, telemetry follows the variables fetched again")
while not telemetry.empty():
    telemetry.get()
check(
    next_telemetry(2 * SEND_EVERY_S, STARTUP_VARIABLE) is None,
    "after reload, the dropped variable is no longer sent",
)

# Flow metrics: every input sent without the retain flag is one receive and one process, a
# success for the kept ones only; the commands and reloads above are never counted.
series = metrics()
received = operations(series, "receive", "success")
check(received == live_inputs, f"{live_inputs} inputs received (got {received})")
check(operations(series, "receive", "error") == 0, "no receive error")
processed = operations(series, "process", "success")
check(processed == len(kept), f"{len(kept)} inputs processed (got {processed})")
rejected = operations(series, "process", "error")
check(rejected == live_inputs - len(kept), f"dropped inputs are process errors (got {rejected})")
check(operations(series, "enqueue", "success") > 0, "telemetry counts as enqueue successes")
check(
    all(operations(series, "acquire", outcome) == 0 for outcome in ("success", "error")),
    "acquire stays at zero: the module records it, never the SDK",
)
for operation, outcome in (("receive", "success"), ("process", "error"), ("enqueue", "success")):
    at = series[f'newboot_flow_last_{outcome}_timestamp_seconds{{operation="{operation}"}}']
    check(abs(time.time() - at) < RUN_S, f"last {operation} {outcome} is a recent Unix time")
check(
    all(series[f'newboot_flow_last_error_timestamp_seconds{{operation="{op}"}}'] == 0
        for op in FLOW_OPERATIONS if op != "process"),
    "last error is 0 for the operations that never failed",
)

client.loop_stop()
print(f"KEPT INPUTS: {' '.join(kept)}", flush=True)
print("ALL CHECKS PASSED", flush=True)
