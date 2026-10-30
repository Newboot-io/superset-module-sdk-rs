"""Drives the running alarm-monitor sample: sends it smoothed temperatures (`temp_avg`) as the
smoother sample would and checks its `alarm` and `alarm_count` readings, its silence on other
inputs, and its limits after a reload.

e2e/run.sh then checks the module's log: one "alert would be sent" per raised alarm (the last line
this script prints before ALL CHECKS PASSED gives the count) and never the alert token."""

import json
import os
import queue
import ssl
import time
import urllib.request

import paho.mqtt.client as mqtt
from fixtures import ALARM_COUNT_VARIABLE, ALARM_VARIABLE, CLUSTER, MODULE, NODE, TENANT
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.subscribeoptions import SubscribeOptions

COMMAND_PREFIX = f"input/directmethod/{TENANT}/{CLUSTER}/{MODULE}"
INPUT_TOPIC = f"output/telemetry/{TENANT}/{NODE}/sensor/v"
MODULE_HTTP = "http://module:8080"
REPLY_PREFIX = f"output/directmethod/{TENANT}/{CLUSTER}/{MODULE}/res/"
SCOPE = {"clusterId": CLUSTER, "moduleId": MODULE, "tenantConfigId": TENANT}
SILENCE_S = 3
STARTUP_S = 90
TELEMETRY_TOPIC = f"output/telemetry/{TENANT}/{NODE}/{MODULE}/v"
TTL_MS = 5000
WAIT_S = 15
OWN = {ALARM_VARIABLE: "alarm", ALARM_COUNT_VARIABLE: "alarm_count"}

telemetry: "queue.Queue[dict]" = queue.Queue()
replies: "queue.Queue[dict]" = queue.Queue()


def fail(what):
    print(f"FAIL {what}", flush=True)
    os._exit(1)


def check(condition, what):
    if not condition:
        fail(what)
    print(f"PASS {what}", flush=True)


def on_message(_client, _userdata, message):
    if message.topic == TELEMETRY_TOPIC:
        telemetry.put(json.loads(message.payload))
    elif message.topic.startswith(REPLY_PREFIX):
        replies.put(json.loads(message.payload))


def http_status(path):
    try:
        with urllib.request.urlopen(MODULE_HTTP + path, timeout=2) as response:
            return response.status
    except OSError as error:
        return getattr(error, "code", None)


def send_input(name, value):
    body = {"name": name, "timestamp": int(time.time() * 1000), "value": value,
            "variableId": f"variable-{name}"}
    client.publish(INPUT_TOPIC, json.dumps(body), qos=1).wait_for_publish()


def readings():
    """The module's next alarm and alarm_count readings, by name."""
    got = {}
    end = time.monotonic() + WAIT_S
    while len(got) < len(OWN) and (left := end - time.monotonic()) > 0:
        try:
            message = telemetry.get(timeout=left)
        except queue.Empty:
            break
        name = OWN.get(message.get("variableId"))
        if name is None or message.get("name") != name:
            fail(f"unexpected telemetry: {message}")
        got[name] = message["value"]
    return got


def expect(value, alarm, count):
    send_input("temp_avg", value)
    got = readings()
    check(got == {"alarm": alarm, "alarm_count": count},
          f"temp_avg {value}: alarm {alarm}, alarm_count {count} (got {got})")


tls = ssl.create_default_context(cafile="/certs/ca.crt")
tls.load_cert_chain("/certs/checker.pem")
client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="checker", protocol=mqtt.MQTTv5)
client.tls_set_context(tls)
client.on_message = on_message
client.connect("broker", 8883)
client.subscribe([("output/#", SubscribeOptions(qos=1, retainAsPublished=True))])
client.loop_start()

end = time.monotonic() + STARTUP_S
while http_status("/readyz") != 200 and time.monotonic() < end:
    time.sleep(0.5)
check(http_status("/readyz") == 200, "GET /readyz answers 200 once subscribed")
check(http_status("/healthz") == 200, "GET /healthz answers 200")

# Limits 10..30 from the twin at start.
expect(20, 0, 0)
expect(35, 1, 1)
expect(36, 1, 1)  # still on: not a new alarm
expect(20, 0, 1)
expect(5, 1, 2)  # the low side counts too

# Other linked readings (the smoother's temp_f) and non-numbers are not watched.
send_input("temp_f", 99)
send_input("temp_avg", "hot")
time.sleep(SILENCE_S)
check(telemetry.empty(), "no reading for temp_f or a non-number temp_avg")

# A reload lowers the high limit to 25: 27 now raises an alarm.
now = int(time.time() * 1000)
command = {"command": "reload", "correlationId": "reload-1", "delivery": "unicast",
           "expiresAtUnixMs": now + TTL_MS, "issuedAtUnixMs": now, "payload": {},
           "replyTopic": REPLY_PREFIX + "reload-1", "target": SCOPE}
client.publish(f"{COMMAND_PREFIX}/unicast/reload", json.dumps(command), qos=1).wait_for_publish()
try:
    answer = replies.get(timeout=WAIT_S)
except queue.Empty:
    answer = None
check(answer and answer.get("result") == {"reloaded": True}, f"reload: {answer}")
expect(20, 0, 2)
expect(27, 1, 3)

client.loop_stop()
print("ALERTS: 3", flush=True)
print("ALL CHECKS PASSED", flush=True)
