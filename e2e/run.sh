#!/usr/bin/env bash
# End-to-end test: runs the example module (examples/minimal), then the alarm-monitor sample
# (samples/alarm-monitor), against a throwaway MQTT broker and module twin server on a private
# Docker network (no route out), and checks what the SDK and the sample promise.
# Docker only; safe to re-run: every container, network, image and temp file it makes is removed
# on exit.
#
#   e2e/run.sh
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
MODULE_ID=my-module
OPENSSL_IMAGE=alpine:3.23@sha256:ff7f0c727dd796aa36787bfcd9a64bf35a6ce72a73451b3359fde849b6703d63
PYTHON_IMAGE=python:3.12-alpine@sha256:5789aa194f757888f41fa3d48e14b6feaa1b57c0af0b56fcb1372d6474d3bff0
BROKER_IMAGE=eclipse-mosquitto:2@sha256:860aeb5d63ce509508f6dcfb2bc8a1c1dd2bc60555b562b45ce90e52d8dabb0f
WORK=$(mktemp -d)
# Per-run names: two runs at once never touch each other's containers, network or images.
PREFIX=sdk-rs-e2e-$(basename "$WORK" | tr -c 'a-zA-Z0-9\n' '-' | tr 'A-Z' 'a-z')
NETWORK=$PREFIX-net
CHECKER_IMAGE=$PREFIX-checker:test
MODULE_IMAGE=$PREFIX-module:test
ALARM_IMAGE=$PREFIX-alarm-monitor:test

# Everything a run makes carries its label, so cleanup finds it all, even after an interrupt.
LABEL=superset-module-sdk-e2e=$PREFIX
remove() {
  docker ps -aq --filter "label=$LABEL" | xargs -r docker rm -f >/dev/null 2>&1 || true
  docker network ls -q --filter "label=$LABEL" | xargs -r docker network rm >/dev/null 2>&1 || true
  docker images -q --filter "label=$LABEL" | sort -u | xargs -r docker rmi -f >/dev/null 2>&1 || true
}
CLEANUP_PASSES=5
cleanup() {
  trap '' INT TERM # finish cleaning up, whatever arrives now
  # A `docker run` killed mid-create can leave its container behind after a first pass.
  for _ in $(seq "$CLEANUP_PASSES"); do
    remove
    [ -z "$(docker ps -aq --filter "label=$LABEL")" ] && break
    sleep 1
  done
  rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "== certificates (throwaway CA, broker, module twin server, module, checker)"
mkdir -p "$WORK/certs"
docker run --rm --label "$LABEL" -v "$WORK/certs:/w" -w /w "$OPENSSL_IMAGE" sh -euc '
  apk add -q --no-cache openssl >/dev/null
  key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1.key" 2>/dev/null; }
  key ca
  openssl req -x509 -new -key ca.key -subj /CN=e2e-ca -days 1 -out ca.crt
  leaf() { # name, common name, extensions
    key "$1"
    openssl req -new -key "$1.key" -subj "/CN=$2" -out "$1.csr"
    printf "%s\n" "$3" >"$1.ext"
    openssl x509 -req -in "$1.csr" -CA ca.crt -CAkey ca.key -days 1 -extfile "$1.ext" -out "$1.crt" 2>/dev/null
  }
  leaf broker broker "subjectAltName=DNS:broker
extendedKeyUsage=serverAuth"
  leaf twin twin "subjectAltName=DNS:twin
extendedKeyUsage=serverAuth"
  for client in '"$MODULE_ID"' '"$MODULE_ID"'-twin checker; do
    leaf "$client" "$client" "extendedKeyUsage=clientAuth"
    # One PEM per identity: certificate, chain, then key, like the files a Superset mounts.
    cat "$client.crt" ca.crt "$client.key" >"$client.pem"
  done
  chmod 644 ./*
  chown -R '"$(id -u):$(id -g)"' /w'

echo "== broker and module twin server configuration"
cat >"$WORK/mosquitto.conf" <<EOF
listener 8883
cafile /certs/ca.crt
certfile /certs/broker.crt
keyfile /certs/broker.key
require_certificate true
use_identity_as_username true
allow_anonymous false
acl_file /mosquitto/config/acl
EOF
# Default deny, like a Superset broker: the module publishes its telemetry and replies, and reads
# its commands and the one upstream module its Superset links to it.
cat >"$WORK/acl" <<EOF
user $MODULE_ID
topic write output/telemetry/tenant-a/node-a/$MODULE_ID/v
topic write output/directmethod/tenant-a/cluster-a/$MODULE_ID/res/+
topic read input/directmethod/tenant-a/cluster-a/$MODULE_ID/#
topic read output/telemetry/tenant-a/node-a/sensor/v

user checker
topic readwrite #
EOF
cat >"$WORK/module.json" <<EOF
{
  "brokerUrl": "tcps://broker:8883",
  "configuration": { "inputs": ["output/telemetry/tenant-a/node-a/sensor/v"], "secretFiles": ["/vault/material/device-password.json"] },
  "mqtt": {
    "ca": "/etc/newboot/ca/mqtt-ca.crt",
    "identity": "/vault/material/mqtt-client.pem",
    "mode": "mutual-tls"
  },
  "target": {
    "clusterId": "cluster-a",
    "moduleId": "$MODULE_ID",
    "nodeId": "node-a",
    "tenantConfigId": "tenant-a"
  },
  "twin": {
    "ca": "/etc/newboot/ca/twin-ca.crt",
    "endpoint": "https://twin/twin",
    "identity": "/vault/material/twin-client.pem"
  }
}
EOF
# One declared secret, as a Superset renders it: the value as a JSON string. No log may show it.
SECRET_VALUE="e2e-$(date +%s)-secret"
printf '"%s"' "$SECRET_VALUE" >"$WORK/device-password.json"
# The same value in two other shapes, valid JSON that is no string and no JSON at all (a parse
# error can quote the file): the module must refuse either at start, naming the secret only.
printf '{"value": "%s"}' "$SECRET_VALUE" >"$WORK/device-password-object.json"
printf '%s' "$SECRET_VALUE" >"$WORK/device-password-raw.json"
# The alarm-monitor sample: the same identity, its own secret.
sed 's|/vault/material/device-password.json|/vault/material/alert-token.json|' "$WORK/module.json" \
  >"$WORK/alarm-module.json"
ALERT_TOKEN="alert-$(date +%s)-token"
printf '"%s"' "$ALERT_TOKEN" >"$WORK/alert-token.json"
chmod 644 "$WORK/mosquitto.conf" "$WORK/acl" "$WORK/module.json" "$WORK/device-password.json" \
  "$WORK/device-password-object.json" "$WORK/device-password-raw.json" "$WORK/alarm-module.json" \
  "$WORK/alert-token.json"

echo "== images (built with network access; the test runs without it)"
docker build -q --label "$LABEL" -t "$CHECKER_IMAGE" - >/dev/null <<EOF
FROM $PYTHON_IMAGE
RUN pip install --no-cache-dir paho-mqtt==2.1.0
EOF
docker build -q --label "$LABEL" -t "$MODULE_IMAGE" -f "$ROOT/examples/minimal/Dockerfile" "$ROOT" >/dev/null
docker build -q --label "$LABEL" -t "$ALARM_IMAGE" -f "$ROOT/samples/alarm-monitor/Dockerfile" "$ROOT" >/dev/null

docker network create --internal --label "$LABEL" "$NETWORK" >/dev/null
docker run -d --label "$LABEL" --name "$PREFIX-broker" --network "$NETWORK" --network-alias broker \
  -v "$WORK/certs:/certs:ro" -v "$WORK/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro" \
  -v "$WORK/acl:/mosquitto/config/acl:ro" "$BROKER_IMAGE" >/dev/null
# $1: the twin profile ("" for the example module).
start_twin_server() {
  docker rm -f "$PREFIX-twin" >/dev/null 2>&1 || true
  docker run -d --label "$LABEL" --name "$PREFIX-twin" --network "$NETWORK" --network-alias twin \
    -v "$WORK/certs:/certs:ro" -v "$HERE/test:/test:ro" -e PYTHONDONTWRITEBYTECODE=1 \
    -e TWIN_PROFILE="$1" "$PYTHON_IMAGE" python -u /test/twin_server.py >/dev/null
}
start_twin_server ""

# The way a Superset runs a module: uid 10001, read-only root, no capabilities, its files mounted.
# $1: the secret file to mount; $2: the module config ($WORK/module.json by default); $3: the
# secret's name (device-password by default). Sets MODULE_ARGS, the `docker run` options.
module_args() {
  MODULE_ARGS=(--label "$LABEL" --network "$NETWORK"
    --read-only --user 10001 --cap-drop ALL --security-opt no-new-privileges
    -e MODULE_CONFIG=/etc/newboot/module.json
    -v "$WORK/${2:-module.json}:/etc/newboot/module.json:ro"
    -v "$WORK/certs/ca.crt:/etc/newboot/ca/mqtt-ca.crt:ro"
    -v "$WORK/certs/ca.crt:/etc/newboot/ca/twin-ca.crt:ro"
    -v "$WORK/certs/$MODULE_ID.pem:/vault/material/mqtt-client.pem:ro"
    -v "$WORK/certs/$MODULE_ID-twin.pem:/vault/material/twin-client.pem:ro"
    -v "$WORK/$1:/vault/material/${3:-device-password}.json:ro")
}

# A secret file that is not a JSON string ($1) stops the module at start, with a message naming
# the secret and never its value.
refuses_unreadable_secret() {
  local log status=0
  module_args "$1"
  log=$(timeout 60 docker run "${MODULE_ARGS[@]}" --rm --name "$PREFIX-refusal" "$MODULE_IMAGE" 2>&1) ||
    status=$?
  # A module still running when the timeout ends the client is removed here.
  docker rm -f "$PREFIX-refusal" >/dev/null 2>&1 || true
  [ "$status" -ne 0 ] && [ "$status" -ne 124 ] &&
    grep -qF 'secret device-password' <<<"$log" &&
    ! grep -qF "$SECRET_VALUE" <<<"$log"
}

# Every input the checker names must be logged by name, and nothing it sent to be dropped.
logged_inputs_match() {
  local kept name log
  log=$(docker logs "$PREFIX-module" 2>&1)
  kept=$(sed -n 's/^KEPT INPUTS: //p' "$WORK/checker.log")
  [ -n "$kept" ] || return 1
  for name in $kept; do
    grep -qx "input from $name" <<<"$log" || { echo "not logged: $name"; return 1; }
  done
  ! grep -F 'dropped-' <<<"$log"
}

echo "== example module"
module_args device-password.json
docker run -d "${MODULE_ARGS[@]}" --name "$PREFIX-module" --network-alias module "$MODULE_IMAGE" >/dev/null
if docker run --rm --label "$LABEL" --name "$PREFIX-checker" --network "$NETWORK" \
  -v "$WORK/certs:/certs:ro" -v "$HERE/test:/test:ro" -e PYTHONDONTWRITEBYTECODE=1 \
  "$CHECKER_IMAGE" python -u /test/checker.py | tee "$WORK/checker.log" &&
  logged_inputs_match &&
  # The SDK logs "mqtt: …" when its connection drops; it may not have dropped, even over the
  # checker's big messages (mosquitto honours a client's packet limit, a Superset broker not).
  ! docker logs "$PREFIX-module" 2>&1 | grep '^mqtt: ' &&
  # The module reads its secret at start and never logs it.
  ! docker logs "$PREFIX-module" 2>&1 | grep -F "$SECRET_VALUE" &&
  refuses_unreadable_secret device-password-object.json &&
  refuses_unreadable_secret device-password-raw.json; then
  echo "== example module: PASSED"
else
  echo "== E2E FAILED (example module); module log:"
  docker logs "$PREFIX-module" 2>&1 | tail -40
  exit 1
fi
docker rm -f "$PREFIX-module" >/dev/null

# One "alert would be sent" per alarm the checker raised, and never the alert token.
alerts_logged() {
  local expected log
  log=$(docker logs "$PREFIX-module" 2>&1)
  expected=$(sed -n 's/^ALERTS: //p' "$WORK/alarm-checker.log")
  [ -n "$expected" ] && [ "$(grep -c 'alert would be sent' <<<"$log")" -eq "$expected" ] &&
    ! grep -F "$ALERT_TOKEN" <<<"$log"
}

echo "== alarm-monitor sample"
start_twin_server alarm-monitor
module_args alert-token.json alarm-module.json alert-token
docker run -d "${MODULE_ARGS[@]}" --name "$PREFIX-module" --network-alias module "$ALARM_IMAGE" >/dev/null
if docker run --rm --label "$LABEL" --name "$PREFIX-checker" --network "$NETWORK" \
  -v "$WORK/certs:/certs:ro" -v "$HERE/test:/test:ro" -e PYTHONDONTWRITEBYTECODE=1 \
  "$CHECKER_IMAGE" python -u /test/alarm_checker.py | tee "$WORK/alarm-checker.log" &&
  alerts_logged &&
  ! docker logs "$PREFIX-module" 2>&1 | grep '^mqtt: '; then
  echo "== alarm-monitor sample: PASSED"
  echo "== E2E PASSED"
else
  echo "== E2E FAILED (alarm-monitor sample); module log:"
  docker logs "$PREFIX-module" 2>&1 | tail -40
  exit 1
fi
