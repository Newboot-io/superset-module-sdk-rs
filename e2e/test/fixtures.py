"""Names the end-to-end test's broker, module twin server and checker share."""

CLUSTER, MODULE, NODE, TENANT = "cluster-a", "my-module", "node-a", "tenant-a"
TWIN_CLIENT = f"{MODULE}-twin"
TWIN_PATH = "/twin"

# Console variables are named `variable-<uuid>`; the module twin lists the module's own.
STARTUP_VARIABLE = "variable-0c6e3d1a-5b0f-4f43-9a52-7f1e2d3c4b5a"
STARTUP_VARIABLE_NAME = "Boiler temperature"
RELOADED_VARIABLE = "variable-9d8c7b6a-1e2f-4a3b-8c4d-5e6f7a8b9c0d"

# The module twin server fails every reload with one of these correlation ids: an HTTP 500, an
# answer cut mid-body (the connection closes after 10 of 1000 bytes), a JSON array, a redirect
# carrying a valid twin, JSON nested too deep to parse on a small stack.
FAILING_RELOAD = "reload-fail"
CUT_RELOAD = "reload-cut"
ARRAY_RELOAD = "reload-array"
REDIRECT_RELOAD = "reload-redirect"
DEEP_RELOAD = "reload-deep"
CUT_DECLARED_BYTES = 1000
CUT_SENT_BYTES = 10
DEEP_LEVELS = 100_000
# The module twin server announces SHORT_MISSING_BYTES more than the valid twin it sends, then
# closes the connection cleanly: only the length tells the module the answer is incomplete.
SHORT_RELOAD = "reload-short"
SHORT_MISSING_BYTES = 100

# Slow answers, a valid twin written a byte at a time over DRIP_SECONDS: past the SDK's 12 s
# deadline (TWIN_DEADLINE_S) but within the command's 30 s, from the headers on or the body after
# prompt headers. The module must give up and answer reload_failed.
TWIN_DEADLINE_S = 12
DRIP_HEADERS_RELOAD = "reload-drip-headers"
DRIP_BODY_RELOAD = "reload-drip-body"
DRIP_SECONDS = TWIN_DEADLINE_S + 8


def reloaded_name(correlation_id):
    """What the module twin server names the variable it serves to a reload."""
    return f"reload-{correlation_id}"


# The alarm-monitor sample (TWIN_PROFILE=alarm-monitor on the module twin server): its two own
# variables, found by name, and its limits at start and after any reload.
ALARM_PROFILE = "alarm-monitor"
ALARM_VARIABLE = "variable-1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"
ALARM_COUNT_VARIABLE = "variable-2b3c4d5e-6f7a-4b8c-9d0e-1f2a3b4c5d6e"
ALARM_LIMITS = {"highLimit": 30, "lowLimit": 10}
ALARM_RELOADED_LIMITS = {"highLimit": 25, "lowLimit": 10}


# The common module flow contract: GET /metrics on the module's port 8080 serves exactly these 17
# series, and nothing else.
METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
FLOW_OPERATIONS = ("acquire", "receive", "process", "enqueue")
FLOW_SERIES = {
    "newboot_flow_mqtt_connected",
    *(
        f'newboot_flow_operations_total{{operation="{operation}",outcome="{outcome}"}}'
        for operation in FLOW_OPERATIONS
        for outcome in ("success", "error")
    ),
    *(
        f'newboot_flow_last_{outcome}_timestamp_seconds{{operation="{operation}"}}'
        for operation in FLOW_OPERATIONS
        for outcome in ("success", "error")
    ),
}


def flow_series(text):
    """A /metrics answer: how many series it has, and their values by name and labels."""
    lines = [line for line in text.splitlines() if line and not line.startswith("#")]
    series = (line.rsplit(" ", 1) for line in lines)
    return len(lines), {name: float(value) for name, value in series}


def operations(series, operation, outcome):
    """One operation's count for one outcome."""
    return series[f'newboot_flow_operations_total{{operation="{operation}",outcome="{outcome}"}}']
