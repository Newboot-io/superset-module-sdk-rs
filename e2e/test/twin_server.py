"""A stand-in for the module twin endpoint: HTTPS with client certificates, one module per
certificate. At start it serves one variable; a reload gets another, named after the reload. The
correlation ids in fixtures.py select the failing answers."""

import json
import os
import ssl
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fixtures import (
    ALARM_COUNT_VARIABLE,
    ALARM_LIMITS,
    ALARM_PROFILE,
    ALARM_RELOADED_LIMITS,
    ALARM_VARIABLE,
    ARRAY_RELOAD,
    CUT_DECLARED_BYTES,
    CUT_RELOAD,
    CUT_SENT_BYTES,
    DEEP_LEVELS,
    DEEP_RELOAD,
    DRIP_BODY_RELOAD,
    DRIP_HEADERS_RELOAD,
    DRIP_SECONDS,
    FAILING_RELOAD,
    MODULE,
    REDIRECT_RELOAD,
    RELOADED_VARIABLE,
    SHORT_MISSING_BYTES,
    SHORT_RELOAD,
    STARTUP_VARIABLE,
    STARTUP_VARIABLE_NAME,
    TWIN_CLIENT,
    TWIN_PATH,
    reloaded_name,
)

MODEL_ID = "dtmi:example:Variable;1"


PROFILE = os.environ.get("TWIN_PROFILE", "")


def alarm_monitor(correlation_id):
    """The alarm-monitor sample's twin: its limits (lowered by a reload) and its two variables."""
    variables = {
        ALARM_COUNT_VARIABLE: {"modelId": MODEL_ID, "name": "alarm_count"},
        ALARM_VARIABLE: {"modelId": MODEL_ID, "name": "alarm"},
    }
    limits = ALARM_RELOADED_LIMITS if correlation_id else ALARM_LIMITS
    return {**limits, "variables": variables}


def settings(correlation_id):
    if PROFILE == ALARM_PROFILE:
        return json.dumps({"modules": {MODULE: alarm_monitor(correlation_id)}}).encode()
    variable, name = (
        (RELOADED_VARIABLE, reloaded_name(correlation_id))
        if correlation_id
        else (STARTUP_VARIABLE, STARTUP_VARIABLE_NAME)
    )
    module = {"serverport": 4840, "variables": {variable: {"modelId": MODEL_ID, "name": name}}}
    return json.dumps({"modules": {MODULE: module}, "path": "/site/scope"}).encode()


class Twin(BaseHTTPRequestHandler):
    def answer(self, status, body=b"", length=None):
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body) if length is None else length))
        self.end_headers()
        self.wfile.write(body)

    def drip(self, data):
        """Write a valid answer a byte at a time over DRIP_SECONDS: only a module without a total
        deadline would wait for it (and then answer reloaded within the command's 30 s)."""
        try:
            for byte in data:
                self.wfile.write(bytes([byte]))
                self.wfile.flush()
                time.sleep(DRIP_SECONDS / len(data))
        except OSError:
            pass  # the module gave up: what it should do

    def do_GET(self):
        subject = dict(field[0] for field in self.connection.getpeercert()["subject"])
        if self.path != TWIN_PATH or subject.get("commonName") != TWIN_CLIENT:
            return self.answer(403)
        correlation_id = self.headers.get("x-newboot-correlation-id")
        print(f"twin: GET for {correlation_id or 'startup'}", flush=True)
        if correlation_id == FAILING_RELOAD:
            return self.answer(500)
        if correlation_id == CUT_RELOAD:
            # Promise more than is sent; the connection then closes when the handler returns.
            return self.answer(200, b"{" * CUT_SENT_BYTES, length=CUT_DECLARED_BYTES)
        if correlation_id == ARRAY_RELOAD:
            return self.answer(200, b"[1]")
        if correlation_id == REDIRECT_RELOAD:
            self.send_response(302)
            self.send_header("location", TWIN_PATH)
            body = settings(correlation_id)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            return self.wfile.write(body)
        if correlation_id == SHORT_RELOAD:
            body = settings(correlation_id)
            self.answer(200, body, length=len(body) + SHORT_MISSING_BYTES)
            self.wfile.flush()
            self.close_connection = True
            return self.connection.unwrap()  # a clean TLS close, the body still short
        if correlation_id == DEEP_RELOAD:
            return self.answer(200, b"[" * DEEP_LEVELS + b"]" * DEEP_LEVELS)
        if correlation_id == DRIP_HEADERS_RELOAD:
            body = settings(correlation_id)
            head = f"HTTP/1.0 200 OK\r\ncontent-length: {len(body)}\r\n\r\n".encode()
            self.wfile.write(head[:1])
            return self.drip(head[1:] + body)
        if correlation_id == DRIP_BODY_RELOAD:
            body = settings(correlation_id)
            self.send_response(200)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.flush()
            return self.drip(body)
        self.answer(200, settings(correlation_id))


context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.verify_mode = ssl.CERT_REQUIRED
context.load_verify_locations("/certs/ca.crt")
context.load_cert_chain("/certs/twin.crt", "/certs/twin.key")
server = ThreadingHTTPServer(("", 443), Twin)
server.daemon_threads = True
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
