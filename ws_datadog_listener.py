#!/usr/bin/env python3
"""
WebSocket → Datadog GovCloud Listener
Maintains a persistent WebSocket connection and forwards every push event
to Datadog GovCloud (ddog-gov.com) as custom metrics via DogStatsD (UDP)
and/or the Datadog HTTP API.

Dependencies:
    pip install websocket-client datadog datadog-api-client

Usage:
    WS_URL="wss://your-endpoint/ws" \
    DD_API_KEY="your_api_key" \
    DD_APP_KEY="your_app_key" \
    python ws_datadog_listener.py

GovCloud notes:
  - DD_SITE is hardcoded to ddog-gov.com; do NOT override to datadoghq.com.
  - HTTP API requests are routed to https://api.ddog-gov.com
  - DogStatsD still points at a local Datadog Agent (localhost:8125 by default).
    Ensure your Agent's datadog.yaml has `site: ddog-gov.com`.
  - All traffic should route through FedRAMP-compliant network paths.
    Confirm proxy/firewall rules allow outbound HTTPS to *.ddog-gov.com.
"""

import os
import json
import time
import logging
import signal
import sys
import threading
from datetime import datetime, timezone
from typing import Any

import websocket  # pip install websocket-client

# Optional: DogStatsD client (requires `datadog` package or `datadog-api-client`)
try:
    from datadog import initialize, statsd
    STATSD_AVAILABLE = True
except ImportError:
    STATSD_AVAILABLE = False

# Optional: Datadog HTTP API client
try:
    from datadog_api_client import ApiClient, Configuration
    from datadog_api_client.v2.api.metrics_api import MetricsApi
    from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
    from datadog_api_client.v2.model.metric_payload import MetricPayload
    from datadog_api_client.v2.model.metric_series import MetricSeries
    from datadog_api_client.v2.model.metric_point import MetricPoint
    from datadog_api_client.v2.model.metric_resource import MetricResource
    HTTP_API_AVAILABLE = True
except ImportError:
    HTTP_API_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------

WS_URL          = os.getenv("WS_URL", "wss://your-websocket-endpoint/ws")
DD_API_KEY      = os.getenv("DD_API_KEY", "")
DD_APP_KEY      = os.getenv("DD_APP_KEY", "")

# GovCloud: site is fixed — never change this to datadoghq.com
DD_SITE         = "ddog-gov.com"
DD_API_BASE_URL = "https://api.ddog-gov.com"   # used for HTTP API emit

STATSD_HOST     = os.getenv("STATSD_HOST", "localhost")
STATSD_PORT     = int(os.getenv("STATSD_PORT", "8125"))

# Metric names emitted to Datadog
METRIC_UPDATE   = os.getenv("DD_METRIC_UPDATE", "websocket.entity.update")
METRIC_LATENCY  = os.getenv("DD_METRIC_LATENCY", "websocket.entity.latency_ms")
METRIC_ERRORS   = os.getenv("DD_METRIC_ERRORS",  "websocket.listener.errors")

# Tag applied to every metric — add static tags here
BASE_TAGS       = [
    "source:websocket_listener",
    f"env:{os.getenv('DD_ENV', 'production')}",
    "site:govcloud",   # identifies metrics as originating from GovCloud deployment
]

# Reconnect settings
RECONNECT_INITIAL_DELAY = float(os.getenv("RECONNECT_INITIAL_DELAY", "2"))
RECONNECT_MAX_DELAY     = float(os.getenv("RECONNECT_MAX_DELAY", "60"))
RECONNECT_MULTIPLIER    = float(os.getenv("RECONNECT_MULTIPLIER", "2"))

# How to ship metrics: "statsd" | "http" | "both"
EMIT_MODE = os.getenv("DD_EMIT_MODE", "statsd" if STATSD_AVAILABLE else "http")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("ws_datadog_listener")

# ---------------------------------------------------------------------------
# Datadog emitters
# ---------------------------------------------------------------------------

def _init_statsd() -> None:
    if not STATSD_AVAILABLE:
        log.warning("datadog package not installed; StatsD unavailable. pip install datadog")
        return
    initialize(statsd_host=STATSD_HOST, statsd_port=STATSD_PORT)
    log.info("DogStatsD initialised → %s:%s", STATSD_HOST, STATSD_PORT)
    log.info(
        "GovCloud reminder: ensure your Datadog Agent datadog.yaml has `site: ddog-gov.com`"
    )


def emit_metric(name: str, value: float, tags: list[str], metric_type: str = "count") -> None:
    """
    Emit a metric to Datadog using the configured mode.
    metric_type: "count" | "gauge" | "histogram"
    """
    all_tags = BASE_TAGS + tags

    if EMIT_MODE in ("statsd", "both"):
        _emit_statsd(name, value, all_tags, metric_type)

    if EMIT_MODE in ("http", "both"):
        _emit_http(name, value, all_tags)


def _emit_statsd(name: str, value: float, tags: list[str], metric_type: str) -> None:
    if not STATSD_AVAILABLE:
        return
    try:
        fn = {"count": statsd.increment, "gauge": statsd.gauge, "histogram": statsd.histogram}.get(
            metric_type, statsd.gauge
        )
        if metric_type == "count":
            fn(name, tags=tags)
        else:
            fn(name, value, tags=tags)
    except Exception as exc:
        log.error("StatsD emit failed: %s", exc)


def _emit_http(name: str, value: float, tags: list[str]) -> None:
    if not HTTP_API_AVAILABLE:
        log.debug("datadog-api-client not installed; HTTP emit skipped.")
        return
    if not DD_API_KEY:
        log.warning("DD_API_KEY not set; HTTP emit skipped.")
        return
    try:
        cfg = Configuration()
        cfg.api_key["apiKeyAuth"] = DD_API_KEY
        if DD_APP_KEY:
            cfg.api_key["appKeyAuth"] = DD_APP_KEY

        # GovCloud: explicitly set the server URL to api.ddog-gov.com.
        # The `server_variables` approach can silently fall back to datadoghq.com
        # if the SDK version doesn't recognise the site string, so we set the
        # host directly to guarantee GovCloud routing.
        cfg.server_variables["site"] = DD_SITE
        cfg.host = DD_API_BASE_URL

        now_unix = int(time.time())

        series = MetricSeries(
            metric=name,
            type=MetricIntakeType.GAUGE,
            points=[MetricPoint(timestamp=now_unix, value=value)],
            tags=tags,
        )
        payload = MetricPayload(series=[series])

        with ApiClient(cfg) as client:
            MetricsApi(client).submit_metrics(body=payload)

        log.debug("HTTP metric emitted → %s=%s [%s]", name, value, DD_API_BASE_URL)
    except Exception as exc:
        log.error("HTTP API emit failed: %s", exc)

# ---------------------------------------------------------------------------
# Message parser — adapt this to your WebSocket message schema
# ---------------------------------------------------------------------------

def parse_message(raw: str) -> dict[str, Any]:
    """
    Parse an incoming WebSocket message into a normalised dict.

    Expected shape (customise to match your endpoint's actual payload):
        {
            "entity_id": "sensor_42",
            "value": 99.5,
            "timestamp": "2025-05-12T14:00:00Z",   # optional ISO8601
            "type": "temperature"                   # optional
        }

    Returns a dict with at least:
        entity_id, value, latency_ms, extra_tags
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Non-JSON messages: treat the raw string as the entity update
        log.debug("Non-JSON message received; using raw value")
        data = {"entity_id": "unknown", "value": 1, "raw": raw}

    entity_id = str(data.get("entity_id", data.get("id", "unknown")))
    value     = float(data.get("value", data.get("v", 1)))

    # Calculate latency if message carries a server-side timestamp
    latency_ms = None
    ts_raw = data.get("timestamp") or data.get("ts")
    if ts_raw:
        try:
            if isinstance(ts_raw, (int, float)):
                sent_at = float(ts_raw)
            else:
                sent_at = datetime.fromisoformat(
                    ts_raw.replace("Z", "+00:00")
                ).timestamp()
            latency_ms = (time.time() - sent_at) * 1000
        except Exception:
            pass

    extra_tags = [f"entity_id:{entity_id}"]
    if "type" in data:
        extra_tags.append(f"entity_type:{data['type']}")

    return {
        "entity_id": entity_id,
        "value": value,
        "latency_ms": latency_ms,
        "extra_tags": extra_tags,
        "raw": data,
    }

# ---------------------------------------------------------------------------
# WebSocket callbacks
# ---------------------------------------------------------------------------

class WebSocketListener:
    def __init__(self):
        self.ws: websocket.WebSocketApp | None = None
        self._shutdown = threading.Event()
        self._reconnect_delay = RECONNECT_INITIAL_DELAY
        self._message_count = 0
        self._error_count = 0

    # -- WebSocketApp callbacks ----------------------------------------------

    def on_open(self, ws: websocket.WebSocketApp) -> None:
        log.info("WebSocket connection opened → %s", WS_URL)
        self._reconnect_delay = RECONNECT_INITIAL_DELAY  # reset backoff on success

    def on_message(self, ws: websocket.WebSocketApp, raw: str) -> None:
        self._message_count += 1
        try:
            msg = parse_message(raw)
            log.debug(
                "Message #%d | entity=%s value=%s latency=%.1fms",
                self._message_count,
                msg["entity_id"],
                msg["value"],
                msg["latency_ms"] or -1,
            )

            # 1. Count every update per entity
            emit_metric(METRIC_UPDATE, 1, msg["extra_tags"], metric_type="count")

            # 2. Forward the numeric value as a gauge
            emit_metric(
                f"{METRIC_UPDATE}.value",
                msg["value"],
                msg["extra_tags"],
                metric_type="gauge",
            )

            # 3. Emit end-to-end latency if available
            if msg["latency_ms"] is not None:
                emit_metric(
                    METRIC_LATENCY,
                    msg["latency_ms"],
                    msg["extra_tags"],
                    metric_type="gauge",
                )

        except Exception as exc:
            self._error_count += 1
            log.error("Error processing message: %s", exc, exc_info=True)
            emit_metric(METRIC_ERRORS, 1, ["reason:processing_error"], metric_type="count")

    def on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        self._error_count += 1
        log.error("WebSocket error: %s", error)
        emit_metric(METRIC_ERRORS, 1, ["reason:ws_error"], metric_type="count")

    def on_close(self, ws: websocket.WebSocketApp, code: int, reason: str) -> None:
        log.warning("WebSocket closed (code=%s reason=%s)", code, reason)

    # -- Run loop ------------------------------------------------------------

    def run(self) -> None:
        _init_statsd()
        log.info(
            "Starting WebSocket listener | url=%s emit_mode=%s dd_endpoint=%s",
            WS_URL, EMIT_MODE, DD_API_BASE_URL
        )

        while not self._shutdown.is_set():
            self.ws = websocket.WebSocketApp(
                WS_URL,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )

            # run_forever blocks until the connection drops
            self.ws.run_forever(
                ping_interval=30,   # send a WS ping every 30 s to keep alive
                ping_timeout=10,
                reconnect=0,        # we handle reconnect ourselves for backoff
            )

            if self._shutdown.is_set():
                break

            log.info(
                "Reconnecting in %.1fs (messages=%d errors=%d)…",
                self._reconnect_delay, self._message_count, self._error_count,
            )
            self._shutdown.wait(timeout=self._reconnect_delay)

            # Exponential backoff capped at max delay
            self._reconnect_delay = min(
                self._reconnect_delay * RECONNECT_MULTIPLIER,
                RECONNECT_MAX_DELAY,
            )

    def shutdown(self) -> None:
        log.info(
            "Shutting down (messages=%d errors=%d)",
            self._message_count, self._error_count,
        )
        self._shutdown.set()
        if self.ws:
            self.ws.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    listener = WebSocketListener()

    def _handle_signal(sig, frame):
        log.info("Signal %s received — shutting down gracefully", sig)
        listener.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    listener.run()


if __name__ == "__main__":
    main()
