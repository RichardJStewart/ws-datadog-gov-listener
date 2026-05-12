# ws-datadog-gov-listener

A persistent Python WebSocket listener that captures every push event from a WebSocket endpoint and forwards metrics to **Datadog GovCloud (`ddog-gov.com`)** in real time.

Built to solve the push-vs-pull monitoring problem: rather than polling on a schedule and missing irregular updates, this service stays subscribed continuously and emits a metric for every message the moment it arrives.

---

## Table of Contents

- [Why This Exists](#why-this-exists)
- [How It Works](#how-it-works)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Listener](#running-the-listener)
- [Metrics Reference](#metrics-reference)
- [Customising the Message Parser](#customising-the-message-parser)
- [Emit Modes](#emit-modes)
- [GovCloud-Specific Notes](#govcloud-specific-notes)
- [Reconnection & Reliability](#reconnection--reliability)
- [Running as a Service (systemd)](#running-as-a-service-systemd)
- [Running with Docker](#running-with-docker)
- [Troubleshooting](#troubleshooting)

---

## Why This Exists

Datadog Synthetic WebSocket tests operate on a **polling** model — they open a connection, check the state at that instant, and close. If your WebSocket endpoint pushes updates at irregular intervals (e.g. ~40 entities updating unpredictably), polling will miss most events.

This listener solves that by holding a **single persistent WebSocket connection** and emitting a Datadog metric on every push. No gaps, no missed events.

---

## How It Works

```
WebSocket Endpoint
      │  pushes updates
      ▼
ws_datadog_listener.py  ◄── runs continuously, reconnects automatically
      │
      ├── parse_message()   extracts entity_id, value, timestamp
      │
      ├── DogStatsD (UDP)  ──► Datadog Agent ──► api.ddog-gov.com
      └── HTTP API (HTTPS) ──────────────────► api.ddog-gov.com
```

On each received message the listener emits three metrics (see [Metrics Reference](#metrics-reference)) tagged with `entity_id` so you can filter and alert per entity in Datadog dashboards.

---

## Requirements

- Python 3.10+
- A Datadog GovCloud account with an API key (and optionally an App key)
- A Datadog Agent running locally **if using DogStatsD emit mode** (see [GovCloud-Specific Notes](#govcloud-specific-notes))

---

## Installation

```bash
# 1. Clone the repository
git clone git@github.com:<your-org>/ws-datadog-gov-listener.git
cd ws-datadog-gov-listener

# 2. Create and activate a virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Configuration

All configuration is supplied via **environment variables** — no secrets in code or config files.

| Variable | Required | Default | Description |
|---|---|---|---|
| `WS_URL` | ✅ | `wss://your-websocket-endpoint/ws` | Full WebSocket URL to subscribe to |
| `DD_API_KEY` | ✅ (HTTP mode) | `""` | Datadog GovCloud API key |
| `DD_APP_KEY` | ❌ | `""` | Datadog GovCloud App key (some endpoints require it) |
| `DD_ENV` | ❌ | `production` | Value for the `env:` tag on all metrics |
| `DD_EMIT_MODE` | ❌ | `statsd` (if available) else `http` | How to ship metrics: `statsd`, `http`, or `both` |
| `STATSD_HOST` | ❌ | `localhost` | Hostname of the local Datadog Agent (DogStatsD) |
| `STATSD_PORT` | ❌ | `8125` | UDP port of the local Datadog Agent |
| `DD_METRIC_UPDATE` | ❌ | `websocket.entity.update` | Base metric name for entity update events |
| `DD_METRIC_LATENCY` | ❌ | `websocket.entity.latency_ms` | Metric name for end-to-end message latency |
| `DD_METRIC_ERRORS` | ❌ | `websocket.listener.errors` | Metric name for listener error events |
| `RECONNECT_INITIAL_DELAY` | ❌ | `2` | Seconds before first reconnect attempt |
| `RECONNECT_MAX_DELAY` | ❌ | `60` | Maximum seconds between reconnect attempts |
| `RECONNECT_MULTIPLIER` | ❌ | `2` | Backoff multiplier applied after each failed reconnect |

> **Never commit secrets.** Use a `.env` file (excluded by `.gitignore`) or a secrets manager such as AWS Secrets Manager or HashiCorp Vault.

### Example `.env` file

```bash
WS_URL=wss://your-internal-endpoint/ws
DD_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
DD_APP_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
DD_ENV=production
DD_EMIT_MODE=statsd
```

Load it before running:

```bash
export $(grep -v '^#' .env | xargs)
```

---

## Running the Listener

```bash
# Minimal — DogStatsD mode, Agent running locally
WS_URL="wss://your-endpoint/ws" python ws_datadog_listener.py

# HTTP API mode (no local Agent required)
WS_URL="wss://your-endpoint/ws" \
DD_API_KEY="your_api_key" \
DD_EMIT_MODE="http" \
python ws_datadog_listener.py

# Both modes simultaneously
WS_URL="wss://your-endpoint/ws" \
DD_API_KEY="your_api_key" \
DD_EMIT_MODE="both" \
python ws_datadog_listener.py
```

Stop with `Ctrl+C` — the listener shuts down gracefully, logging final message and error counts.

---

## Metrics Reference

Three metrics are emitted for each WebSocket message received:

| Metric | Type | Description |
|---|---|---|
| `websocket.entity.update` | Count | Incremented once per received message |
| `websocket.entity.update.value` | Gauge | The numeric value extracted from the message payload |
| `websocket.entity.latency_ms` | Gauge | End-to-end latency in milliseconds (only emitted if the message contains a server-side timestamp) |
| `websocket.listener.errors` | Count | Incremented on WebSocket errors or message parse failures |

### Tags applied to every metric

| Tag | Example | Description |
|---|---|---|
| `source` | `source:websocket_listener` | Always present; identifies this listener |
| `env` | `env:production` | Set via `DD_ENV` |
| `site` | `site:govcloud` | Hardcoded; confirms GovCloud routing |
| `entity_id` | `entity_id:sensor_42` | Extracted from each message payload |
| `entity_type` | `entity_type:temperature` | Extracted from message payload if present |

---

## Customising the Message Parser

The `parse_message()` function in `ws_datadog_listener.py` is the only section you need to change to match your WebSocket payload schema.

**Default expected payload shape:**

```json
{
  "entity_id": "sensor_42",
  "value": 99.5,
  "timestamp": "2025-05-12T14:00:00Z",
  "type": "temperature"
}
```

Field fallbacks already built in:
- `entity_id` → falls back to `id` → falls back to `"unknown"`
- `value` → falls back to `v` → falls back to `1`
- `timestamp` → also accepts `ts` as field name; accepts Unix epoch (int/float) or ISO 8601 string
- Non-JSON messages are handled gracefully without crashing

To add custom tags from your payload, extend the `extra_tags` list inside `parse_message()`:

```python
# Example: add a region tag if your payload includes one
if "region" in data:
    extra_tags.append(f"region:{data['region']}")
```

---

## Emit Modes

### `statsd` (default when `datadog` package is installed)

Sends metrics over UDP to a local Datadog Agent. The Agent batches and forwards them to Datadog GovCloud. This is the lowest-latency option and recommended for production.

**Requirement:** Datadog Agent must be running locally with `site: ddog-gov.com` in `datadog.yaml`.

### `http`

Sends metrics directly to `https://api.ddog-gov.com` via the Datadog API client. No local Agent needed. Slightly higher latency than StatsD but simpler to deploy in containerised or serverless environments.

**Requirement:** `DD_API_KEY` must be set.

### `both`

Sends via both channels simultaneously. Useful during initial validation to compare delivery.

---

## GovCloud-Specific Notes

> These are enforced in code and are not overridable via environment variables.

- **`DD_SITE` is hardcoded to `ddog-gov.com`** — the env var is intentionally not read to prevent accidental routing to `datadoghq.com`.
- **`cfg.host` is set to `https://api.ddog-gov.com`** — the Datadog Python SDK's `server_variables["site"]` approach can silently fall back to the commercial endpoint on some SDK versions. Setting `cfg.host` directly guarantees GovCloud routing.
- **Datadog Agent `site` setting** — if using DogStatsD mode, the Agent must be configured to forward to GovCloud. Verify `datadog.yaml`:

  ```yaml
  site: ddog-gov.com
  api_key: <your_govcloud_api_key>
  ```

- **Network / firewall** — ensure outbound HTTPS (port 443) is permitted to `*.ddog-gov.com` from the host running this listener.
- **FedRAMP compliance** — this service only transmits metric data (names, numeric values, tags). Ensure no PII or controlled data is included in WebSocket message payloads before they are forwarded as metric tags or values.

---

## Reconnection & Reliability

The listener uses exponential backoff with a configurable cap:

```
attempt 1: wait 2s
attempt 2: wait 4s
attempt 3: wait 8s
...
attempt N: wait 60s (max)
```

On a successful reconnection the backoff resets to the initial delay. The reconnect loop runs in the main thread — no background threads are leaked on disconnect.

---

## Running as a Service (systemd)

Create `/etc/systemd/system/ws-datadog-listener.service`:

```ini
[Unit]
Description=WebSocket → Datadog GovCloud Listener
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=datadog-listener
WorkingDirectory=/opt/ws-datadog-gov-listener
EnvironmentFile=/opt/ws-datadog-gov-listener/.env
ExecStart=/opt/ws-datadog-gov-listener/.venv/bin/python ws_datadog_listener.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable ws-datadog-listener
sudo systemctl start ws-datadog-listener
sudo journalctl -u ws-datadog-listener -f
```

---

## Running with Docker

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ws_datadog_listener.py .
CMD ["python", "ws_datadog_listener.py"]
```

```bash
docker build -t ws-datadog-gov-listener .

docker run -d \
  -e WS_URL="wss://your-endpoint/ws" \
  -e DD_API_KEY="your_api_key" \
  -e DD_EMIT_MODE="http" \
  --name ws-listener \
  ws-datadog-gov-listener
```

---

## Troubleshooting

**No metrics appearing in Datadog**
- Confirm `DD_API_KEY` is a GovCloud key (not a commercial key).
- Check logs for `HTTP API emit failed` or `StatsD emit failed` lines.
- If using `statsd` mode, verify the Datadog Agent is running: `sudo systemctl status datadog-agent`.
- Verify Agent `site` is `ddog-gov.com`, not `datadoghq.com`.
- Check firewall rules allow outbound HTTPS to `api.ddog-gov.com`.

**WebSocket connection failing immediately**
- Test the endpoint manually: `wscat -c wss://your-endpoint/ws`
- Check if the endpoint requires authentication headers — add them to the `WebSocketApp` constructor's `header` argument.

**`latency_ms` metric not appearing**
- The metric is only emitted if the incoming message contains a `timestamp` or `ts` field. If your payload doesn't include one, only `update` and `update.value` metrics will be emitted.

**High error counts in `websocket.listener.errors`**
- Set `logging.basicConfig(level=logging.DEBUG)` temporarily to see full stack traces for parse failures.
- Check that `parse_message()` field names match your actual payload schema.
