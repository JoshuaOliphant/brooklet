# Observability Harness

Local observability stack for brooklet development. Provides telemetry
collection (traces, metrics, logs) via a lightweight binary-only setup —
no Docker or containers required.

## Architecture

```
brooklet (OTel SDK) ──OTLP──▶ Vector ──▶ VictoriaLogs   (logs)
                                     ├──▶ VictoriaMetrics (metrics)
                                     └──▶ JSONL files     (dog-food)
```

## Quick Start

```bash
# Install binaries (one-time, ~100 MB)
bash .claude/harness/observability/install.sh

# Start stack (idempotent — safe to run repeatedly)
bash .claude/harness/observability/start.sh

# Check health
bash .claude/harness/observability/status.sh

# Stop everything
bash .claude/harness/observability/stop.sh
```

## SessionStart Hook

The stack auto-starts via the Claude Code `SessionStart` hook configured in
`.claude/settings.json`. No manual intervention needed.

## Ports

| Service           | Port | URL                         |
|-------------------|------|-----------------------------|
| Vector OTLP gRPC  | 4317 | `grpc://127.0.0.1:4317`    |
| Vector OTLP HTTP  | 4318 | `http://127.0.0.1:4318`    |
| VictoriaLogs      | 9428 | `http://127.0.0.1:9428`    |
| VictoriaMetrics   | 8428 | `http://127.0.0.1:8428`    |

## Python Instrumentation

Install the optional `otel` dependency group:

```bash
uv sync --group otel
```

Then configure once at startup:

```python
from brooklet.contrib.otel import configure
configure()  # exports to Vector at http://127.0.0.1:4318
```

The `brooklet.contrib.otel` module is a **no-op** when OTel packages are not
installed — zero runtime cost in production.

## Data Layout

```
.claude/harness/observability/
├── bin/           # Downloaded binaries (gitignored)
├── data/          # Runtime data (gitignored)
│   ├── victoria-logs/
│   ├── victoria-metrics/
│   ├── vector/
│   └── jsonl/     # JSONL telemetry (dog-food with brooklet)
├── logs/          # Service stdout/stderr (gitignored)
├── pids/          # PID files (gitignored)
├── install.sh     # Binary downloader
├── start.sh       # Idempotent startup
├── stop.sh        # Graceful shutdown
├── status.sh      # Health check
└── vector.toml    # Pipeline configuration
```
