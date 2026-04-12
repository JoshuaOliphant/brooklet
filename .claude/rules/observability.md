---
paths:
  - ".claude/harness/observability/**"
  - "src/brooklet/contrib/otel.py"
---

## Observability Stack

Local observability stack: Vector (OTLP collector) → VictoriaLogs + VictoriaMetrics + JSONL files.

### Quick Reference

```bash
# Check stack status
bash .claude/harness/observability/status.sh

# Start / stop
bash .claude/harness/observability/start.sh
bash .claude/harness/observability/stop.sh

# Install binaries (first time only — start.sh auto-calls this)
bash .claude/harness/observability/install.sh
```

### Querying Logs (VictoriaLogs — LogsQL)

```bash
# Recent logs
curl -s 'http://127.0.0.1:9428/select/logsql/query?query=*&limit=10' | jq .

# Filter by stream
curl -s 'http://127.0.0.1:9428/select/logsql/query?query=_stream:brooklet&limit=20' | jq .

# Search by message content
curl -s 'http://127.0.0.1:9428/select/logsql/query?query=_msg:error&limit=10' | jq .

# Time-bounded query (last 5 minutes)
curl -s 'http://127.0.0.1:9428/select/logsql/query?query=*&start=5m&limit=50' | jq .
```

### Querying Metrics (VictoriaMetrics — PromQL)

```bash
# Events produced total
curl -s 'http://127.0.0.1:8428/api/v1/query?query=brooklet_events_produced_total' | jq .

# Events consumed total
curl -s 'http://127.0.0.1:8428/api/v1/query?query=brooklet_events_consumed_total' | jq .

# Batch size histogram
curl -s 'http://127.0.0.1:8428/api/v1/query?query=brooklet_batch_size_bucket' | jq .

# All available metrics
curl -s 'http://127.0.0.1:8428/api/v1/label/__name__/values' | jq .
```

### Dog-Fooding with Brooklet

JSONL telemetry files land in `.claude/harness/observability/data/jsonl/`. Register them as brooklet topics:

```bash
brooklet register otel/logs ".claude/harness/observability/data/jsonl/logs/*.jsonl" --mode glob
brooklet register otel/traces ".claude/harness/observability/data/jsonl/traces/*.jsonl" --mode glob
brooklet consume otel/logs --group agent
```

### Python Instrumentation

The `brooklet.contrib.otel` module provides `tracer` and `meter` that are no-ops when OTel is not installed. To enable real telemetry:

```bash
uv sync --group otel
```

Then call `configure()` early in your code:
```python
from brooklet.contrib.otel import configure
configure()  # connects to Vector at http://127.0.0.1:4318
```

### Ports

| Service | Port | Protocol |
|---------|------|----------|
| Vector OTLP gRPC | 4317 | gRPC |
| Vector OTLP HTTP | 4318 | HTTP |
| VictoriaLogs | 9428 | HTTP |
| VictoriaMetrics | 8428 | HTTP |
