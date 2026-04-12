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
from brooklet.contrib import otel
otel.configure()  # connects to Vector at http://127.0.0.1:4318
```

### Real-Time Monitoring

Use the Monitor tool to stream observability data while working on a task:

```bash
# Stream logs in real time — start a polling loop in background, use Monitor to watch
bash -c 'while true; do curl -s "http://127.0.0.1:9428/select/logsql/query?query=*&limit=5&start=30s" 2>/dev/null | jq -c ".[]" 2>/dev/null; sleep 5; done' &
# Then use the Monitor tool on the background process to stream events

# Watch a specific metric change during test runs
bash -c 'while true; do val=$(curl -s "http://127.0.0.1:8428/api/v1/query?query=brooklet_events_consumed_total" 2>/dev/null | jq -r ".data.result[0].value[1] // \"0\"" 2>/dev/null); echo "events_consumed=$val"; sleep 2; done' &
```

### When to Query (Workflow Guidance)

| Scenario | What to check |
|----------|--------------|
| **Debugging follow-mode** | `brooklet_events_consumed_total` — are events being read? Then check VictoriaLogs for warnings |
| **After running tests** | Query `_stream:brooklet` logs to see if instrumented code paths fired |
| **Performance work** | `rate(brooklet_events_produced_total[5m])` for throughput during benchmarks |
| **Verifying a logging fix** | Query VictoriaLogs after reproducing the bug to confirm the warning/error appears |
| **Investigating consumer lag** | Compare `brooklet_events_produced_total` vs `brooklet_events_consumed_total` per topic |

### Ports

| Service | Port | Protocol |
|---------|------|----------|
| Vector OTLP gRPC | 4317 | gRPC |
| Vector OTLP HTTP | 4318 | HTTP |
| VictoriaLogs | 9428 | HTTP |
| VictoriaMetrics | 8428 | HTTP |
