# brooklet

[![tests](https://github.com/joshuaoliphant/brooklet/actions/workflows/test.yml/badge.svg)](https://github.com/joshuaoliphant/brooklet/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/brooklet)](https://pypi.org/project/brooklet/)

The SQLite of event streaming — consumer coordination on top of JSONL files.

Brooklet adds **offsets, tailing, and topic discovery** to the append-only JSONL files that tools like Claude Code, structlog, and OpenTelemetry already produce. It doesn't replace your files or add a broker — it just makes them consumable as event streams.

## Install

```bash
uv add brooklet             # as a library dependency
uv tool install brooklet    # as a global CLI tool
pip install brooklet        # or with pip
```

## Quickstart

```python
import brooklet

# Open a stream directory (creates .brooklet/ metadata)
stream = brooklet.open("./my-streams")

# Register an external JSONL file as a named topic
stream.register("claude-history", path="~/.claude/history.jsonl", mode="single-file")

# Consume events with automatic offset tracking
for event in stream.consume("claude-history", group="my-app"):
    print(event["_seq"], event["_ts"], list(event.keys())[:3])

# Second run with same group — picks up only new events
for event in stream.consume("claude-history", group="my-app"):
    print("New:", event)
```

### Follow mode (live tailing)

```python
# Tail a file for new events (like `tail -f` but with offsets)
consumer = stream.consume("claude-history", group="watcher", follow=True)
for event in consumer:
    print(f"Live: {event['_seq']}")
    if should_stop():
        consumer.close()
        break
```

### Glob mode (multiple files)

```python
# Register a glob pattern — all matching files are consumed in sorted order
stream.register("sessions", path="~/.claude/projects/*/*.jsonl", mode="glob")
for event in stream.consume("sessions", group="analytics"):
    print(event["_seq"], event.get("type"))

# Glob + follow detects both new lines in existing files AND new files
for event in stream.consume("sessions", group="live", follow=True):
    print(f"New event from {event.get('sessionId', 'unknown')}")
```

### Produce (derived topics)

```python
# Consumers that transform data can produce to local topics
stream.produce("scout/stats", {"type": "session-stats", "tokens": 12345}, source="scout")

# Produced topics are auto-registered and immediately consumable
for event in stream.consume("scout/stats", group="dashboard"):
    print(event)  # Has _ts, _seq, _src envelope fields
```

## Envelope

Every event gets thin metadata auto-injected:

| Field | Description | Behavior |
|-------|-------------|----------|
| `_ts` | ISO 8601 timestamp | Set if missing, preserved if present |
| `_seq` | Monotonic sequence number | Always set by brooklet |
| `_src` | Producer identifier | Set from `source` param or topic name |

The `_` prefix avoids collisions with any producer's payload.

## Key Concepts

- **`echo >>` is the universal producer API** — external tools write JSONL, brooklet reads it
- **`produce()` for derived data** — consumers that transform and re-emit use `stream.produce()`
- **Consumer groups** — independent offset tracking per group name
- **Source registration** — maps external file paths to topic names
- **Byte offsets** — O(1) resume, no line scanning on restart
- **Path-style topics** — `"scout/session-stats"` creates nested directories

## CLI

Brooklet ships a unified CLI with core commands and plugin subcommands:

```bash
# Core commands — pipe-friendly Unix citizens
echo '{"type":"hello"}' | brooklet produce my-topic --stream-dir ./streams
brooklet consume my-topic --group reader --stream-dir ./streams | jq '.'
brooklet cat my-topic --stream-dir ./streams             # read-only, no offset tracking
brooklet register sessions "~/.claude/projects/*/*.jsonl" --mode glob --stream-dir ./streams
brooklet topics --stream-dir ./streams --json
```

Set `BROOKLET_DIR` to avoid repeating `--stream-dir`:

```bash
export BROOKLET_DIR=./streams
echo '{"event":"test"}' | brooklet produce events
brooklet consume events --group reader
```

### Plugin system

Brooklet uses [pluggy](https://pluggy.readthedocs.io/) for plugin discovery. Built-in plugins (`scout`, `pytest`) and third-party plugins use the same interface. Third-party packages register via entry points:

```toml
# In your package's pyproject.toml
[project.entry-points.brooklet]
my-plugin = "my_package:MyPlugin"
```

### Scout (Claude Code analytics)

```bash
# Scan all sessions for a project
brooklet scout scan ~/.claude/projects/-Users-you-your-project/

# Current session only
brooklet scout scan ~/.claude/projects/-Users-you-your-project/ --current

# Live dashboard
brooklet scout scan ~/.claude/projects/-Users-you-your-project/ --current --follow --dashboard

# Produce stats as JSONL for downstream consumers
brooklet scout scan ~/.claude/projects/-Users-you-your-project/ --output scout/session-stats
```

Reports token usage, tool call frequency, model breakdown, session duration, and event counts.

### pytest (test run analytics)

Consumes [pytest-reportlog](https://github.com/pytest-dev/pytest-reportlog) JSONL output:

```bash
# Analyze a single test run
brooklet pytest scan path/to/test-results.jsonl

# Analyze multiple runs (glob mode)
brooklet pytest scan "reports/run-*.jsonl" --glob

# Produce summary stats to a brooklet topic for downstream consumers
brooklet pytest scan "reports/run-*.jsonl" --glob --output pytest/summaries
```

Reports pass/fail/skip/error counts, total duration, slowest 5 tests, and failure details per run.

To generate the input JSONL, install `pytest-reportlog` and run:

```bash
pytest --report-log=test-results.jsonl
```

### Pipeline example: CI health gate

The `--output` flag produces structured summaries to a brooklet topic that downstream consumers can read. See [`examples/ci_health_check.py`](examples/ci_health_check.py) for a complete example that gates CI on test health:

```bash
# Run tests → analyze → produce summaries → health check
pytest --report-log=reports/results.jsonl
brooklet pytest scan reports/results.jsonl --output pytest/summaries
python examples/ci_health_check.py reports/
```

The health check consumes the `pytest/summaries` topic and fails if any run has failures or tests exceeding a duration threshold. This pipeline runs in brooklet's own CI — see [`.github/workflows/test.yml`](.github/workflows/test.yml).

### Example: JSONL logging as an event stream

Any Python app using structured JSON logging (via `python-json-logger`, `structlog`, or a custom formatter) produces JSONL that brooklet can consume directly — no adapter needed. See [`examples/jsonl_logging.py`](examples/jsonl_logging.py) for a complete example:

```bash
# Terminal 1 — generate log events (writes app.jsonl every second)
uv run python examples/jsonl_logging.py produce /tmp/log-demo

# Terminal 2 — tail the log stream in follow mode
uv run python examples/jsonl_logging.py follow /tmp/log-demo
```

The example registers the log file as an `app/logs` topic and consumes it with offset tracking. It works with or without `python-json-logger` installed (falls back to a stdlib-only formatter).

## Try It

### Pipe anything through brooklet

```bash
# Git log as a consumable stream
git log --format='{"hash":"%h","author":"%an","date":"%aI","msg":"%s"}' -20 \
  | brooklet produce git/log --stream-dir ./demo

# Consume and transform with jq
brooklet consume git/log --group viewer --stream-dir ./demo \
  | jq -r '"\(.date[0:10]) \(.hash) \(.msg[0:60])"'

# System processes as events
ps aux | awk 'NR>1 {printf "{\"user\":\"%s\",\"pid\":%s,\"cpu\":%s}\n",$1,$2,$3}' \
  | brooklet produce system/procs --stream-dir ./demo

# Weather as a stream (via wttr.in)
curl -s "wttr.in/YourCity?format=j1" \
  | jq -c '{temp: .current_condition[0].temp_F, desc: .current_condition[0].weatherDesc[0].value}' \
  | brooklet produce weather --stream-dir ./demo --source wttr
```

### Offset tracking just works

```bash
# First consume reads everything
brooklet consume git/log --group reader --stream-dir ./demo | wc -l  # → 20

# Second consume reads nothing (already caught up)
brooklet consume git/log --group reader --stream-dir ./demo | wc -l  # → 0

# Different group = independent position
brooklet consume git/log --group other --stream-dir ./demo | wc -l   # → 20
```

**Tip:** Use `jq -c` when piping pretty-printed JSON into `brooklet produce` — brooklet reads one JSON object per line.

## API

| Method | Purpose |
|--------|---------|
| `brooklet.open(path)` | Open a stream directory |
| `stream.register(name, path, mode)` | Map external JSONL to a topic name |
| `stream.consume(topic, group, follow)` | Read events with offset tracking |
| `stream.produce(topic, event, source)` | Write events to a local topic |
| `stream.topics()` | List all registered topics |

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release history. Generated with [git-cliff](https://git-cliff.org/).

## Development

```bash
uv run pytest -v          # Run all tests
uv run pytest tests/bdd/  # BDD acceptance tests
uv run ruff check .       # Lint
```

## License

MIT
