# brooklet

The SQLite of event streaming — consumer coordination on top of JSONL files.

Brooklet adds **offsets, tailing, and topic discovery** to the append-only JSONL files that tools like Claude Code, structlog, and OpenTelemetry already produce. It doesn't replace your files or add a broker — it just makes them consumable as event streams.

## Quickstart

```bash
pip install brooklet   # or: uv add brooklet
```

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
import threading

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
stream.register("sessions", path="~/.claude/projects/*/*.jsonl", mode="glob")
for event in stream.consume("sessions", group="analytics"):
    print(event["_seq"], event.get("type"))
```

## Envelope

Every event gets thin metadata auto-injected on read:

| Field | Description | Behavior |
|-------|-------------|----------|
| `_ts` | ISO 8601 timestamp | Set if missing, preserved if present |
| `_seq` | Monotonic sequence number | Always set by brooklet |
| `_src` | Producer identifier | Set if `source` param provided |

The `_` prefix avoids collisions with any producer's payload.

## Key Concepts

- **No `produce()`** — `echo '{"type":"hello"}' >> topic.jsonl` is the universal producer API
- **Consumer groups** — independent offset tracking per group name
- **Source registration** — maps external file paths to topic names
- **Byte offsets** — O(1) resume, no line scanning on restart

## Development

```bash
uv run pytest -v          # Run tests (43 tests)
uv run ruff check .       # Lint
```

## License

MIT
