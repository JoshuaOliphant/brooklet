# ABOUTME: Example showing how to consume Python JSONL logs as a brooklet stream.
# ABOUTME: Demonstrates python-json-logger setup, registration, and follow-mode tailing.

"""JSONL Logging — tail Python application logs as a brooklet event stream.

Python's logging module can output structured JSON lines using libraries like
``python-json-logger``. Since the output is plain JSONL, brooklet can consume
it directly — no special adapter needed.

This example shows three things:

1. Configuring ``python-json-logger`` to write JSONL to a file.
2. Registering that file as a brooklet topic.
3. Consuming the log stream (batch or follow mode).

Setup::

    uv add python-json-logger  # optional; a stdlib fallback is included

Usage::

    # Terminal 1 — generate some log events
    uv run python examples/jsonl_logging.py produce /tmp/log-demo

    # Terminal 2 — tail the log stream with brooklet
    uv run python examples/jsonl_logging.py consume /tmp/log-demo

    # Or follow mode (tails forever, like ``tail -f``)
    uv run python examples/jsonl_logging.py follow /tmp/log-demo
"""

import json
import logging
import sys
import time
from pathlib import Path

LOG_FILE = "app.jsonl"
TOPIC = "app/logs"


# ---------------------------------------------------------------------------
# 1. Configure python-json-logger
# ---------------------------------------------------------------------------


def setup_json_logging(log_path: Path) -> logging.Logger:
    """Set up a logger that writes one JSON object per line.

    Uses ``python-json-logger`` if available, otherwise falls back to a
    minimal stdlib-only formatter so the example works without extra deps.
    """
    logger = logging.getLogger("myapp")
    logger.setLevel(logging.DEBUG)

    handler = logging.FileHandler(log_path)

    try:
        from pythonjsonlogger import jsonlogger

        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level"},
        )
    except ImportError:
        # Stdlib-only fallback — still valid JSONL.
        class _JSONLFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                return json.dumps(
                    {
                        "timestamp": self.formatTime(record),
                        "name": record.name,
                        "level": record.levelname,
                        "message": record.getMessage(),
                    }
                )

        formatter = _JSONLFormatter()

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


# ---------------------------------------------------------------------------
# 2. Generate sample log events
# ---------------------------------------------------------------------------


def produce_logs(stream_dir: Path) -> None:
    """Write sample application logs to a JSONL file."""
    log_path = stream_dir / LOG_FILE
    stream_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_json_logging(log_path)

    print(f"Writing log events to {log_path}")
    print("Press Ctrl-C to stop.\n")

    samples = [
        (logging.INFO, "Server started", {"port": 8080}),
        (logging.INFO, "Request received", {"method": "GET", "path": "/api/health"}),
        (logging.WARNING, "Slow query", {"duration_ms": 1523, "query": "SELECT ..."}),
        (logging.INFO, "Request received", {"method": "POST", "path": "/api/events"}),
        (logging.ERROR, "Connection refused", {"host": "db.internal", "retries": 3}),
        (logging.INFO, "Request completed", {"method": "GET", "path": "/", "status": 200}),
        (logging.DEBUG, "Cache hit", {"key": "user:42"}),
        (logging.INFO, "Scheduled job ran", {"job": "cleanup", "deleted": 17}),
    ]

    try:
        i = 0
        while True:
            level, msg, extra = samples[i % len(samples)]
            logger.log(level, msg, extra=extra)
            print(f"  logged: [{logging.getLevelName(level)}] {msg}")
            i += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nWrote {i} events to {log_path}")


# ---------------------------------------------------------------------------
# 3. Consume log events with brooklet
# ---------------------------------------------------------------------------


def consume_logs(stream_dir: Path, follow: bool = False) -> None:
    """Register the log file and consume it as a brooklet stream."""
    import brooklet

    log_path = stream_dir / LOG_FILE
    if not log_path.exists():
        print(f"No log file at {log_path}. Run 'produce' first.", file=sys.stderr)
        sys.exit(1)

    stream = brooklet.open(str(stream_dir))

    # Register the external JSONL file as a topic.
    stream.register(TOPIC, path=str(log_path), mode="single-file")

    group = "log-viewer"
    mode = "follow" if follow else "batch"
    print(f"Consuming '{TOPIC}' ({mode} mode, group='{group}')\n")

    consumer = stream.consume(TOPIC, group=group, follow=follow)
    try:
        for event in consumer:
            level = event.get("level") or event.get("levelname", "?")
            msg = event.get("message", "")
            ts = event.get("timestamp") or event.get("_ts", "")
            seq = event.get("_seq", "")
            print(f"  [{seq}] {ts}  {level:>7s}  {msg}")
    except KeyboardInterrupt:
        pass
    finally:
        if hasattr(consumer, "close"):
            consumer.close()

    print("\nDone.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    usage = (
        "Usage:\n"
        "  uv run python examples/jsonl_logging.py produce <stream-dir>\n"
        "  uv run python examples/jsonl_logging.py consume <stream-dir>\n"
        "  uv run python examples/jsonl_logging.py follow  <stream-dir>\n"
    )

    if len(sys.argv) < 3:
        print(usage, file=sys.stderr)
        return 1

    command = sys.argv[1]
    stream_dir = Path(sys.argv[2])

    if command == "produce":
        produce_logs(stream_dir)
    elif command == "consume":
        consume_logs(stream_dir, follow=False)
    elif command == "follow":
        consume_logs(stream_dir, follow=True)
    else:
        print(f"Unknown command: {command}\n{usage}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
