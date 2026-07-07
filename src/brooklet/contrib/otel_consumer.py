# ABOUTME: OTLP consumer adapter — reads Vector JSONL output and surfaces trace/metric/log data
# ABOUTME: 3-layer pattern: parsing (pure) → consumer integration (brooklet API) → CLI plugin

from __future__ import annotations

import glob as glob_module
import json
import logging
from collections.abc import Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

import brooklet
from brooklet.cli.plugins import hookimpl
from brooklet.contrib.cli_options import StreamDirOption

_logger = logging.getLogger("brooklet.contrib.otel")


# ---------------------------------------------------------------------------
# Layer 1: Parsing (pure functions, no I/O)
# ---------------------------------------------------------------------------


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def parse_trace_event(event: dict) -> dict | None:
    """Extract key fields from a Vector OTLP span event.

    Returns a normalized dict with name, span_id, parent_span_id, is_root,
    duration_ms, status, service, attributes, and timestamp.
    Returns None if required fields are missing or malformed.
    """
    name = event.get("name")
    start_str = event.get("start_time_unix_nano")
    end_str = event.get("end_time_unix_nano")

    if not name or not start_str or not end_str:
        return None

    try:
        start = _parse_iso(start_str)
        end = _parse_iso(end_str)
        duration_ms = round((end - start).total_seconds() * 1000, 3)
    except (ValueError, AttributeError):
        return None

    parent_span_id = event.get("parent_span_id", "")
    # Guard against null status field from Vector
    status_obj = event.get("status") or {}
    status_code = status_obj.get("code", 0)

    return {
        "name": name,
        "span_id": event.get("span_id", ""),
        "parent_span_id": parent_span_id,
        "trace_id": event.get("trace_id", ""),
        "is_root": not parent_span_id,
        "duration_ms": duration_ms,
        "status": "error" if status_code != 0 else "ok",
        "service": (event.get("resources") or {}).get("service.name", ""),
        "attributes": event.get("attributes") or {},
        "timestamp": start_str,
    }


def parse_metric_event(event: dict) -> dict | None:
    """Extract key fields from a Vector OTLP metric event.

    Returns a normalized dict with name, timestamp, kind, metric_type, value
    (or count/sum for histograms), and service.
    Returns None if required fields are missing or the value type is unknown.
    """
    name = event.get("name")
    timestamp = event.get("timestamp")

    if not name or not timestamp:
        return None

    service = (event.get("tags") or {}).get("resource.service.name", "")
    kind = event.get("kind", "")
    base = {
        "name": name,
        "timestamp": timestamp,
        "kind": kind,
        "service": service,
        "tags": event.get("tags") or {},
    }

    if "counter" in event:
        return {**base, "metric_type": "counter", "value": event["counter"].get("value", 0.0)}
    if "gauge" in event:
        return {**base, "metric_type": "gauge", "value": event["gauge"].get("value", 0.0)}
    if "histogram" in event:
        h = event["histogram"]
        return {
            **base,
            "metric_type": "histogram",
            "count": h.get("count", 0),
            "sum": h.get("sum", 0.0),
        }

    return None


def parse_log_event(event: dict) -> dict | None:
    """Extract key fields from a Vector OTLP log event.

    Returns a normalized dict with message, severity, severity_number,
    trace_id, span_id, and attributes.
    Returns None if the message field is absent or empty.
    """
    message = event.get("message")
    if not message:
        return None

    return {
        "message": message,
        "timestamp": event.get("timestamp", ""),
        "severity": event.get("severity_text", ""),
        "severity_number": event.get("severity_number", 0),
        "trace_id": event.get("trace_id", ""),
        "span_id": event.get("span_id", ""),
        "attributes": event.get("attributes") or {},
    }


# ---------------------------------------------------------------------------
# Layer 2: Consumer integration (uses brooklet API)
# ---------------------------------------------------------------------------


def _iter_jsonl(filepath: str) -> Iterator[dict]:
    """Yield parsed dicts from a JSONL file, logging malformed lines."""
    try:
        with open(filepath) as f:
            for line_num, raw_line in enumerate(f, 1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    _logger.warning(
                        "otel_consumer: malformed JSONL line skipped",
                        extra={"path": filepath, "line": line_num, "error": str(exc)},
                    )
    except OSError as exc:
        _logger.warning(
            "otel_consumer: failed to read JSONL file",
            extra={"path": filepath, "error": str(exc)},
        )


def _scan(
    harness_dir: str,
    kind: str,
    topic: str,
    parser: Callable[[dict], dict | None],
    stream_dir: str | None,
    follow: bool,
    group: str,
) -> Iterator[dict]:
    """Yield parsed events from Vector's JSONL output for a single signal kind.

    Shared implementation behind scan_traces/scan_metrics/scan_logs. In batch
    mode reads {harness_dir}/{kind}/*.jsonl directly; in follow mode uses
    brooklet's consumer for offset tracking and tailing.

    Args:
        harness_dir: Base directory containing the {kind}/ subdir.
        kind: Signal subdirectory name (traces/metrics/logs).
        topic: Brooklet topic name for follow-mode registration.
        parser: Pure parse_*_event function applied to each raw event.
        stream_dir: Directory for brooklet offset state (follow mode only).
        follow: If True, tail for new events via brooklet consumer.
        group: Consumer group name for offset tracking.
    """
    harness_path = Path(harness_dir).resolve()
    kind_glob = str(harness_path / kind / "*.jsonl")

    if follow:
        _stream = brooklet.open(stream_dir or str(harness_path))
        _stream.register(topic, kind_glob, "glob")
        with _stream.consume(topic, group=group, follow=True) as consumer:
            for event in consumer:
                parsed = parser(event)
                if parsed is not None:
                    yield parsed
    else:
        kind_dir = harness_path / kind
        if not kind_dir.exists():
            _logger.warning(
                f"otel_consumer: {kind} directory not found", extra={"path": str(kind_dir)}
            )
            return
        for filepath in sorted(glob_module.glob(kind_glob)):
            for event in _iter_jsonl(filepath):
                parsed = parser(event)
                if parsed is not None:
                    yield parsed


def scan_traces(
    harness_dir: str,
    stream_dir: str | None = None,
    follow: bool = False,
    group: str = "otel",
) -> Iterator[dict]:
    """Yield parsed trace/span events from Vector's traces JSONL output.

    In batch mode reads {harness_dir}/traces/*.jsonl directly.
    In follow mode uses brooklet's consumer for offset tracking and tailing.

    Args:
        harness_dir: Base directory containing the traces/ subdir.
        stream_dir: Directory for brooklet offset state (follow mode only).
        follow: If True, tail for new spans via brooklet consumer.
        group: Consumer group name for offset tracking.
    """
    yield from _scan(
        harness_dir, "traces", "otel/traces", parse_trace_event, stream_dir, follow, group
    )


def scan_metrics(
    harness_dir: str,
    stream_dir: str | None = None,
    follow: bool = False,
    group: str = "otel",
) -> Iterator[dict]:
    """Yield parsed metric events from Vector's metrics JSONL output.

    In batch mode reads {harness_dir}/metrics/*.jsonl directly.
    In follow mode uses brooklet's consumer for offset tracking and tailing.

    Args:
        harness_dir: Base directory containing the metrics/ subdir.
        stream_dir: Directory for brooklet offset state (follow mode only).
        follow: If True, tail for new metrics via brooklet consumer.
        group: Consumer group name for offset tracking.
    """
    yield from _scan(
        harness_dir, "metrics", "otel/metrics", parse_metric_event, stream_dir, follow, group
    )


def scan_logs(
    harness_dir: str,
    stream_dir: str | None = None,
    follow: bool = False,
    group: str = "otel",
) -> Iterator[dict]:
    """Yield parsed log events from Vector's logs JSONL output.

    In batch mode reads {harness_dir}/logs/*.jsonl directly.
    In follow mode uses brooklet's consumer for offset tracking and tailing.

    Args:
        harness_dir: Base directory containing the logs/ subdir.
        stream_dir: Directory for brooklet offset state (follow mode only).
        follow: If True, tail for new log records via brooklet consumer.
        group: Consumer group name for offset tracking.
    """
    yield from _scan(
        harness_dir, "logs", "otel/logs", parse_log_event, stream_dir, follow, group
    )


# ---------------------------------------------------------------------------
# Layer 3: Output renderers and CLI plugin
# ---------------------------------------------------------------------------


def _render_trace(span: dict) -> str:
    root_marker = "ROOT" if span["is_root"] else span["parent_span_id"][:8]
    attrs = (
        " ".join(f"{k}={v}" for k, v in span["attributes"].items()) if span["attributes"] else ""
    )
    parts = [span["name"], f"{span['duration_ms']}ms", root_marker, span["status"]]
    if attrs:
        parts.append(attrs)
    return "  ".join(parts)


def _render_metric(m: dict) -> str:
    if m["metric_type"] == "histogram":
        return f"{m['name']}  count={m['count']} sum={m['sum']}  [{m['kind']}]"
    return f"{m['name']}  {m['value']}  [{m['metric_type']}/{m['kind']}]"


def _render_log(log: dict) -> str:
    sev = log["severity"] or "?"
    return f"{sev}  {log['message']}"


class OtelPlugin:
    """Pluggy plugin that registers the brooklet otel subcommand."""

    @hookimpl
    def brooklet_commands(self, cli: typer.Typer) -> None:
        otel_app = typer.Typer(help="OTLP observability consumer (Vector JSONL)")

        @otel_app.command()
        def traces(
            harness_dir: Annotated[
                str, typer.Argument(help="Base directory containing traces/ subdir")
            ],
            follow: Annotated[bool, typer.Option(help="Tail for new spans")] = False,
            group: Annotated[str, typer.Option(help="Consumer group name")] = "otel",
            stream_dir: StreamDirOption = None,
        ) -> None:
            """Show OTLP trace spans from Vector JSONL output."""
            for span in scan_traces(
                harness_dir, str(stream_dir) if stream_dir else None, follow, group
            ):
                typer.echo(_render_trace(span))

        @otel_app.command()
        def metrics(
            harness_dir: Annotated[
                str, typer.Argument(help="Base directory containing metrics/ subdir")
            ],
            follow: Annotated[bool, typer.Option(help="Tail for new metrics")] = False,
            group: Annotated[str, typer.Option(help="Consumer group name")] = "otel",
            stream_dir: StreamDirOption = None,
        ) -> None:
            """Show OTLP metrics from Vector JSONL output."""
            for m in scan_metrics(
                harness_dir, str(stream_dir) if stream_dir else None, follow, group
            ):
                typer.echo(_render_metric(m))

        @otel_app.command()
        def logs(
            harness_dir: Annotated[
                str, typer.Argument(help="Base directory containing logs/ subdir")
            ],
            follow: Annotated[bool, typer.Option(help="Tail for new log records")] = False,
            group: Annotated[str, typer.Option(help="Consumer group name")] = "otel",
            stream_dir: StreamDirOption = None,
        ) -> None:
            """Show OTLP log records from Vector JSONL output."""
            for log in scan_logs(
                harness_dir, str(stream_dir) if stream_dir else None, follow, group
            ):
                typer.echo(_render_log(log))

        cli.add_typer(otel_app, name="otel")
