# ABOUTME: OTLP consumer adapter — reads Vector JSONL output and surfaces trace/metric/log data
# ABOUTME: 3-layer pattern: parsing (pure) → consumer integration (brooklet API) → CLI plugin

from __future__ import annotations

import glob as glob_module
import json
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

import brooklet
from brooklet.plugins import hookimpl

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
    status_code = event.get("status", {}).get("code", 0)

    return {
        "name": name,
        "span_id": event.get("span_id", ""),
        "parent_span_id": parent_span_id,
        "trace_id": event.get("trace_id", ""),
        "is_root": not parent_span_id,
        "duration_ms": duration_ms,
        "status": "error" if status_code != 0 else "ok",
        "service": event.get("resources", {}).get("service.name", ""),
        "attributes": event.get("attributes", {}),
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

    service = event.get("tags", {}).get("resource.service.name", "")
    kind = event.get("kind", "")
    base = {
        "name": name,
        "timestamp": timestamp,
        "kind": kind,
        "service": service,
        "tags": event.get("tags", {}),
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
    Returns None if the message field is absent.
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
        "attributes": event.get("attributes", {}),
    }


# ---------------------------------------------------------------------------
# Layer 2: Consumer integration (uses brooklet API)
# ---------------------------------------------------------------------------


def _iter_jsonl(filepath: str) -> Iterator[dict]:
    """Yield parsed dicts from a JSONL file, skipping malformed lines."""
    try:
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except OSError as e:
        print(f"Warning: {filepath}: {e}", file=sys.stderr)


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
    traces_glob = str(Path(harness_dir) / "traces" / "*.jsonl")

    if follow:
        _stream = brooklet.open(stream_dir or harness_dir)
        _stream.register("otel/traces", traces_glob, "glob")
        with _stream.consume("otel/traces", group=group, follow=True) as consumer:
            for event in consumer:
                parsed = parse_trace_event(event)
                if parsed is not None:
                    yield parsed
    else:
        for filepath in sorted(glob_module.glob(traces_glob)):
            for event in _iter_jsonl(filepath):
                parsed = parse_trace_event(event)
                if parsed is not None:
                    yield parsed


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
    metrics_glob = str(Path(harness_dir) / "metrics" / "*.jsonl")

    if follow:
        _stream = brooklet.open(stream_dir or harness_dir)
        _stream.register("otel/metrics", metrics_glob, "glob")
        with _stream.consume("otel/metrics", group=group, follow=True) as consumer:
            for event in consumer:
                parsed = parse_metric_event(event)
                if parsed is not None:
                    yield parsed
    else:
        for filepath in sorted(glob_module.glob(metrics_glob)):
            for event in _iter_jsonl(filepath):
                parsed = parse_metric_event(event)
                if parsed is not None:
                    yield parsed


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
    logs_glob = str(Path(harness_dir) / "logs" / "*.jsonl")

    if follow:
        _stream = brooklet.open(stream_dir or harness_dir)
        _stream.register("otel/logs", logs_glob, "glob")
        with _stream.consume("otel/logs", group=group, follow=True) as consumer:
            for event in consumer:
                parsed = parse_log_event(event)
                if parsed is not None:
                    yield parsed
    else:
        for filepath in sorted(glob_module.glob(logs_glob)):
            for event in _iter_jsonl(filepath):
                parsed = parse_log_event(event)
                if parsed is not None:
                    yield parsed


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
            stream_dir: Annotated[
                Path | None,
                typer.Option("--stream-dir", envvar="BROOKLET_DIR", help="Brooklet stream dir"),
            ] = None,
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
            stream_dir: Annotated[
                Path | None,
                typer.Option("--stream-dir", envvar="BROOKLET_DIR", help="Brooklet stream dir"),
            ] = None,
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
            stream_dir: Annotated[
                Path | None,
                typer.Option("--stream-dir", envvar="BROOKLET_DIR", help="Brooklet stream dir"),
            ] = None,
        ) -> None:
            """Show OTLP log records from Vector JSONL output."""
            for log in scan_logs(
                harness_dir, str(stream_dir) if stream_dir else None, follow, group
            ):
                typer.echo(_render_log(log))

        cli.add_typer(otel_app, name="otel")
