# ABOUTME: Test helpers for otel_consumer tests — factory functions for OTLP JSONL events
# ABOUTME: Mirrors the actual Vector output format discovered from the live harness

from __future__ import annotations

import json
from pathlib import Path


def make_trace_event(
    name: str = "produce",
    span_id: str = "07950107db47cba4",
    parent_span_id: str = "0a1c73353622833e",
    trace_id: str = "646ed83a29e92055a5d4ac3a731d2580",
    start: str = "2026-04-18T14:55:52.176620Z",
    end: str = "2026-04-18T14:55:52.177800Z",
    attributes: dict | None = None,
    service: str = "brooklet",
    status_code: int = 0,
) -> dict:
    return {
        "name": name,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "trace_id": trace_id,
        "start_time_unix_nano": start,
        "end_time_unix_nano": end,
        "attributes": attributes or {},
        "resources": {
            "service.name": service,
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.41.0",
        },
        "status": {"code": status_code, "message": ""},
        "kind": 1,
        "dropped_attributes_count": 0,
        "dropped_events_count": 0,
        "dropped_links_count": 0,
        "ingest_timestamp": "2026-04-18T14:55:54.189458Z",
        "trace_state": "",
    }


def make_root_trace_event(**kwargs) -> dict:
    return make_trace_event(parent_span_id="", **kwargs)


def make_metric_event(
    name: str = "brooklet.events_produced",
    timestamp: str = "2026-04-18T14:55:54.181074Z",
    kind: str = "incremental",
    value: float = 1.0,
    metric_type: str = "counter",
    tags: dict | None = None,
) -> dict:
    event: dict = {
        "name": name,
        "timestamp": timestamp,
        "kind": kind,
        "tags": tags
        or {
            "resource.service.name": "brooklet",
            "resource.telemetry.sdk.language": "python",
            "resource.telemetry.sdk.name": "opentelemetry",
            "resource.telemetry.sdk.version": "1.41.0",
        },
    }
    if metric_type == "counter":
        event["counter"] = {"value": value}
    elif metric_type == "gauge":
        event["gauge"] = {"value": value}
    elif metric_type == "histogram":
        event["histogram"] = {"count": int(value), "sum": value * 10.0}
    return event


def make_log_event(
    message: str = "test log message",
    timestamp: str = "2026-04-18T14:55:52.176620Z",
    severity_text: str = "INFO",
    severity_number: int = 9,
    attributes: dict | None = None,
) -> dict:
    return {
        "message": message,
        "timestamp": timestamp,
        "severity_text": severity_text,
        "severity_number": severity_number,
        "attributes": attributes or {"service.name": "brooklet"},
        "trace_id": "",
        "span_id": "",
    }


def write_traces_file(directory: Path, name: str, events: list[dict]) -> Path:
    path = directory / f"{name}.jsonl"
    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return path


def write_metrics_file(directory: Path, name: str, events: list[dict]) -> Path:
    path = directory / f"{name}.jsonl"
    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return path


def write_logs_file(directory: Path, name: str, events: list[dict]) -> Path:
    path = directory / f"{name}.jsonl"
    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return path
