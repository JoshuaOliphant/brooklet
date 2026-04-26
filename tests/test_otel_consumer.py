# ABOUTME: Tests for the OTLP consumer adapter — covers all three layers (parse, scan, CLI)
# ABOUTME: Uses event fixtures from otel_helpers matching actual Vector JSONL output format

from __future__ import annotations

import json
import logging

import pytest
from typer.testing import CliRunner

from brooklet.cli.app import app
from brooklet.contrib.otel_consumer import (
    parse_log_event,
    parse_metric_event,
    parse_trace_event,
    scan_logs,
    scan_metrics,
    scan_traces,
)
from tests.otel_helpers import (
    make_log_event,
    make_metric_event,
    make_root_trace_event,
    make_trace_event,
    write_logs_file,
    write_metrics_file,
    write_traces_file,
)

# ---------------------------------------------------------------------------
# Layer 1: Parsing — pure functions, no I/O
# ---------------------------------------------------------------------------


class TestParseTraceEvent:
    def test_parse_produce_span_extracts_key_fields(self):
        event = make_trace_event(
            name="produce",
            span_id="07950107db47cba4",
            parent_span_id="0a1c73353622833e",
            start="2026-04-18T14:55:52.176620Z",
            end="2026-04-18T14:55:52.177800Z",
            attributes={"brooklet.topic": "myqueue"},
            service="brooklet",
        )
        result = parse_trace_event(event)
        assert result is not None
        assert result["name"] == "produce"
        assert result["span_id"] == "07950107db47cba4"
        assert result["parent_span_id"] == "0a1c73353622833e"
        assert result["is_root"] is False
        assert result["service"] == "brooklet"
        assert result["duration_ms"] == pytest.approx(1.18, abs=0.1)

    def test_root_span_flagged(self):
        event = make_root_trace_event(name="test-span")
        result = parse_trace_event(event)
        assert result is not None
        assert result["is_root"] is True
        assert result["parent_span_id"] == ""

    def test_status_ok_when_code_zero(self):
        result = parse_trace_event(make_trace_event(status_code=0))
        assert result["status"] == "ok"

    def test_status_error_when_code_nonzero(self):
        result = parse_trace_event(make_trace_event(status_code=2))
        assert result["status"] == "error"

    def test_null_status_field_treated_as_ok(self):
        event = make_trace_event()
        event["status"] = None  # Vector may emit null for unset status
        result = parse_trace_event(event)
        assert result is not None
        assert result["status"] == "ok"

    def test_attributes_included(self):
        result = parse_trace_event(make_trace_event(attributes={"brooklet.topic": "events"}))
        assert result["attributes"] == {"brooklet.topic": "events"}

    def test_missing_required_field_returns_none(self):
        assert parse_trace_event({}) is None
        assert parse_trace_event({"name": "x"}) is None  # missing timestamps

    def test_malformed_timestamp_returns_none(self):
        event = make_trace_event(start="not-a-date", end="also-not-a-date")
        assert parse_trace_event(event) is None


class TestParseMetricEvent:
    def test_parse_counter_extracts_value(self):
        event = make_metric_event(
            name="brooklet.events_produced",
            kind="incremental",
            value=5.0,
            metric_type="counter",
        )
        result = parse_metric_event(event)
        assert result is not None
        assert result["name"] == "brooklet.events_produced"
        assert result["value"] == 5.0
        assert result["metric_type"] == "counter"
        assert result["kind"] == "incremental"

    def test_parse_gauge_extracts_value(self):
        event = make_metric_event(
            name="otel.sdk.span.live", kind="absolute", value=3.0, metric_type="gauge"
        )
        result = parse_metric_event(event)
        assert result is not None
        assert result["value"] == 3.0
        assert result["metric_type"] == "gauge"

    def test_parse_histogram_extracts_count_and_sum(self):
        event = make_metric_event(
            name="brooklet.produce.latency",
            kind="absolute",
            value=10.0,
            metric_type="histogram",
        )
        result = parse_metric_event(event)
        assert result is not None
        assert result["metric_type"] == "histogram"
        assert result["count"] == 10
        assert result["sum"] == pytest.approx(100.0)

    def test_service_extracted_from_tags(self):
        result = parse_metric_event(make_metric_event(tags={"resource.service.name": "myservice"}))
        assert result["service"] == "myservice"

    def test_missing_name_returns_none(self):
        assert parse_metric_event({}) is None
        assert parse_metric_event({"timestamp": "x"}) is None

    def test_unknown_value_type_returns_none(self):
        assert parse_metric_event({"name": "x", "timestamp": "y", "kind": "z"}) is None


class TestParseLogEvent:
    def test_parse_log_extracts_key_fields(self):
        event = make_log_event(
            message="test message",
            severity_text="WARN",
            severity_number=13,
        )
        result = parse_log_event(event)
        assert result is not None
        assert result["message"] == "test message"
        assert result["severity"] == "WARN"
        assert result["severity_number"] == 13

    def test_missing_message_returns_none(self):
        assert parse_log_event({}) is None

    def test_empty_message_returns_none(self):
        assert parse_log_event({"message": ""}) is None

    def test_trace_ids_included_when_present(self):
        event = make_log_event()
        event["trace_id"] = "abc123"
        event["span_id"] = "def456"
        result = parse_log_event(event)
        assert result["trace_id"] == "abc123"
        assert result["span_id"] == "def456"


# ---------------------------------------------------------------------------
# Layer 2: Consumer integration — file I/O + brooklet stream
# ---------------------------------------------------------------------------


class TestScanTraces:
    def test_scan_yields_parsed_traces(self, tmp_path):
        traces_dir = tmp_path / "traces"
        traces_dir.mkdir()
        write_traces_file(
            traces_dir,
            "2026-04-18",
            [
                make_root_trace_event(name="root-op"),
                make_trace_event(name="produce", attributes={"brooklet.topic": "q1"}),
            ],
        )

        results = list(scan_traces(harness_dir=str(tmp_path), group="test-traces"))
        assert len(results) == 2
        assert results[0]["name"] == "root-op"
        assert results[1]["name"] == "produce"

    def test_scan_skips_unparseable_events(self, tmp_path):
        traces_dir = tmp_path / "traces"
        traces_dir.mkdir()
        (traces_dir / "2026-04-18.jsonl").write_text(
            '{"garbage": true}\n' + json.dumps(make_trace_event(name="good")) + "\n"
        )

        results = list(scan_traces(harness_dir=str(tmp_path)))
        assert len(results) == 1
        assert results[0]["name"] == "good"

    def test_scan_warns_when_traces_dir_missing(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="brooklet.contrib.otel"):
            results = list(scan_traces(harness_dir=str(tmp_path)))
        assert results == []
        assert "traces directory not found" in caplog.text


class TestScanMetrics:
    def test_scan_yields_parsed_metrics(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()
        write_metrics_file(
            metrics_dir,
            "2026-04-18",
            [
                make_metric_event(name="brooklet.events_produced", value=3.0),
                make_metric_event(name="brooklet.test.count", value=5.0),
            ],
        )

        results = list(scan_metrics(harness_dir=str(tmp_path)))
        assert len(results) == 2
        names = [r["name"] for r in results]
        assert "brooklet.events_produced" in names
        assert "brooklet.test.count" in names

    def test_scan_warns_when_metrics_dir_missing(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="brooklet.contrib.otel"):
            results = list(scan_metrics(harness_dir=str(tmp_path)))
        assert results == []
        assert "metrics directory not found" in caplog.text


class TestScanLogs:
    def test_scan_yields_parsed_logs(self, tmp_path):
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        write_logs_file(
            logs_dir,
            "2026-04-18",
            [
                make_log_event(message="first log", severity_text="INFO"),
                make_log_event(message="second log", severity_text="WARN"),
            ],
        )

        results = list(scan_logs(harness_dir=str(tmp_path)))
        assert len(results) == 2
        assert results[0]["message"] == "first log"
        assert results[1]["severity"] == "WARN"

    def test_scan_empty_logs_dir_returns_nothing(self, tmp_path):
        (tmp_path / "logs").mkdir()
        assert list(scan_logs(harness_dir=str(tmp_path))) == []

    def test_scan_warns_when_logs_dir_missing(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="brooklet.contrib.otel"):
            results = list(scan_logs(harness_dir=str(tmp_path)))
        assert results == []
        assert "logs directory not found" in caplog.text

    def test_malformed_jsonl_line_is_skipped_with_warning(self, tmp_path, caplog):
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        (logs_dir / "2026-04-18.jsonl").write_text(
            "not-valid-json\n" + json.dumps(make_log_event(message="valid")) + "\n"
        )

        with caplog.at_level(logging.WARNING, logger="brooklet.contrib.otel"):
            results = list(scan_logs(harness_dir=str(tmp_path)))
        assert len(results) == 1
        assert results[0]["message"] == "valid"
        assert "malformed JSONL line skipped" in caplog.text


# ---------------------------------------------------------------------------
# Layer 3: CLI plugin
# ---------------------------------------------------------------------------


class TestOtelCLI:
    def test_traces_command_prints_output(self, tmp_path):
        traces_dir = tmp_path / "traces"
        traces_dir.mkdir()
        write_traces_file(
            traces_dir,
            "2026-04-18",
            [make_root_trace_event(name="cli-test-span")],
        )

        runner = CliRunner()
        result = runner.invoke(
            app,
            ["otel", "traces", str(tmp_path), "--stream-dir", str(tmp_path / "stream")],
        )
        assert result.exit_code == 0, result.output
        assert "cli-test-span" in result.output

    def test_metrics_command_prints_output(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()
        write_metrics_file(
            metrics_dir,
            "2026-04-18",
            [make_metric_event(name="brooklet.events_produced", value=7.0)],
        )

        runner = CliRunner()
        result = runner.invoke(
            app,
            ["otel", "metrics", str(tmp_path), "--stream-dir", str(tmp_path / "stream")],
        )
        assert result.exit_code == 0, result.output
        assert "brooklet.events_produced" in result.output

    def test_metrics_command_renders_histogram(self, tmp_path):
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()
        write_metrics_file(
            metrics_dir,
            "2026-04-18",
            [make_metric_event(name="brooklet.latency", value=5.0, metric_type="histogram")],
        )

        runner = CliRunner()
        result = runner.invoke(
            app,
            ["otel", "metrics", str(tmp_path), "--stream-dir", str(tmp_path / "stream")],
        )
        assert result.exit_code == 0, result.output
        assert "count=" in result.output
        assert "sum=" in result.output

    def test_logs_command_renders_log_data(self, tmp_path):
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        write_logs_file(
            logs_dir,
            "2026-04-18",
            [make_log_event(message="hello from test", severity_text="INFO")],
        )

        runner = CliRunner()
        result = runner.invoke(
            app,
            ["otel", "logs", str(tmp_path), "--stream-dir", str(tmp_path / "stream")],
        )
        assert result.exit_code == 0, result.output
        assert "hello from test" in result.output
        assert "INFO" in result.output

    def test_logs_command_exits_cleanly_with_no_data(self, tmp_path):
        (tmp_path / "logs").mkdir()
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["otel", "logs", str(tmp_path), "--stream-dir", str(tmp_path / "stream")],
        )
        assert result.exit_code == 0, result.output

    def test_otel_subcommand_registered(self):
        runner = CliRunner()
        result = runner.invoke(app, ["otel", "--help"])
        assert result.exit_code == 0
        assert "traces" in result.output
        assert "metrics" in result.output
        assert "logs" in result.output
