# ABOUTME: Unit tests for pytest analytics parsing and aggregation
# ABOUTME: Tests Layer 1 (pure functions), Layer 2 (consumer integration), and Layer 3 (rendering)

import pytest

import brooklet as bl
from brooklet.contrib.pytest_analytics import (
    aggregate_run,
    is_test_result,
    main,
    parse_test_event,
    render_cumulative,
    render_run_block,
    scan_runs,
)
from tests.pytest_fixtures import (
    ALL_PASS_EVENTS,
    EMPTY_RUN_EVENTS,
    MULTI_RUN_EVENTS,
    SINGLE_RUN_EVENTS,
    write_run_file,
)


class TestParseTestEvent:
    def test_parse_test_report(self):
        event = {
            "$report_type": "TestReport",
            "nodeid": "tests/test_math.py::test_add",
            "outcome": "passed",
            "when": "call",
            "duration": 0.0032,
        }
        result = parse_test_event(event)
        assert result is not None
        assert result["report_type"] == "TestReport"
        assert result["nodeid"] == "tests/test_math.py::test_add"
        assert result["outcome"] == "passed"
        assert result["when"] == "call"
        assert result["duration"] == 0.0032

    def test_parse_session_start(self):
        event = {"$report_type": "SessionStart", "exitstatus": None}
        result = parse_test_event(event)
        assert result is not None
        assert result["report_type"] == "SessionStart"

    def test_parse_session_finish(self):
        event = {"$report_type": "SessionFinish", "exitstatus": 0}
        result = parse_test_event(event)
        assert result is not None
        assert result["report_type"] == "SessionFinish"

    def test_returns_none_for_unrecognized_type(self):
        event = {"$report_type": "UnknownThing", "data": "whatever"}
        result = parse_test_event(event)
        assert result is None

    def test_returns_none_for_missing_report_type(self):
        event = {"nodeid": "foo", "outcome": "passed"}
        result = parse_test_event(event)
        assert result is None

    def test_missing_fields_default_to_none(self):
        event = {"$report_type": "TestReport"}
        result = parse_test_event(event)
        assert result is not None
        assert result["nodeid"] is None
        assert result["outcome"] is None
        assert result["when"] is None
        assert result["duration"] == 0.0
        assert result["longrepr"] is None

    def test_null_duration_coerced_to_zero(self):
        event = {"$report_type": "TestReport", "nodeid": "t", "duration": None, "when": "call"}
        result = parse_test_event(event)
        assert result["duration"] == 0.0

    def test_string_duration_coerced_to_zero(self):
        event = {"$report_type": "TestReport", "nodeid": "t", "duration": "bad", "when": "call"}
        result = parse_test_event(event)
        assert result["duration"] == 0.0

    def test_parse_failed_test_with_longrepr(self):
        event = {
            "$report_type": "TestReport",
            "nodeid": "tests/test_math.py::test_divide",
            "outcome": "failed",
            "when": "call",
            "duration": 0.002,
            "longrepr": "ZeroDivisionError: division by zero",
        }
        result = parse_test_event(event)
        assert result["longrepr"] == "ZeroDivisionError: division by zero"


class TestIsTestResult:
    def test_test_report_call_is_true(self):
        parsed = {"report_type": "TestReport", "when": "call"}
        assert is_test_result(parsed) is True

    def test_test_report_setup_is_false(self):
        parsed = {"report_type": "TestReport", "when": "setup"}
        assert is_test_result(parsed) is False

    def test_test_report_teardown_is_false(self):
        parsed = {"report_type": "TestReport", "when": "teardown"}
        assert is_test_result(parsed) is False

    def test_session_start_is_false(self):
        parsed = {"report_type": "SessionStart", "when": None}
        assert is_test_result(parsed) is False

    def test_collect_report_is_false(self):
        parsed = {"report_type": "CollectReport", "when": None}
        assert is_test_result(parsed) is False


class TestAggregateRun:
    def test_single_run_counts(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        assert stats.total == 5
        assert stats.passed == 3
        assert stats.failed == 1
        assert stats.skipped == 1
        assert stats.errored == 0

    def test_single_run_duration(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        # Sum of call-phase durations: 0.0032 + 0.0150 + 0.0510 + 0.0024 + 0.0001
        assert abs(stats.duration_s - 0.0717) < 0.0001

    def test_single_run_slowest(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        assert len(stats.slowest) == 5
        assert stats.slowest[0]["nodeid"] == "tests/test_math.py::test_multiply"
        assert stats.slowest[0]["duration"] == 0.0510

    def test_single_run_failures(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        assert len(stats.failures) == 1
        assert stats.failures[0]["nodeid"] == "tests/test_math.py::test_divide"
        assert "ZeroDivisionError" in stats.failures[0]["longrepr"]

    def test_all_pass_run(self):
        stats = aggregate_run("clean", ALL_PASS_EVENTS)
        assert stats.total == 3
        assert stats.passed == 3
        assert stats.failed == 0
        assert stats.failures == []

    def test_empty_run(self):
        stats = aggregate_run("empty", EMPTY_RUN_EVENTS)
        assert stats.total == 0
        assert stats.passed == 0
        assert stats.duration_s == 0.0
        assert stats.slowest == []
        assert stats.failures == []

    def test_run_id_is_set(self):
        stats = aggregate_run("my-run-id", SINGLE_RUN_EVENTS)
        assert stats.run_id == "my-run-id"


class TestRunStatsToDict:
    def test_to_dict_roundtrip(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        d = stats.to_dict()
        assert d["run_id"] == "test-run"
        assert d["total"] == 5
        assert d["passed"] == 3
        assert d["failed"] == 1
        assert d["skipped"] == 1
        assert isinstance(d["slowest"], list)
        assert isinstance(d["failures"], list)


class TestScanRunsSingleFile:
    def test_single_file_yields_one_run(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "results", SINGLE_RUN_EVENTS)

        runs = list(scan_runs(str(reports_dir / "results.jsonl"), mode="single-file"))
        assert len(runs) == 1
        assert runs[0].run_id == "results"
        assert runs[0].total == 5
        assert runs[0].passed == 3

    def test_single_file_run_id_from_filename(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "my-test-run", ALL_PASS_EVENTS)

        runs = list(scan_runs(str(reports_dir / "my-test-run.jsonl"), mode="single-file"))
        assert runs[0].run_id == "my-test-run"


class TestScanRunsGlob:
    def test_glob_yields_one_run_per_file(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        for name, events in MULTI_RUN_EVENTS.items():
            write_run_file(reports_dir, name, events)

        runs = list(scan_runs(str(reports_dir / "run-*.jsonl"), mode="glob"))
        assert len(runs) == 3

    def test_glob_runs_in_lexicographic_order(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        for name, events in MULTI_RUN_EVENTS.items():
            write_run_file(reports_dir, name, events)

        runs = list(scan_runs(str(reports_dir / "run-*.jsonl"), mode="glob"))
        assert [r.run_id for r in runs] == ["run-001", "run-002", "run-003"]

    def test_glob_run_stats_are_independent(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        for name, events in MULTI_RUN_EVENTS.items():
            write_run_file(reports_dir, name, events)

        runs = list(scan_runs(str(reports_dir / "run-*.jsonl"), mode="glob"))
        # run-001: 1 pass, 1 fail
        assert runs[0].passed == 1
        assert runs[0].failed == 1
        # run-002: 2 pass
        assert runs[1].passed == 2
        assert runs[1].failed == 0
        # run-003: 2 pass, 1 skip
        assert runs[2].passed == 2
        assert runs[2].skipped == 1


class TestRenderRunBlock:
    def test_render_includes_run_id(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        output = render_run_block(stats)
        assert "test-run" in output

    def test_render_includes_counts(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        output = render_run_block(stats)
        assert "5 tests" in output
        assert "3 passed" in output
        assert "1 failed" in output
        assert "1 skipped" in output

    def test_render_includes_failures(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        output = render_run_block(stats)
        assert "test_divide" in output

    def test_render_all_pass_no_failures_section(self):
        stats = aggregate_run("clean", ALL_PASS_EVENTS)
        output = render_run_block(stats)
        assert "3 passed" in output
        assert "FAIL" not in output


class TestIntegration:
    """Integration tests — full register → consume → produce roundtrip."""

    def test_register_consume_has_envelope_fields(self, tmp_path):
        """AC-1 integration: registered file yields events with _ts, _seq."""
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "integration", SINGLE_RUN_EVENTS)

        stream = bl.open(str(reports_dir))
        stream.register("pytest/int", str(reports_dir / "integration.jsonl"), "single-file")
        events = list(stream.consume("pytest/int", group="int-test"))

        assert len(events) > 0
        for event in events:
            assert "_ts" in event
            assert "_seq" in event

    def test_produce_summary_to_derived_topic(self, tmp_path):
        """AC-6 integration: produce stats to a derived topic, then consume them."""
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "prod-test", SINGLE_RUN_EVENTS)

        stream = bl.open(str(reports_dir))
        runs = list(scan_runs(str(reports_dir / "prod-test.jsonl"), mode="single-file"))
        assert len(runs) == 1

        # Produce summary to derived topic
        stream.produce("pytest/summaries", runs[0].to_dict(), source="pytest-analytics")

        # Consume the derived topic
        summaries = list(stream.consume("pytest/summaries", group="verify"))
        assert len(summaries) == 1
        assert summaries[0]["total"] == 5
        assert summaries[0]["passed"] == 3


class TestAggregateRunErrorOutcome:
    """Test that the "error" outcome is counted correctly."""

    def test_error_outcome_counted(self):
        events = [
            {
                "$report_type": "TestReport",
                "nodeid": "tests/test_setup.py::test_broken_fixture",
                "outcome": "error",
                "when": "call",
                "duration": 0.001,
            },
        ]
        stats = aggregate_run("error-run", events)
        assert stats.errored == 1
        assert stats.total == 1
        assert stats.passed == 0


class TestRenderCumulative:
    """Test render_cumulative — aggregate totals across runs."""

    def test_empty_runs(self):
        output = render_cumulative([])
        assert output == "No runs processed."

    def test_multi_run_totals(self):
        stats1 = aggregate_run("run-1", SINGLE_RUN_EVENTS)
        stats2 = aggregate_run("run-2", ALL_PASS_EVENTS)
        output = render_cumulative([stats1, stats2])
        assert "2 runs totals" in output
        assert "8 tests" in output
        assert "6 passed" in output


class TestScanRunsValidation:
    """Test scan_runs input validation."""

    def test_invalid_mode_raises_value_error(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "test", SINGLE_RUN_EVENTS)

        with pytest.raises(ValueError, match="mode must be one of"):
            list(scan_runs(str(reports_dir / "test.jsonl"), mode="multi-file"))

    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Report log not found"):
            list(scan_runs(str(tmp_path / "nonexistent.jsonl"), mode="single-file"))


class TestMainCLI:
    """Test main() CLI entry point."""

    def test_single_file_prints_output(self, tmp_path, capsys):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "cli-test", SINGLE_RUN_EVENTS)

        main([str(reports_dir / "cli-test.jsonl")])
        captured = capsys.readouterr()
        assert "cli-test" in captured.out
        assert "5 tests" in captured.out
        assert "1 runs totals" in captured.out

    def test_glob_mode_prints_multiple_runs(self, tmp_path, capsys):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        for name, events in MULTI_RUN_EVENTS.items():
            write_run_file(reports_dir, name, events)

        main([str(reports_dir / "run-*.jsonl"), "--glob"])
        captured = capsys.readouterr()
        assert "run-001" in captured.out
        assert "run-002" in captured.out
        assert "run-003" in captured.out
        assert "3 runs totals" in captured.out

    def test_output_flag_produces_to_topic(self, tmp_path, capsys):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "output-test", SINGLE_RUN_EVENTS)

        main([
            str(reports_dir / "output-test.jsonl"),
            "--output", "pytest/summaries",
        ])

        # Verify events were produced
        stream = bl.open(str(reports_dir))
        summaries = list(stream.consume("pytest/summaries", group="verify"))
        assert len(summaries) == 1
        assert summaries[0]["total"] == 5

    def test_missing_file_exits_with_error(self, tmp_path):
        with pytest.raises(SystemExit):
            main([str(tmp_path / "nope.jsonl")])
