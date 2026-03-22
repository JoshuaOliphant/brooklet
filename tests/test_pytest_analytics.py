# ABOUTME: Unit tests for pytest analytics parsing and aggregation
# ABOUTME: Tests Layer 1 (pure functions) and Layer 2 (consumer integration)

from brooklet.contrib.pytest_analytics import aggregate_run, is_test_result, parse_test_event
from tests.pytest_fixtures import (
    ALL_PASS_EVENTS,
    EMPTY_RUN_EVENTS,
    SINGLE_RUN_EVENTS,
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
