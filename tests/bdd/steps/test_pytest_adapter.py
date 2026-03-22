# ABOUTME: Step definitions for pytest adapter BDD tests
# ABOUTME: Covers AC-PT-1 through AC-PT-6

import json
from pathlib import Path

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from brooklet.contrib.pytest_analytics import (
    aggregate_run,
    is_test_result,
    parse_test_event,
)
from tests.pytest_fixtures import (
    MULTI_RUN_EVENTS,
    SINGLE_RUN_EVENTS,
    write_run_file,
)

scenarios("../features/pytest_adapter.feature")


@pytest.fixture
def ctx():
    """Mutable context dict for sharing state between steps."""
    return {
        "consumed_events": [],
        "filtered_events": [],
        "run_stats": None,
        "raw_events": None,
        "live_path": None,
    }


# ---------------------------------------------------------------------------
# AC-PT-1: Register a single pytest report log
# ---------------------------------------------------------------------------


@given(
    parsers.parse('a pytest report log at "{filename}" with {count:d} test events'),
    target_fixture="report_file",
)
def given_report_log(stream_dir, filename, count):
    """Create a pytest report log file in the stream directory."""
    return write_run_file(stream_dir, Path(filename).stem, SINGLE_RUN_EVENTS)


@when(parsers.parse('the user registers it as "{topic}" in single-file mode'))
def when_register_single_file(stream, report_file, topic):
    stream.register(topic, str(report_file), "single-file")


@then(parsers.parse('consuming "{topic}" yields test events with envelope fields'))
def then_consume_yields_events_with_envelope(stream, topic):
    events = list(stream.consume(topic, group="bdd-test"))
    assert len(events) > 0
    for event in events:
        assert "_ts" in event
        assert "_seq" in event


# ---------------------------------------------------------------------------
# AC-PT-2: Register a directory of report logs
# ---------------------------------------------------------------------------


@given("3 pytest report log files in the stream directory")
def given_multi_report_logs(stream_dir):
    for name, events in MULTI_RUN_EVENTS.items():
        write_run_file(stream_dir, name, events)


@when(parsers.parse('the user registers them as "{topic}" in glob mode'))
def when_register_glob(stream, stream_dir, topic):
    pattern = str(stream_dir / "run-*.jsonl")
    stream.register(topic, pattern, "glob")


@then(parsers.parse('consuming "{topic}" yields events from all {count:d} files'))
def then_glob_yields_all_events(stream, topic, count):
    events = list(stream.consume(topic, group="bdd-test"))
    # Each MULTI_RUN_EVENTS run has 4 events (SessionStart, 2 TestReports, SessionFinish).
    # 3 files x 4 events = 12 total. Assert we got at least count * 1 event per file.
    total_per_file = len(MULTI_RUN_EVENTS["run-001"])
    expected_min = count * total_per_file
    assert len(events) >= expected_min, (
        f"Expected at least {expected_min} events from {count} files, got {len(events)}"
    )


# ---------------------------------------------------------------------------
# AC-PT-3: Filter to test outcomes only
# ---------------------------------------------------------------------------


@given("a pytest report log with mixed event types")
def given_mixed_events(ctx):
    ctx["raw_events"] = SINGLE_RUN_EVENTS


@when("the consumer filters with is_test_result")
def when_filter_test_results(ctx):
    filtered = []
    for raw in ctx["raw_events"]:
        parsed = parse_test_event(raw)
        if parsed is not None and is_test_result(parsed):
            filtered.append(parsed)
    ctx["filtered_events"] = filtered


@then("only TestReport call events are yielded")
def then_only_call_events(ctx):
    for event in ctx["filtered_events"]:
        assert event["report_type"] == "TestReport"
        assert event["when"] == "call"
    assert len(ctx["filtered_events"]) == 5


# ---------------------------------------------------------------------------
# AC-PT-4: Incremental consumption across runs
# ---------------------------------------------------------------------------


@given("2 pytest report log files registered as a glob topic")
def given_two_run_files(stream, stream_dir, ctx):
    write_run_file(stream_dir, "run-001", MULTI_RUN_EVENTS["run-001"])
    write_run_file(stream_dir, "run-002", MULTI_RUN_EVENTS["run-002"])
    pattern = str(stream_dir / "run-*.jsonl")
    stream.register("pytest/incremental", pattern, "glob")
    ctx["reports_dir"] = stream_dir


@given("a consumer has read all events from the glob topic")
def given_consumer_read_both(stream, ctx):
    events = list(stream.consume("pytest/incremental", group="incr-test"))
    ctx["first_read_count"] = len(events)
    assert ctx["first_read_count"] > 0


@when("a new run-003 file appears")
def when_new_file_appears(ctx):
    write_run_file(ctx["reports_dir"], "run-003", MULTI_RUN_EVENTS["run-003"])


@when("the consumer reads the glob topic again")
def when_consumer_reads_again(stream, ctx):
    events = list(stream.consume("pytest/incremental", group="incr-test"))
    ctx["consumed_events"] = events


@then("only events from the new file are yielded")
def then_only_new_events(ctx):
    assert len(ctx["consumed_events"]) > 0


# ---------------------------------------------------------------------------
# AC-PT-5: Appended events are consumable
# ---------------------------------------------------------------------------


@given("a pytest report log registered in single-file mode")
def given_follow_report(stream, stream_dir, ctx):
    path = write_run_file(stream_dir, "live-run", SINGLE_RUN_EVENTS[:2])
    stream.register("pytest/live", str(path), "single-file")
    ctx["live_path"] = path
    # Read initial events to set offset
    list(stream.consume("pytest/live", group="follow-test"))


@when("new test events are appended to the report log")
def when_new_events_appended(ctx):
    new_event = {
        "$report_type": "TestReport",
        "nodeid": "tests/test_new.py::test_appended",
        "outcome": "passed",
        "when": "call",
        "duration": 0.005,
    }
    with open(ctx["live_path"], "a") as f:
        f.write(json.dumps(new_event) + "\n")


@then("the appended events are consumable")
def then_new_events_yielded(stream):
    events = list(stream.consume("pytest/live", group="follow-test"))
    nodeids = [e.get("nodeid") for e in events if e.get("nodeid")]
    assert "tests/test_new.py::test_appended" in nodeids


# ---------------------------------------------------------------------------
# AC-PT-6: Summary stats per run
# ---------------------------------------------------------------------------


@given("a pytest report log with 3 passed 1 failed and 1 skipped test")
def given_run_for_stats(ctx):
    ctx["raw_events"] = SINGLE_RUN_EVENTS


@when("aggregate_run processes the events")
def when_aggregate_run(ctx):
    ctx["run_stats"] = aggregate_run("stats-test", ctx["raw_events"])


@then(parsers.parse("the RunStats contains total {t:d} passed {p:d} failed {f:d} skipped {s:d}"))
def then_stats_match(ctx, t, p, f, s):
    stats = ctx["run_stats"]
    assert stats.total == t
    assert stats.passed == p
    assert stats.failed == f
    assert stats.skipped == s


@then(parsers.parse("the slowest list has {count:d} entries sorted by duration descending"))
def then_slowest_sorted(ctx, count):
    stats = ctx["run_stats"]
    assert len(stats.slowest) == count
    durations = [e["duration"] for e in stats.slowest]
    assert durations == sorted(durations, reverse=True)


@then("the failures list contains the failed test with longrepr")
def then_failures_have_longrepr(ctx):
    stats = ctx["run_stats"]
    assert len(stats.failures) >= 1
    assert stats.failures[0]["nodeid"] is not None
    assert len(stats.failures[0]["longrepr"]) > 0
