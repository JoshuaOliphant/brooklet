# ABOUTME: Step definitions for stream.produce() BDD tests
# ABOUTME: Stubs generated from AC-1 through AC-12 — implement TODO markers

import json
from datetime import datetime
from pathlib import Path

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("../features/produce.feature")


# ---------------------------------------------------------------------------
# Shared state container
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx():
    """Mutable context dict for sharing state between steps."""
    return {
        "events_produced": [],
        "consumed_events": [],
        "error": None,
        "original_lines": [],
    }


# ---------------------------------------------------------------------------
# Given steps
# ---------------------------------------------------------------------------


@given(parsers.parse('{count:d} events have been produced to topic "{topic}"'))
def given_n_events_produced(stream, ctx, count, topic):
    """Produce N events to a topic for setup."""
    for i in range(count):
        event = {"type": f"event-{i + 1}", "index": i + 1}
        stream.produce(topic, event)
        ctx["events_produced"].append(event)
    # Capture current file state for AC-5 (append atomicity check)
    stream_dir = Path(stream._path)
    data_file = stream_dir / topic / "data.jsonl"
    if data_file.exists():
        ctx["original_lines"] = data_file.read_text().splitlines()


@given(parsers.parse('an external source registered as "{name}"'))
def given_external_source(stream, tmp_path, name):
    """Register an external source to set up name collision test."""
    dummy = tmp_path / "external.jsonl"
    dummy.write_text('{"type": "external"}\n')
    stream.register(name, path=str(dummy), mode="single-file")


# ---------------------------------------------------------------------------
# When steps
# ---------------------------------------------------------------------------


@when(parsers.parse('the user produces an event to topic "{topic}" with type "{event_type}"'))
def when_produce_event(stream, ctx, topic, event_type):
    """Produce a single event with a type field."""
    event = {"type": event_type}
    stream.produce(topic, event)
    ctx["events_produced"].append(event)


@when(
    parsers.parse(
        'the user produces an event to topic "{topic}"'
        ' with type "{event_type}" and source "{source}"'
    )
)
def when_produce_event_with_source(stream, ctx, topic, event_type, source):
    """Produce a single event with a type field and source."""
    event = {"type": event_type}
    stream.produce(topic, event, source=source)
    ctx["events_produced"].append(event)


@when(parsers.parse('the user produces an event with _ts "{ts}" to topic "{topic}"'))
def when_produce_event_with_ts(stream, ctx, topic, ts):
    """Produce an event with a pre-set _ts field."""
    event = {"type": "with-ts", "_ts": ts}
    stream.produce(topic, event)
    ctx["events_produced"].append(event)


@when(parsers.parse('the user tries to produce to topic "{topic}"'))
def when_try_produce_to_topic(stream, ctx, topic):
    """Attempt to produce to a topic, capturing any error."""
    try:
        stream.produce(topic, {"type": "test"})
    except (ValueError, TypeError) as e:
        ctx["error"] = e


@when(parsers.parse('the user tries to produce a string "{value}" to topic "{topic}"'))
def when_try_produce_non_dict(stream, ctx, value, topic):
    """Attempt to produce a non-dict value."""
    try:
        stream.produce(topic, value)
    except TypeError as e:
        ctx["error"] = e


@when(parsers.parse('a consumer reads from "{topic}" with group "{group}"'))
def when_consume_from_topic(stream, ctx, topic, group):
    """Consume all events from a topic."""
    ctx["consumed_events"] = list(stream.consume(topic, group=group))


# ---------------------------------------------------------------------------
# Then steps
# ---------------------------------------------------------------------------


@then(parsers.parse('a directory "{path}" exists inside the stream directory'))
def then_directory_exists(stream, path):
    """Verify a directory was created."""
    full_path = Path(stream._path) / path
    assert full_path.is_dir(), f"Expected directory at {full_path}"


@then(parsers.parse('a file "{path}" exists inside the stream directory'))
def then_file_exists(stream, path):
    """Verify a file was created."""
    full_path = Path(stream._path) / path
    assert full_path.is_file(), f"Expected file at {full_path}"


@then(parsers.parse('the file "{path}" contains exactly {count:d} JSON line'))
def then_file_has_n_lines_singular(stream, path, count):
    """Verify the number of JSON lines in a file (singular form)."""
    full_path = Path(stream._path) / path
    lines = [line for line in full_path.read_text().splitlines() if line.strip()]
    assert len(lines) == count, f"Expected {count} lines, got {len(lines)}"


@then(parsers.parse('the file "{path}" contains exactly {count:d} JSON lines'))
def then_file_has_n_lines_plural(stream, path, count):
    """Verify the number of JSON lines in a file (plural form)."""
    full_path = Path(stream._path) / path
    lines = [line for line in full_path.read_text().splitlines() if line.strip()]
    assert len(lines) == count, f"Expected {count} lines, got {len(lines)}"


@then(parsers.parse('the JSON line contains the original payload field type "{expected}"'))
def then_payload_contains_type(stream, expected):
    """Verify the produced event contains the original type field."""
    # Read the last written topic's data file
    # Find the most recently modified data.jsonl
    data_files = list(Path(stream._path).rglob("data.jsonl"))
    assert data_files, "No data.jsonl files found"
    last_line = data_files[0].read_text().splitlines()[-1]
    event = json.loads(last_line)
    assert event["type"] == expected


@then("the written event contains a valid ISO 8601 timestamp in _ts")
def then_event_has_valid_ts(stream):
    """Verify _ts is a valid ISO 8601 timestamp."""
    data_files = list(Path(stream._path).rglob("data.jsonl"))
    last_line = data_files[-1].read_text().splitlines()[-1]
    event = json.loads(last_line)
    assert "_ts" in event
    datetime.fromisoformat(event["_ts"])


@then(parsers.parse("the written event contains _seq set to {seq:d}"))
def then_event_has_seq(stream, seq):
    """Verify _seq value in the last written event."""
    data_files = list(Path(stream._path).rglob("data.jsonl"))
    last_line = data_files[-1].read_text().splitlines()[-1]
    event = json.loads(last_line)
    assert event["_seq"] == seq


@then(parsers.parse('the written event contains _src set to "{src}"'))
def then_event_has_src(stream, src):
    """Verify _src value in the last written event."""
    data_files = list(Path(stream._path).rglob("data.jsonl"))
    last_line = data_files[-1].read_text().splitlines()[-1]
    event = json.loads(last_line)
    assert event["_src"] == src


@then(parsers.parse('the last written event has _seq set to {seq:d}'))
def then_last_event_has_seq(stream, seq):
    """Verify _seq value in the last line of the most recent data file."""
    data_files = list(Path(stream._path).rglob("data.jsonl"))
    last_line = data_files[-1].read_text().splitlines()[-1]
    event = json.loads(last_line)
    assert event["_seq"] == seq


@then(parsers.parse('the written event contains _ts "{expected_ts}"'))
def then_event_has_specific_ts(stream, expected_ts):
    """Verify _ts was preserved (not overwritten)."""
    data_files = list(Path(stream._path).rglob("data.jsonl"))
    last_line = data_files[-1].read_text().splitlines()[-1]
    event = json.loads(last_line)
    assert event["_ts"] == expected_ts


@then("the first 2 lines are unchanged")
def then_first_lines_unchanged(stream, ctx):
    """Verify that appending didn't modify existing lines."""
    data_files = list(Path(stream._path).rglob("data.jsonl"))
    current_lines = data_files[-1].read_text().splitlines()
    for i, original in enumerate(ctx["original_lines"]):
        assert current_lines[i] == original, f"Line {i} was modified"


@then(parsers.parse('consuming "{topic}" with group "{group}" yields the produced event'))
def then_consuming_yields_event(stream, topic, group):
    """Verify consuming the auto-registered topic returns the produced event."""
    events = list(stream.consume(topic, group=group))
    assert len(events) >= 1
    assert events[-1]["type"] is not None


@then("all 3 events are yielded with correct envelope fields")
def then_all_events_yielded(ctx):
    """Verify all produced events were consumed with envelope fields."""
    assert len(ctx["consumed_events"]) == 3
    for i, event in enumerate(ctx["consumed_events"]):
        assert event["_seq"] == i + 1
        assert "_ts" in event


@then(parsers.parse('consuming again from "{topic}" with group "{group}" yields 0 events'))
def then_second_consume_empty(stream, topic, group):
    """Verify second consumption with same group yields nothing."""
    events = list(stream.consume(topic, group=group))
    assert len(events) == 0


@then(parsers.parse('a ValueError is raised with message containing "{substring}"'))
def then_value_error_with_message(ctx, substring):
    """Verify a ValueError was raised with expected message."""
    assert ctx["error"] is not None, "Expected ValueError but no error was raised"
    assert isinstance(ctx["error"], ValueError), f"Expected ValueError, got {type(ctx['error'])}"
    assert substring in str(ctx["error"]), f"Expected '{substring}' in '{ctx['error']}'"


@then("a TypeError is raised")
def then_type_error_raised(ctx):
    """Verify a TypeError was raised."""
    assert ctx["error"] is not None, "Expected TypeError but no error was raised"
    assert isinstance(ctx["error"], TypeError), f"Expected TypeError, got {type(ctx['error'])}"
