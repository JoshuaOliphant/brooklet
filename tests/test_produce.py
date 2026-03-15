# ABOUTME: Unit tests for stream.produce() and supporting functions
# ABOUTME: Covers envelope serialization, registry local topics, produce (AC-1–AC-12)

import json

import pytest

import brooklet
from brooklet.envelope import serialize

# ---------------------------------------------------------------------------
# serialize() tests (Step 1)
# ---------------------------------------------------------------------------


class TestSerialize:
    """Tests for envelope.serialize() — dict to JSON line with envelope fields."""

    def test_serialize_adds_envelope(self):
        """AC-2: serialize injects _ts, _seq into the output."""
        result = serialize({"type": "hello"}, seq=1)
        event = json.loads(result)
        assert "_ts" in event
        assert event["_seq"] == 1
        assert event["type"] == "hello"
        assert result.endswith("\n")

    def test_serialize_preserves_existing_ts(self):
        """AC-4: existing _ts in the event dict is not overwritten."""
        ts = "2026-01-01T00:00:00Z"
        result = serialize({"type": "test", "_ts": ts}, seq=1)
        event = json.loads(result)
        assert event["_ts"] == ts

    def test_serialize_seq_always_overwritten(self):
        """AC-4: _seq is always set by brooklet, even if present in input."""
        result = serialize({"type": "test", "_seq": 999}, seq=5)
        event = json.loads(result)
        assert event["_seq"] == 5

    def test_serialize_source_param(self):
        """AC-8: source parameter sets _src if not already present."""
        result = serialize({"type": "test"}, seq=1, source="my-source")
        event = json.loads(result)
        assert event["_src"] == "my-source"

    def test_serialize_preserves_existing_src(self):
        """_src in the event dict is preserved over source param."""
        result = serialize({"type": "test", "_src": "original"}, seq=1, source="override")
        event = json.loads(result)
        assert event["_src"] == "original"

    def test_serialize_no_source(self):
        """No _src field when source param is None and not in event."""
        result = serialize({"type": "test"}, seq=1)
        event = json.loads(result)
        assert "_src" not in event


# ---------------------------------------------------------------------------
# Registry local topic tests (Step 2)
# ---------------------------------------------------------------------------


class TestRegistryLocalTopics:
    """Tests for registry.register_local() and is_external()."""

    def test_produce_auto_registers_topic(self, tmp_path):
        """AC-6: producing to a topic adds it to registry."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("my-topic", {"type": "test"})
        assert "my-topic" in s.topics()

    def test_produce_errors_on_external_name_collision(self, tmp_path):
        """AC-9: producing to a registered external topic raises ValueError."""
        s = brooklet.open(str(tmp_path / "streams"))
        dummy = tmp_path / "external.jsonl"
        dummy.write_text('{"type": "external"}\n')
        s.register("taken", path=str(dummy), mode="single-file")

        with pytest.raises(ValueError, match="already registered"):
            s.produce("taken", {"type": "test"})

    def test_produce_idempotent_local_registration(self, tmp_path):
        """Producing twice to same topic doesn't error or duplicate."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("repeat", {"type": "first"})
        s.produce("repeat", {"type": "second"})
        assert s.topics().count("repeat") == 1


# ---------------------------------------------------------------------------
# stream.produce() tests (Step 3)
# ---------------------------------------------------------------------------


class TestProduce:
    """Tests for Stream.produce() — the write path."""

    def test_produce_creates_directory_and_file(self, tmp_path):
        """AC-1: produce creates topic dir and data.jsonl."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("my-topic", {"type": "hello"})

        topic_dir = tmp_path / "streams" / "my-topic"
        data_file = topic_dir / "data.jsonl"
        assert topic_dir.is_dir()
        assert data_file.is_file()

    def test_produce_envelope_fields(self, tmp_path):
        """AC-2: produced events have _ts, _seq, and payload preserved."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("events", {"type": "hello"}, source="test-src")

        data_file = tmp_path / "streams" / "events" / "data.jsonl"
        event = json.loads(data_file.read_text().strip())
        assert "_ts" in event
        assert event["_seq"] == 1
        assert event["_src"] == "test-src"
        assert event["type"] == "hello"

    def test_produce_monotonic_seq(self, tmp_path):
        """AC-3: _seq increments monotonically across produces."""
        s = brooklet.open(str(tmp_path / "streams"))
        for i in range(4):
            s.produce("counter", {"type": f"event-{i}"})

        data_file = tmp_path / "streams" / "counter" / "data.jsonl"
        lines = data_file.read_text().strip().splitlines()
        for i, line in enumerate(lines):
            event = json.loads(line)
            assert event["_seq"] == i + 1

    def test_produce_preserves_existing_ts(self, tmp_path):
        """AC-4: existing _ts in the event is preserved."""
        s = brooklet.open(str(tmp_path / "streams"))
        ts = "2026-01-01T00:00:00Z"
        s.produce("preserve", {"type": "test", "_ts": ts})

        data_file = tmp_path / "streams" / "preserve" / "data.jsonl"
        event = json.loads(data_file.read_text().strip())
        assert event["_ts"] == ts

    def test_produce_atomic_append(self, tmp_path):
        """AC-5: appending doesn't modify existing lines."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("append", {"type": "first"})
        s.produce("append", {"type": "second"})

        data_file = tmp_path / "streams" / "append" / "data.jsonl"
        first_two = data_file.read_text().splitlines()[:2]

        s.produce("append", {"type": "third"})
        lines_after = data_file.read_text().splitlines()
        assert lines_after[:2] == first_two
        assert len(lines_after) == 3

    def test_produce_appears_in_topics(self, tmp_path):
        """AC-6: produced topic appears in stream.topics()."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("scout/stats", {"type": "metric"})
        assert "scout/stats" in s.topics()

    def test_produce_consume_roundtrip(self, tmp_path):
        """AC-7: produced events can be consumed back."""
        s = brooklet.open(str(tmp_path / "streams"))
        for i in range(3):
            s.produce("output", {"type": f"event-{i}"})

        events = list(s.consume("output", group="reader"))
        assert len(events) == 3
        for i, event in enumerate(events):
            assert event["_seq"] == i + 1
            assert "_ts" in event

        # Second read with same group yields nothing
        assert list(s.consume("output", group="reader")) == []

    def test_produce_with_source(self, tmp_path):
        """AC-8: source parameter sets _src on produced event."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("sourced", {"type": "test"}, source="scout")

        data_file = tmp_path / "streams" / "sourced" / "data.jsonl"
        event = json.loads(data_file.read_text().strip())
        assert event["_src"] == "scout"

    def test_produce_name_collision_with_external(self, tmp_path):
        """AC-9: producing to external topic name raises ValueError."""
        s = brooklet.open(str(tmp_path / "streams"))
        dummy = tmp_path / "external.jsonl"
        dummy.write_text('{"type": "external"}\n')
        s.register("claude/history", path=str(dummy), mode="single-file")

        with pytest.raises(ValueError, match="already registered"):
            s.produce("claude/history", {"type": "test"})

    def test_produce_nested_path_names(self, tmp_path):
        """AC-10: path-style names create nested directories."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("scout/session-stats", {"type": "nested"})

        nested_dir = tmp_path / "streams" / "scout" / "session-stats"
        assert nested_dir.is_dir()
        assert (nested_dir / "data.jsonl").is_file()

    def test_produce_rejects_path_traversal(self, tmp_path):
        """AC-11: topic names with .. are rejected."""
        s = brooklet.open(str(tmp_path / "streams"))
        with pytest.raises(ValueError, match="path traversal"):
            s.produce("../escape", {"type": "test"})

    def test_produce_rejects_non_dict(self, tmp_path):
        """AC-12: producing non-dict raises TypeError."""
        s = brooklet.open(str(tmp_path / "streams"))
        with pytest.raises(TypeError):
            s.produce("bad", "not a dict")
