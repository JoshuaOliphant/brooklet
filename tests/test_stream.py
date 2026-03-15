# ABOUTME: Tests for the Stream orchestrator — the main brooklet API surface
# ABOUTME: Covers directory creation, topic management, and register-then-consume integration

import json

import brooklet
from brooklet.stream import Stream


class TestStream:
    def test_open_creates_brooklet_dir(self, tmp_stream_dir):
        """Opening a stream creates .brooklet/ metadata directory."""
        Stream(str(tmp_stream_dir))
        assert (tmp_stream_dir / ".brooklet").is_dir()

    def test_open_idempotent(self, tmp_stream_dir):
        """Opening twice doesn't error."""
        Stream(str(tmp_stream_dir))
        Stream(str(tmp_stream_dir))

    def test_topics_empty(self, tmp_stream_dir):
        """No registrations returns empty list."""
        stream = Stream(str(tmp_stream_dir))
        assert stream.topics() == []

    def test_topics_after_register(self, tmp_stream_dir):
        """Returns registered topic names."""
        stream = Stream(str(tmp_stream_dir))
        stream.register("alpha", path="/tmp/a.jsonl", mode="single-file")
        stream.register("beta", path="/tmp/b.jsonl", mode="single-file")
        assert sorted(stream.topics()) == ["alpha", "beta"]

    def test_register_then_consume(self, tmp_stream_dir, tmp_path):
        """Integration: register a real JSONL file, consume events."""
        # Create a JSONL file with events
        jsonl_path = tmp_path / "real_events.jsonl"
        events = [
            {"type": "login", "user": "alice"},
            {"type": "action", "user": "alice", "action": "click"},
        ]
        with open(jsonl_path, "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

        stream = Stream(str(tmp_stream_dir))
        stream.register("user-events", path=str(jsonl_path), mode="single-file")

        consumed = list(stream.consume("user-events", group="test"))
        assert len(consumed) == 2
        assert consumed[0]["type"] == "login"
        assert consumed[0]["user"] == "alice"
        assert consumed[0]["_seq"] == 1
        assert consumed[1]["_seq"] == 2

    def test_brooklet_open_function(self, tmp_stream_dir):
        """brooklet.open() returns a Stream instance."""
        stream = brooklet.open(str(tmp_stream_dir))
        assert isinstance(stream, Stream)

    def test_consume_nonexistent_topic_raises(self, tmp_stream_dir):
        """Consuming an unregistered topic raises KeyError."""
        import pytest

        stream = Stream(str(tmp_stream_dir))
        with pytest.raises(KeyError):
            list(stream.consume("nonexistent", group="test"))

    def test_consume_twice_same_group(self, tmp_stream_dir, tmp_path):
        """Second consumption with same group yields no events."""
        jsonl_path = tmp_path / "events.jsonl"
        with open(jsonl_path, "w") as f:
            f.write(json.dumps({"type": "hello"}) + "\n")

        stream = Stream(str(tmp_stream_dir))
        stream.register("t", path=str(jsonl_path), mode="single-file")

        events1 = list(stream.consume("t", group="g"))
        events2 = list(stream.consume("t", group="g"))

        assert len(events1) == 1
        assert len(events2) == 0
