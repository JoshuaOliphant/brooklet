# ABOUTME: Tests for the Stream orchestrator — the main brooklet API surface
# ABOUTME: Covers directory creation, topic management, and register-then-consume integration

import json
import os
import warnings

import pytest

import brooklet
from brooklet.core.stream import Stream


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

    def test_consume_sets_src_from_topic(self, tmp_stream_dir, tmp_path):
        """Events consumed via Stream have _src set to the topic name."""
        jsonl_path = tmp_path / "events.jsonl"
        with open(jsonl_path, "w") as f:
            f.write(json.dumps({"type": "hello"}) + "\n")

        stream = Stream(str(tmp_stream_dir))
        stream.register("my-topic", path=str(jsonl_path), mode="single-file")

        events = list(stream.consume("my-topic", group="test"))
        assert len(events) == 1
        assert events[0]["_src"] == "my-topic"


class TestStreamRelativePathResolution:
    """Reproduce brooklet-uoj: relative paths in sources.json break cross-cwd consumption."""

    def test_produce_with_relative_stream_dir_stores_absolute_path(self, tmp_path, monkeypatch):
        """Stream opened with a relative path must store absolute paths in sources.json.

        Reproduces the bug: if Stream is constructed with Path(".") or a relative path,
        produce() stored relative glob patterns like "demo/data-*.jsonl". A consumer
        opened from a different cwd would then fail to find the files.
        """
        stream_dir = tmp_path / "mystream"
        stream_dir.mkdir()

        # Change cwd to tmp_path so "mystream" is a valid relative path
        monkeypatch.chdir(tmp_path)

        # Open the stream using a relative path (simulates `brooklet produce --stream-dir .`)
        stream = brooklet.open("mystream")
        stream.produce("demo", {"a": 1})

        # The stored path must be absolute so it resolves regardless of cwd
        source = stream._registry.get("demo")
        assert os.path.isabs(source["path"]), (
            f"Expected absolute path in sources.json, got: {source['path']!r}"
        )

    def test_consume_from_different_cwd_after_relative_open(self, tmp_path, monkeypatch):
        """Consumer opened from a different cwd must find files produced via relative stream dir.

        This is the exact failure scenario from the bug report:
        1. produce() run from stream_dir (cwd == stream_dir)
        2. consumer run from a completely different cwd
        3. With relative paths stored, the consumer emits a UserWarning and yields no events.
        """
        stream_dir = tmp_path / "mystream"
        stream_dir.mkdir()

        # Step 1: produce from within the stream dir (cwd = stream_dir)
        monkeypatch.chdir(stream_dir)
        stream = brooklet.open(".")
        stream.produce("demo", {"a": 1})

        # Step 2: switch cwd to somewhere completely unrelated
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        monkeypatch.chdir(other_dir)

        # Step 3: open the stream using its absolute path and consume — must find events
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any UserWarning about missing files = bug
            stream2 = brooklet.open(str(stream_dir))
            events = list(stream2.consume("demo", group="test"))

        assert len(events) == 1, (
            "Consumer found no events — relative path in sources.json"
            " not resolved against stream_dir"
        )


class TestStreamRead:
    def test_read_yields_all_events_without_advancing_offset(self, tmp_path):
        """read() is a full scan: repeated calls re-read the same events."""
        stream = brooklet.open(tmp_path)
        stream.produce("t", {"n": 1})
        stream.produce("t", {"n": 2})

        first = [e["n"] for e in stream.read("t")]
        second = [e["n"] for e in stream.read("t")]

        assert first == [1, 2]
        assert second == [1, 2]

    def test_read_unregistered_topic_raises_keyerror(self, tmp_path):
        stream = brooklet.open(tmp_path)
        with pytest.raises(KeyError):
            list(stream.read("nope"))

    def test_read_seq_monotonic_across_mixed_sources(self, tmp_path):
        """A legacy line after a persisted-_seq line is numbered above it."""
        external = tmp_path / "ext.jsonl"
        external.write_text(
            json.dumps({"_seq": 100, "type": "persisted"})
            + "\n"
            + json.dumps({"type": "legacy"})
            + "\n"
        )
        stream = brooklet.open(tmp_path)
        stream.register("ext", str(external), "single-file")

        seqs = [e["_seq"] for e in stream.read("ext")]
        assert seqs[0] == 100
        assert seqs[1] > 100

    def test_read_error_callback_invoked_on_unreadable_file(self, tmp_path):
        stream = brooklet.open(tmp_path)
        stream.register("ghost", str(tmp_path / "missing.jsonl"), "single-file")

        seen: list[tuple[str, OSError]] = []
        events = list(stream.read("ghost", on_read_error=lambda fp, e: seen.append((fp, e))))

        assert events == []
        assert len(seen) == 1
        assert seen[0][0].endswith("missing.jsonl")
        assert isinstance(seen[0][1], OSError)

    def test_read_logs_warning_on_unreadable_file_by_default(self, tmp_path, caplog):
        """With no callback, an unreadable file is logged and skipped, not raised."""
        stream = brooklet.open(tmp_path)
        stream.register("ghost", str(tmp_path / "missing.jsonl"), "single-file")

        with caplog.at_level("WARNING", logger="brooklet"):
            events = list(stream.read("ghost"))

        assert events == []
        assert any("Cannot read" in r.message for r in caplog.records)
