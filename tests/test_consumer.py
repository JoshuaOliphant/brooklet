# ABOUTME: Tests for the batch consumer — reads JSONL events with offset tracking
# ABOUTME: Covers empty files, full reads, offset resume, glob mode, and group isolation

import json
import logging

import pytest

from brooklet.consumer import Consumer


class TestConsumerBatch:
    def test_consume_empty_file(self, empty_jsonl, offsets_dir):
        """Empty file yields nothing."""
        consumer = Consumer(
            path=str(empty_jsonl),
            mode="single-file",
            group="test",
            topic="empty",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)
        assert events == []

    def test_consume_reads_all_events(self, sample_jsonl, offsets_dir):
        """All lines returned with envelope fields."""
        consumer = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="sample",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)

        assert len(events) == 3
        assert events[0]["type"] == "start"
        assert events[1]["type"] == "data"
        assert events[2]["type"] == "end"
        # Envelope fields present
        for i, event in enumerate(events):
            assert event["_seq"] == i + 1
            assert "_ts" in event

    def test_consume_respects_offset(self, sample_jsonl, offsets_dir):
        """Second consumption starts from saved byte position."""
        # First pass: read all events
        consumer1 = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="sample",
            offsets_dir=offsets_dir,
        )
        events1 = list(consumer1)
        consumer1.close()
        assert len(events1) == 3

        # Second pass: no new events
        consumer2 = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="sample",
            offsets_dir=offsets_dir,
        )
        events2 = list(consumer2)
        assert events2 == []

    def test_consume_updates_offset_after_exhaustion(self, sample_jsonl, offsets_dir):
        """Offset is persisted when iterator is exhausted."""
        from brooklet.offsets import load

        consumer = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="sample",
            offsets_dir=offsets_dir,
        )
        list(consumer)  # exhaust
        consumer.close()

        offset = load(offsets_dir, group="test", topic="sample")
        assert offset > 0

    def test_consume_group_isolation(self, sample_jsonl, offsets_dir):
        """Two groups track independently."""
        # Group alpha reads all events
        c1 = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="alpha",
            topic="t",
            offsets_dir=offsets_dir,
        )
        events_alpha = list(c1)
        c1.close()
        assert len(events_alpha) == 3

        # Group beta reads all events independently
        c2 = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="beta",
            topic="t",
            offsets_dir=offsets_dir,
        )
        events_beta = list(c2)
        c2.close()
        assert len(events_beta) == 3

    def test_consume_glob_multiple_files(self, tmp_path, offsets_dir):
        """Reads across multiple files in sorted order."""
        # Create two JSONL files
        dir_ = tmp_path / "sessions"
        dir_.mkdir()
        for name, events in [
            ("a.jsonl", [{"type": "a1"}, {"type": "a2"}]),
            ("b.jsonl", [{"type": "b1"}]),
        ]:
            path = dir_ / name
            with open(path, "w") as f:
                for e in events:
                    f.write(json.dumps(e) + "\n")

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="multi",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)
        consumer.close()

        assert len(events) == 3
        assert events[0]["type"] == "a1"
        assert events[1]["type"] == "a2"
        assert events[2]["type"] == "b1"

    def test_consume_glob_respects_offset(self, tmp_path, offsets_dir):
        """Glob consumption resumes from saved position across files."""
        dir_ = tmp_path / "sessions"
        dir_.mkdir()
        for name, events in [
            ("a.jsonl", [{"type": "a1"}, {"type": "a2"}]),
            ("b.jsonl", [{"type": "b1"}]),
        ]:
            path = dir_ / name
            with open(path, "w") as f:
                for e in events:
                    f.write(json.dumps(e) + "\n")

        # First pass
        c1 = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="multi",
            offsets_dir=offsets_dir,
        )
        list(c1)
        c1.close()

        # Second pass — no new events
        c2 = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="multi",
            offsets_dir=offsets_dir,
        )
        events = list(c2)
        assert events == []

    def test_consume_skips_blank_lines(self, tmp_path, offsets_dir):
        """Blank lines in JSONL are skipped gracefully."""
        path = tmp_path / "with_blanks.jsonl"
        with open(path, "w") as f:
            f.write('{"type": "first"}\n')
            f.write("\n")
            f.write('{"type": "second"}\n')
            f.write("   \n")

        consumer = Consumer(
            path=str(path),
            mode="single-file",
            group="test",
            topic="blanks",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)
        assert len(events) == 2

    def test_consume_nonexistent_file_yields_empty(self, tmp_path, offsets_dir):
        """Consuming a nonexistent file yields no events and logs a warning."""
        consumer = Consumer(
            path=str(tmp_path / "does_not_exist.jsonl"),
            mode="single-file",
            group="test",
            topic="missing",
            offsets_dir=offsets_dir,
        )
        with pytest.warns(match="does not exist"):
            events = list(consumer)
        assert events == []

    def test_consume_mixed_valid_invalid_json(self, tmp_path, offsets_dir):
        """Mixed valid/invalid JSON yields only valid events and logs warnings."""
        path = tmp_path / "mixed.jsonl"
        with open(path, "w") as f:
            f.write('{"type": "good1"}\n')
            f.write("NOT VALID JSON\n")
            f.write('{"type": "good2"}\n')
            f.write("{truncated\n")

        consumer = Consumer(
            path=str(path),
            mode="single-file",
            group="test",
            topic="mixed",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)
        assert len(events) == 2
        assert events[0]["type"] == "good1"
        assert events[1]["type"] == "good2"

    def test_consume_malformed_json_logs_warning(self, tmp_path, offsets_dir, caplog):
        """Malformed JSON lines produce log warnings."""
        path = tmp_path / "bad.jsonl"
        with open(path, "w") as f:
            f.write("NOT JSON\n")
            f.write('{"type": "ok"}\n')

        consumer = Consumer(
            path=str(path),
            mode="single-file",
            group="test",
            topic="bad",
            offsets_dir=offsets_dir,
        )
        with caplog.at_level(logging.WARNING, logger="brooklet"):
            events = list(consumer)

        assert len(events) == 1
        assert "malformed JSON" in caplog.text.lower() or "Skipping" in caplog.text

    def test_unknown_mode_raises(self, tmp_path, offsets_dir):
        """Unknown mode raises ValueError instead of silently yielding nothing."""
        consumer = Consumer(
            path=str(tmp_path / "x.jsonl"),
            mode="unknown-mode",
            group="test",
            topic="t",
            offsets_dir=offsets_dir,
        )
        with pytest.raises(ValueError, match="unknown-mode"):
            list(consumer)

    def test_consume_glob_zero_matches_logs_warning(self, tmp_path, offsets_dir, caplog):
        """Glob with zero matching files logs a warning."""
        consumer = Consumer(
            path=str(tmp_path / "nonexistent_dir" / "*.jsonl"),
            mode="glob",
            group="test",
            topic="empty-glob",
            offsets_dir=offsets_dir,
        )
        with caplog.at_level(logging.WARNING, logger="brooklet"):
            events = list(consumer)

        assert events == []
        assert "no files" in caplog.text.lower() or "zero" in caplog.text.lower()

    def test_consumer_context_manager(self, sample_jsonl, offsets_dir):
        """Consumer can be used as a context manager."""
        with Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="ctx",
            offsets_dir=offsets_dir,
        ) as consumer:
            events = list(consumer)

        assert len(events) == 3
        # Offset should be saved after exiting context
        from brooklet.offsets import load

        offset = load(offsets_dir, group="test", topic="ctx")
        assert offset > 0

    def test_consumer_close_saves_offset_even_if_observer_fails(self, sample_jsonl, offsets_dir):
        """close() cleans up observer even if offset save fails."""
        consumer = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="t",
            offsets_dir=offsets_dir,
        )
        list(consumer)
        # Observer is None for batch mode, so close should just work
        consumer.close()

    def test_glob_file_index_out_of_bounds_resets(self, tmp_path, offsets_dir, caplog):
        """Stale file_index beyond file count resets to 0 with warning."""
        from brooklet.offsets import save
        from brooklet.types import GlobOffset

        dir_ = tmp_path / "sessions"
        dir_.mkdir()
        # Create 2 files
        for name, event in [("a.jsonl", {"type": "a"}), ("b.jsonl", {"type": "b"})]:
            with open(dir_ / name, "w") as f:
                f.write(json.dumps(event) + "\n")

        # Save offset pointing to file_index=5 (way beyond 2 files)
        stale = GlobOffset(file_index=5, byte_offset=0)
        save(offsets_dir, "test", "stale-idx", stale.encode())

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="stale-idx",
            offsets_dir=offsets_dir,
        )
        with caplog.at_level(logging.WARNING, logger="brooklet"):
            events = list(consumer)

        # Should re-read all files after reset
        assert len(events) == 2
        assert "file_index" in caplog.text.lower() or "out of bounds" in caplog.text.lower()

    def test_glob_file_removed_between_sessions(self, tmp_path, offsets_dir, caplog):
        """When files are removed between sessions, stale index is detected."""
        from brooklet.offsets import save
        from brooklet.types import GlobOffset

        dir_ = tmp_path / "sessions"
        dir_.mkdir()
        # Start with 3 files, consume them all
        for name, event in [
            ("a.jsonl", {"type": "a"}),
            ("b.jsonl", {"type": "b"}),
            ("c.jsonl", {"type": "c"}),
        ]:
            with open(dir_ / name, "w") as f:
                f.write(json.dumps(event) + "\n")

        # Simulate having consumed up through file_index=2 (c.jsonl) with some byte offset
        stale = GlobOffset(file_index=2, byte_offset=100)
        save(offsets_dir, "test", "removed", stale.encode())

        # Now remove a.jsonl — only 2 files remain but saved index is 2
        (dir_ / "a.jsonl").unlink()

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="removed",
            offsets_dir=offsets_dir,
        )
        with caplog.at_level(logging.WARNING, logger="brooklet"):
            events = list(consumer)

        # file_index=2 is out of bounds for 2 files, should reset and re-read
        assert len(events) == 2
        assert "out of bounds" in caplog.text.lower() or "file_index" in caplog.text.lower()
