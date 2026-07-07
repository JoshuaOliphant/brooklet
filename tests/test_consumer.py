# ABOUTME: Tests for the batch consumer — reads JSONL events with offset tracking
# ABOUTME: Covers empty files, full reads, offset resume, glob mode, and group isolation

import json
import logging

import pytest

from brooklet.core.consumer import Consumer


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
        from brooklet.storage.offsets import load

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
        from brooklet.storage.offsets import load

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

    def test_glob_segment_number_out_of_bounds_resets(self, tmp_path, offsets_dir, caplog):
        """Stale segment_number beyond file count resets to 0 with error."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import load, save

        dir_ = tmp_path / "sessions"
        dir_.mkdir()
        # Create 2 files
        for name, event in [("a.jsonl", {"type": "a"}), ("b.jsonl", {"type": "b"})]:
            with open(dir_ / name, "w") as f:
                f.write(json.dumps(event) + "\n")

        # Save offset pointing to segment_number=5 (way beyond 2 files)
        stale = GlobOffset(segment_number=5, byte_offset=0)
        save(offsets_dir, "test", "stale-idx", stale.encode())

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="stale-idx",
            offsets_dir=offsets_dir,
        )
        with caplog.at_level(logging.ERROR, logger="brooklet"):
            events = list(consumer)

        # Should re-read all files after reset
        assert len(events) == 2
        assert events[0]["type"] == "a"
        assert events[1]["type"] == "b"

        # Verify error-level log fired
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(
            "segment_number" in r.message.lower() or "out of bounds" in r.message.lower()
            for r in error_records
        )

        # Verify persisted offset reflects consumption of both files
        raw = load(offsets_dir, "test", "stale-idx")
        persisted = GlobOffset.decode(raw)
        assert persisted.segment_number == 1
        assert persisted.byte_offset > 0

    def test_glob_file_removed_between_sessions(self, tmp_path, offsets_dir, caplog):
        """When files are removed between sessions, stale index is detected."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import load, save

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

        # Simulate having consumed up through segment_number=2 (c.jsonl) with some byte offset
        stale = GlobOffset(segment_number=2, byte_offset=100)
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
        with caplog.at_level(logging.ERROR, logger="brooklet"):
            events = list(consumer)

        # segment_number=2 is out of bounds for 2 files, should reset and re-read
        assert len(events) == 2
        assert events[0]["type"] == "b"
        assert events[1]["type"] == "c"

        # Verify error-level log fired
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(
            "segment_number" in r.message.lower() or "out of bounds" in r.message.lower()
            for r in error_records
        )

        # Verify persisted offset reflects consumption of remaining files
        raw = load(offsets_dir, "test", "removed")
        persisted = GlobOffset.decode(raw)
        assert persisted.segment_number == 1
        assert persisted.byte_offset > 0

    def test_glob_empty_files_with_stale_offset_resets(self, tmp_path, offsets_dir, caplog):
        """Stale offset with no matching files resets to 0 and logs error."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import load, save

        dir_ = tmp_path / "empty_glob"
        dir_.mkdir()

        # Save a stale offset pointing to segment_number=3
        stale = GlobOffset(segment_number=3, byte_offset=42)
        save(offsets_dir, "test", "empty-stale", stale.encode())

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="empty-stale",
            offsets_dir=offsets_dir,
        )
        with caplog.at_level(logging.ERROR, logger="brooklet"):
            events = list(consumer)

        # No events returned, no crash
        assert events == []

        # Verify error about non-zero offset
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(
            "non-zero" in r.message.lower() or "no files" in r.message.lower()
            for r in error_records
        )

        # Verify persisted offset is reset to 0
        raw = load(offsets_dir, "test", "empty-stale")
        persisted = GlobOffset.decode(raw)
        assert persisted.segment_number == 0
        assert persisted.byte_offset == 0

    def test_glob_follow_with_stale_index_resets(self, tmp_path, offsets_dir, caplog):
        """Stale segment_number in follow mode resets and reads all files."""
        import threading
        import time

        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import save

        dir_ = tmp_path / "follow_stale"
        dir_.mkdir()

        # Create 2 files
        for name, event in [("a.jsonl", {"type": "a"}), ("b.jsonl", {"type": "b"})]:
            with open(dir_ / name, "w") as f:
                f.write(json.dumps(event) + "\n")

        # Save stale offset beyond file count
        stale = GlobOffset(segment_number=5, byte_offset=0)
        save(offsets_dir, "test", "follow-stale", stale.encode())

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="follow-stale",
            offsets_dir=offsets_dir,
            follow=True,
        )

        collected = []

        def consume():
            for event in consumer:
                collected.append(event)

        t = threading.Thread(target=consume, daemon=True)
        with caplog.at_level(logging.ERROR, logger="brooklet"):
            t.start()

            # Wait briefly for catch-up to process existing files
            time.sleep(1.0)

            # Append a new event to trigger follow-mode pickup
            with open(dir_ / "b.jsonl", "a") as f:
                f.write(json.dumps({"type": "b2"}) + "\n")

            # Wait for follow mode to pick up the new event
            time.sleep(1.5)

            consumer.close()
            t.join(timeout=5.0)

        # Should have caught up on a + b, plus the appended b2
        types = [e["type"] for e in collected]
        assert "a" in types
        assert "b" in types
        assert "b2" in types

        # Verify error-level reset log fired
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(
            "segment_number" in r.message.lower() or "out of bounds" in r.message.lower()
            for r in error_records
        )


class TestConsumerOffsetSaveDurability:
    """Tests pinning exception-safe offset save contracts for batch modes."""

    def test_glob_batch_saves_offset_on_exception_midway(self, tmp_path, offsets_dir):
        """Glob batch consumer must persist offset when iteration raises mid-flight."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import load

        dir_ = tmp_path / "sessions"
        dir_.mkdir()
        for name, events in [
            ("a.jsonl", [{"type": "a1"}, {"type": "a2"}]),
            ("b.jsonl", [{"type": "b1"}, {"type": "b2"}]),
        ]:
            path = dir_ / name
            with open(path, "w") as f:
                for e in events:
                    f.write(json.dumps(e) + "\n")

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="glob-interrupt",
            offsets_dir=offsets_dir,
        )

        collected = []
        with pytest.raises(RuntimeError, match="simulated interrupt"):
            for event in consumer:
                collected.append(event)
                if len(collected) == 2:
                    # Simulate KeyboardInterrupt/SIGTERM-style bail-out while
                    # the generator is still mid-iteration.
                    raise RuntimeError("simulated interrupt")

        # Offset file must exist with non-zero GlobOffset state so that a
        # restart can resume where we left off.
        raw = load(offsets_dir, "test", "glob-interrupt")
        assert raw > 0
        persisted = GlobOffset.decode(raw)
        assert persisted.byte_offset > 0 or persisted.segment_number > 0

    def test_glob_batch_saves_offset_without_explicit_close(self, tmp_path, offsets_dir):
        """Exhausting a glob batch iterator must save the offset via finally,
        even when the caller never calls close()."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import load

        dir_ = tmp_path / "sessions"
        dir_.mkdir()
        for name, events in [
            ("a.jsonl", [{"type": "a1"}]),
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
            topic="glob-exhaust",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)  # exhaust — do NOT call close()
        assert len(events) == 2

        raw = load(offsets_dir, "test", "glob-exhaust")
        persisted = GlobOffset.decode(raw)
        assert persisted.segment_number == 1
        assert persisted.byte_offset > 0

    def test_single_file_batch_saves_offset_without_explicit_close(self, sample_jsonl, offsets_dir):
        """Exhausting a single-file iterator must save the offset via finally,
        even when the caller never calls close()."""
        from brooklet.storage.offsets import load

        consumer = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="sf-exhaust",
            offsets_dir=offsets_dir,
        )
        list(consumer)  # exhaust — no close()
        offset = load(offsets_dir, group="test", topic="sf-exhaust")
        assert offset > 0

    def test_single_file_save_failure_preserves_in_memory_offset(
        self, sample_jsonl, offsets_dir, monkeypatch, capsys
    ):
        """If _save_offset raises, in-memory self._offset must NOT be clobbered,
        and the failure must surface on stderr (not just the null logger)."""
        from brooklet.core.types import SingleFileOffset

        consumer = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="sf-save-fail",
            offsets_dir=offsets_dir,
        )

        # Pin the initial in-memory offset before iteration.
        original = consumer._offset
        assert isinstance(original, SingleFileOffset)

        # Make _save_offset raise an OSError the way a failing disk would.
        def boom(*_args, **_kwargs) -> None:
            raise OSError("disk full")

        monkeypatch.setattr(consumer, "_save_offset", boom)

        list(consumer)  # iteration swallows the OSError in the finally handler

        # In-memory offset must still be the original — we must not clobber
        # with an unsaved value and diverge from on-disk state.
        assert consumer._offset is original

        # Save failure must be visible on stderr (not buried in null logger).
        captured = capsys.readouterr()
        assert "brooklet" in captured.err
        assert "sf-save-fail" in captured.err

    def test_glob_save_failure_preserves_in_memory_offset(
        self, tmp_path, offsets_dir, monkeypatch, capsys
    ):
        """Glob batch: on save failure, in-memory offset is preserved and
        the failure is visible on stderr."""
        dir_ = tmp_path / "sessions"
        dir_.mkdir()
        (dir_ / "a.jsonl").write_text(json.dumps({"type": "a1"}) + "\n")

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="glob-save-fail",
            offsets_dir=offsets_dir,
        )

        def boom(*_args, **_kwargs) -> None:
            raise OSError("disk full")

        monkeypatch.setattr(consumer, "_save_offset", boom)

        list(consumer)

        captured = capsys.readouterr()
        assert "brooklet" in captured.err
        assert "glob-save-fail" in captured.err


class TestConsumerSegmentSearch:
    """Tests for segment-number-based binary search in glob catch-up."""

    def _make_segment(self, dir_path, segment_num: int, events: list[dict]) -> str:
        """Write a data-NNNN.jsonl segment file and return its path string."""
        filename = f"data-{segment_num:04d}.jsonl"
        path = dir_path / filename
        with open(path, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        return str(path)

    def test_binary_search_finds_correct_segment(self, tmp_path, offsets_dir):
        """With segments 1, 3, 5 (gap at 2, 4), offset at segment_number=3 starts at segment 3."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import save

        dir_ = tmp_path / "topic"
        dir_.mkdir()
        self._make_segment(dir_, 1, [{"type": "s1"}])
        self._make_segment(dir_, 3, [{"type": "s3"}])
        self._make_segment(dir_, 5, [{"type": "s5"}])

        # Offset says we've already consumed segment 1, start from segment 3
        offset = GlobOffset(segment_number=3, byte_offset=0)
        save(offsets_dir, "test", "binary-search", offset.encode())

        consumer = Consumer(
            path=str(dir_ / "data-*.jsonl"),
            mode="glob",
            group="test",
            topic="binary-search",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)

        # Should read segments 3 and 5 (not segment 1 which was already consumed)
        types = [e["type"] for e in events]
        assert "s1" not in types
        assert "s3" in types
        assert "s5" in types

    def test_missing_segment_starts_from_next(self, tmp_path, offsets_dir):
        """Offset at a deleted segment number starts reading from the next available segment."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import save

        dir_ = tmp_path / "topic"
        dir_.mkdir()
        self._make_segment(dir_, 1, [{"type": "s1"}])
        self._make_segment(dir_, 3, [{"type": "s3"}])
        self._make_segment(dir_, 5, [{"type": "s5"}])

        # Offset at segment 2 — but segment 2 was deleted/compacted
        offset = GlobOffset(segment_number=2, byte_offset=0)
        save(offsets_dir, "test", "missing-seg", offset.encode())

        consumer = Consumer(
            path=str(dir_ / "data-*.jsonl"),
            mode="glob",
            group="test",
            topic="missing-seg",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)

        # segment 2 is gone, so start from segment 3 (next >= 2)
        types = [e["type"] for e in events]
        assert "s1" not in types
        assert "s3" in types
        assert "s5" in types

    def test_fallback_to_positional_for_non_segment_files(self, tmp_path, offsets_dir):
        """Files like a.jsonl, b.jsonl that don't match data-NNNN.jsonl use positional indexing."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import save

        dir_ = tmp_path / "external"
        dir_.mkdir()
        for name, event in [("a.jsonl", {"type": "a"}), ("b.jsonl", {"type": "b"})]:
            with open(dir_ / name, "w") as f:
                f.write(json.dumps(event) + "\n")

        # segment_number=1 means positional index 1 (b.jsonl) for non-segment files
        offset = GlobOffset(segment_number=1, byte_offset=0)
        save(offsets_dir, "test", "positional", offset.encode())

        consumer = Consumer(
            path=str(dir_ / "*.jsonl"),
            mode="glob",
            group="test",
            topic="positional",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)

        # Positional fallback: skip index 0 (a.jsonl), read from index 1 (b.jsonl)
        types = [e["type"] for e in events]
        assert "a" not in types
        assert "b" in types

    def test_segment_offset_stable_across_deletion(self, tmp_path, offsets_dir):
        """Delete segment 1 from [1,2,3], consumer at segment_number=2 still finds segment 2."""
        from brooklet.core.types import GlobOffset
        from brooklet.storage.offsets import save

        dir_ = tmp_path / "topic"
        dir_.mkdir()
        seg1 = dir_ / "data-0001.jsonl"
        seg1.write_text(json.dumps({"type": "s1"}) + "\n")
        self._make_segment(dir_, 2, [{"type": "s2"}])
        self._make_segment(dir_, 3, [{"type": "s3"}])

        # Consumer was at segment 2 (had finished segment 1)
        offset = GlobOffset(segment_number=2, byte_offset=0)
        save(offsets_dir, "test", "stable-after-delete", offset.encode())

        # Now delete segment 1 (compaction)
        seg1.unlink()

        consumer = Consumer(
            path=str(dir_ / "data-*.jsonl"),
            mode="glob",
            group="test",
            topic="stable-after-delete",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)

        # Should find segment 2 correctly even though segment 1 is gone
        types = [e["type"] for e in events]
        assert "s1" not in types
        assert "s2" in types
        assert "s3" in types

    def test_consumer_catch_up_across_multiple_segments(self, tmp_path, offsets_dir):
        """Consumer reads events across segments 1, 2, 3 in order."""
        dir_ = tmp_path / "topic"
        dir_.mkdir()
        self._make_segment(dir_, 1, [{"type": "s1a"}, {"type": "s1b"}])
        self._make_segment(dir_, 2, [{"type": "s2a"}])
        self._make_segment(dir_, 3, [{"type": "s3a"}, {"type": "s3b"}])

        consumer = Consumer(
            path=str(dir_ / "data-*.jsonl"),
            mode="glob",
            group="test",
            topic="multi-seg",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)

        types = [e["type"] for e in events]
        assert types == ["s1a", "s1b", "s2a", "s3a", "s3b"]


class TestTopicMonotonicSeq:
    """_seq must be topic-monotonic (the persisted produce-time value), not a
    per-Consumer-instance counter that resets on every new instance.

    Regression coverage for brooklet-a2c: after a gapless resume, the second
    run's first delivered event must carry the topic position assigned at
    produce time, not _seq=1.
    """

    def test_seq_is_topic_monotonic_across_consumer_instances(self, tmp_path):
        """Produce 2, consume (resume), produce 2 more, consume again.

        The second consume must yield _seq 3 and 4 — the persisted
        topic-monotonic positions — not 1 and 2 from a per-run reset.
        """
        from brooklet.core.stream import Stream

        stream = Stream(str(tmp_path))

        stream.produce("demo", {"n": 1})
        stream.produce("demo", {"n": 2})

        first = list(stream.consume("demo", group="g"))
        assert [e["_seq"] for e in first] == [1, 2]

        stream.produce("demo", {"n": 3})
        stream.produce("demo", {"n": 4})

        second = list(stream.consume("demo", group="g"))
        # The bug: a per-run counter would reset and yield [1, 2] here.
        assert [e["n"] for e in second] == [3, 4]
        assert [e["_seq"] for e in second] == [3, 4]

    def test_seq_stable_across_independent_readers(self, tmp_path):
        """The same event gets the same _seq regardless of which reader sees it."""
        from brooklet.core.stream import Stream

        stream = Stream(str(tmp_path))
        for n in range(1, 4):
            stream.produce("t", {"n": n})

        reader_a = [e["_seq"] for e in stream.consume("t", group="a")]
        reader_b = [e["_seq"] for e in stream.consume("t", group="b")]

        assert reader_a == [1, 2, 3]
        assert reader_b == [1, 2, 3]

    def test_mixed_topic_legacy_line_seq_is_monotonic(self, tmp_path, offsets_dir):
        """A legacy (no-_seq) line after a persisted-_seq line gets a greater _seq.

        The fallback counter must track the topic high-water mark: when a line
        carries a valid persisted _seq, the counter advances to at least that
        value, so a subsequent legacy line is numbered above the last seen _seq
        rather than from its position-in-this-read (which could collide with or
        fall below the persisted value).
        """
        path = tmp_path / "mixed-seq.jsonl"
        path.write_text(
            json.dumps({"_seq": 100, "type": "persisted"})
            + "\n"
            + json.dumps({"type": "legacy"})
            + "\n"
        )

        consumer = Consumer(
            path=str(path),
            mode="single-file",
            group="g",
            topic="mixed-seq",
            offsets_dir=offsets_dir,
        )
        events = list(consumer)

        seqs = [e["_seq"] for e in events]
        # Persisted value preserved; legacy line numbered above it (monotonic,
        # no collision). Position-in-read would have given the legacy line 2.
        assert seqs[0] == 100
        assert seqs[1] > 100
        assert seqs[0] < seqs[1]


class TestGlobCatchUpUnit:
    """Directly exercises the extracted `_GlobCatchUp` state machine in isolation.

    These pin the unit's public contract — `events()` iteration plus the
    `offset` reached so far — independent of the `Consumer` that drives it.
    """

    def _segments(self, tmp_path, *batches):
        """Write data-NNNN.jsonl segment files, returning their sorted paths."""
        paths = []
        for n, events in enumerate(batches, start=1):
            path = tmp_path / f"data-{n:04d}.jsonl"
            with open(path, "w") as f:
                for e in events:
                    f.write(json.dumps(e) + "\n")
            paths.append(str(path))
        return sorted(paths)

    def _catch_up(self, tmp_path, **kwargs):
        """Build a `_GlobCatchUp` borrowing a real Consumer's `_read_lines`."""
        from brooklet.core.consumer import Consumer, _GlobCatchUp
        from brooklet.core.types import GlobOffset

        consumer = Consumer(
            path=str(tmp_path / "data-*.jsonl"),
            mode="glob",
            group="g",
            topic="t",
            offsets_dir=str(tmp_path / "offsets"),
            **{k: v for k, v in kwargs.items() if k == "follow"},
        )
        file_positions = kwargs.get("file_positions", {})
        return _GlobCatchUp(
            offset=kwargs.get("offset", GlobOffset(0, 0)),
            follow=kwargs.get("follow", False),
            topic="t",
            group="g",
            read_lines=consumer._read_lines,
            file_positions=file_positions,
        )

    def test_full_read_advances_offset_to_last_segment(self, tmp_path):
        """Reading all segments leaves the offset at the last segment, byte>0."""
        files = self._segments(tmp_path, [{"x": 1}, {"x": 2}], [{"y": 1}])
        catch_up = self._catch_up(tmp_path)

        events = list(catch_up.events(files))

        assert len(events) == 3
        assert catch_up.offset.segment_number == 2
        assert catch_up.offset.byte_offset > 0

    def test_offset_captures_mid_file_position_on_interruption(self, tmp_path):
        """Interrupting mid-file records the in-progress byte offset, not 0."""
        files = self._segments(tmp_path, [{"x": 1}, {"x": 2}, {"x": 3}])
        catch_up = self._catch_up(tmp_path)

        collected = []
        gen = catch_up.events(files)
        for event in gen:
            collected.append(event)
            if len(collected) == 1:
                gen.close()  # GeneratorExit mid-file, like a SIGTERM teardown
                break

        assert catch_up.offset.segment_number == 1
        assert catch_up.offset.byte_offset > 0

    def test_no_files_resets_non_zero_offset(self, tmp_path):
        """Zero matches with a stale non-zero offset resets to (0, 0)."""
        from brooklet.core.types import GlobOffset

        catch_up = self._catch_up(tmp_path, offset=GlobOffset(3, 42))

        assert list(catch_up.events([])) == []
        assert catch_up.offset == GlobOffset(0, 0)

    def test_follow_seeds_file_positions_for_skipped_and_read(self, tmp_path):
        """Follow mode records end positions for both skipped and read files."""
        from brooklet.core.types import GlobOffset

        files = self._segments(tmp_path, [{"x": 1}], [{"y": 1}])
        positions: dict[str, int] = {}
        catch_up = self._catch_up(
            tmp_path,
            follow=True,
            offset=GlobOffset(2, 0),  # skip segment 1, read segment 2
            file_positions=positions,
        )

        list(catch_up.events(files))

        assert positions[files[0]] > 0  # skipped file's size recorded
        assert positions[files[1]] > 0  # read file's end position recorded
