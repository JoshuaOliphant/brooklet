# ABOUTME: Tests for the batch consumer — reads JSONL events with offset tracking
# ABOUTME: Covers empty files, full reads, offset resume, glob mode, and group isolation

import json

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
