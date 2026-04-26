# ABOUTME: Tests for segment rotation in produce()
# ABOUTME: Covers rotation at size threshold, sidecar, locking, and legacy migration

import glob
import json
import subprocess
import sys
from pathlib import Path

import pytest

import brooklet
from brooklet.core.types import BrookletWriteLockError


class TestSegmentCreation:
    """Tests for segment file creation on first produce."""

    def test_produce_creates_first_segment(self, tmp_path):
        """First produce creates data-0001.jsonl, not data.jsonl."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("events", {"type": "hello"})

        topic_dir = tmp_path / "streams" / "events"
        assert (topic_dir / "data-0001.jsonl").is_file()
        assert not (topic_dir / "data.jsonl").exists()

    def test_produce_envelope_in_segments(self, tmp_path):
        """Events written to segments have correct _ts, _seq, _src fields."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("events", {"type": "hello"}, source="test-src")

        segment = tmp_path / "streams" / "events" / "data-0001.jsonl"
        event = json.loads(segment.read_text().strip())
        assert "_ts" in event
        assert event["_seq"] == 1
        assert event["_src"] == "test-src"
        assert event["type"] == "hello"

    def test_produce_sequence_monotonic_across_segments(self, tmp_path):
        """_seq increments monotonically even across segment boundaries."""
        s = brooklet.open(str(tmp_path / "streams"))
        # Write with tiny max_segment_bytes to force rotation
        for i in range(5):
            s.produce("counter", {"type": f"event-{i}"}, max_segment_bytes=50)

        # Collect all events from all segments in order
        topic_dir = tmp_path / "streams" / "counter"
        all_seqs = []
        for seg_path in sorted(glob.glob(str(topic_dir / "data-*.jsonl"))):
            for line in Path(seg_path).read_text().splitlines():
                if line.strip():
                    all_seqs.append(json.loads(line)["_seq"])

        assert all_seqs == list(range(1, len(all_seqs) + 1))


class TestSegmentRotation:
    """Tests for segment rotation logic."""

    def test_produce_rotates_at_size_threshold(self, tmp_path):
        """With max_segment_bytes=100, writing enough data creates a second segment."""
        s = brooklet.open(str(tmp_path / "streams"))
        # Write events large enough to exceed 100 bytes per segment
        for _ in range(5):
            s.produce("rotate", {"type": "x" * 50}, max_segment_bytes=100)

        topic_dir = tmp_path / "streams" / "rotate"
        segments = sorted(glob.glob(str(topic_dir / "data-*.jsonl")))
        assert len(segments) >= 2

    def test_produce_segment_numbering_increments(self, tmp_path):
        """Segments are numbered 0001, 0002, 0003, etc."""
        s = brooklet.open(str(tmp_path / "streams"))
        # Force multiple rotations with a tiny threshold
        for _ in range(10):
            s.produce("numbered", {"type": "x" * 20}, max_segment_bytes=30)

        topic_dir = tmp_path / "streams" / "numbered"
        segments = sorted(glob.glob(str(topic_dir / "data-*.jsonl")))
        assert len(segments) >= 3
        # Verify numbering pattern
        for seg in segments:
            name = Path(seg).name
            assert name.startswith("data-") and name.endswith(".jsonl")
            num = int(name[5:9])
            assert num >= 1


class TestSidecar:
    """Tests for sidecar cache interaction."""

    def test_produce_sidecar_updated(self, tmp_path):
        """After producing, sidecar has correct next_seq."""
        from brooklet.storage.sidecar import read_next_seq

        s = brooklet.open(str(tmp_path / "streams"))
        brooklet_dir = tmp_path / "streams" / ".brooklet"

        s.produce("sidecar-topic", {"type": "first"})
        s.produce("sidecar-topic", {"type": "second"})

        cached = read_next_seq(brooklet_dir, "sidecar-topic")
        assert cached == 3  # next after seq=2

    def test_produce_sidecar_crash_recovery(self, tmp_path):
        """Corrupt sidecar still results in correct next_seq via re-derivation."""
        s = brooklet.open(str(tmp_path / "streams"))
        brooklet_dir = tmp_path / "streams" / ".brooklet"

        # Produce two events normally
        s.produce("crash-topic", {"type": "a"})
        s.produce("crash-topic", {"type": "b"})

        # Corrupt the sidecar
        seq_dir = brooklet_dir / "seq"
        sidecar = seq_dir / "crash-topic.json"
        sidecar.write_text("NOT VALID JSON {{{{")

        # Produce a third event — should re-derive and produce _seq=3
        s.produce("crash-topic", {"type": "c"})

        topic_dir = tmp_path / "streams" / "crash-topic"
        all_seqs = []
        for seg_path in sorted(glob.glob(str(topic_dir / "data-*.jsonl"))):
            for line in Path(seg_path).read_text().splitlines():
                if line.strip():
                    all_seqs.append(json.loads(line)["_seq"])

        assert all_seqs == [1, 2, 3]


class TestLegacyMigration:
    """Tests for migration of legacy data.jsonl files."""

    def test_produce_legacy_migration(self, tmp_path):
        """If data.jsonl exists (no segments), it is renamed to data-0000.jsonl."""
        stream_dir = tmp_path / "streams"
        stream_dir.mkdir()
        topic_dir = stream_dir / "legacy-topic"
        topic_dir.mkdir()

        # Write a legacy data.jsonl file
        old_event = {"type": "old", "_seq": 1, "_ts": "2026-01-01T00:00:00Z"}
        (topic_dir / "data.jsonl").write_text(json.dumps(old_event) + "\n")

        s = brooklet.open(str(stream_dir))
        s.produce("legacy-topic", {"type": "new"})

        # Legacy file should be renamed
        assert (topic_dir / "data-0000.jsonl").is_file()
        assert not (topic_dir / "data.jsonl").exists()
        # New event goes into next segment
        assert (topic_dir / "data-0001.jsonl").is_file()

    def test_produce_legacy_migration_updates_registry(self, tmp_path):
        """After migration, registry entry has mode=glob and glob pattern path."""
        stream_dir = tmp_path / "streams"
        stream_dir.mkdir()
        topic_dir = stream_dir / "legacy-reg"
        topic_dir.mkdir()

        old_event = {"type": "old", "_seq": 1, "_ts": "2026-01-01T00:00:00Z"}
        (topic_dir / "data.jsonl").write_text(json.dumps(old_event) + "\n")

        s = brooklet.open(str(stream_dir))
        s.produce("legacy-reg", {"type": "new"})

        # Get the registry source — should be glob mode
        source = s._registry.get("legacy-reg")
        assert source["mode"] == "glob"
        assert "data-*.jsonl" in source["path"]


class TestLockContention:
    """Tests for lock contention behavior."""

    def test_produce_lock_contention_raises(self, tmp_path):
        """Concurrent produce on same topic from subprocess raises BrookletWriteLockError."""
        stream_dir = tmp_path / "streams"
        stream_dir.mkdir()

        # Pre-create topic dir
        topic_dir = stream_dir / "locked-topic"
        topic_dir.mkdir()
        brooklet_dir = stream_dir / ".brooklet"
        brooklet_dir.mkdir()

        # Script to hold the lock and signal readiness
        lock_holder = f"""
import sys
sys.path.insert(0, '{Path(__file__).parents[1] / "src"}')
from pathlib import Path
from brooklet.storage.locking import acquire_topic_lock, release_topic_lock
import time
brooklet_dir = Path('{brooklet_dir}')
fd = acquire_topic_lock(brooklet_dir, 'locked-topic')
# Hold the lock and output signal
print('LOCKED', flush=True)
time.sleep(5)
release_topic_lock(fd)
"""
        proc = subprocess.Popen(
            [sys.executable, "-c", lock_holder],
            stdout=subprocess.PIPE,
            text=True,
        )
        # Wait for lock to be held
        line = proc.stdout.readline()
        assert line.strip() == "LOCKED"

        try:
            s = brooklet.open(str(stream_dir))
            with pytest.raises(BrookletWriteLockError):
                s.produce("locked-topic", {"type": "test"})
        finally:
            proc.terminate()
            proc.wait()


class TestAutoRegistration:
    """Tests for auto-registration with glob pattern."""

    def test_produce_auto_registers_glob_pattern(self, tmp_path):
        """After first produce, registry entry has mode='glob' and glob pattern path."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("auto-glob", {"type": "test"})

        source = s._registry.get("auto-glob")
        assert source["mode"] == "glob"
        assert "data-*.jsonl" in source["path"]

    def test_produce_idempotent_glob_registration(self, tmp_path):
        """Multiple produces to the same topic don't error or duplicate registry entry."""
        s = brooklet.open(str(tmp_path / "streams"))
        s.produce("repeat-glob", {"type": "first"})
        s.produce("repeat-glob", {"type": "second"})
        assert s.topics().count("repeat-glob") == 1


class TestConsumeRoundtrip:
    """Tests for produce/consume integration with segments."""

    def test_produce_consume_roundtrip_with_segments(self, tmp_path):
        """Produce 3 events, consume them back correctly."""
        s = brooklet.open(str(tmp_path / "streams"))
        for i in range(3):
            s.produce("roundtrip", {"type": f"event-{i}"})

        events = list(s.consume("roundtrip", group="reader"))
        assert len(events) == 3
        for i, event in enumerate(events):
            assert event["_seq"] == i + 1
            assert "_ts" in event
            assert event["type"] == f"event-{i}"

    def test_produce_consume_roundtrip_across_segments(self, tmp_path):
        """Events produced across multiple segments are all consumed correctly."""
        s = brooklet.open(str(tmp_path / "streams"))
        for i in range(8):
            s.produce("multi-seg", {"type": f"e-{i}", "pad": "x" * 20}, max_segment_bytes=60)

        events = list(s.consume("multi-seg", group="reader"))
        assert len(events) == 8
        seqs = [e["_seq"] for e in events]
        assert seqs == list(range(1, 9))
