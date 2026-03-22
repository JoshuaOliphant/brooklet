# ABOUTME: Tests for consumer follow mode — tailing JSONL files for new events
# ABOUTME: Covers catch-up, append detection, offset persistence, and glob+follow

import json
import logging
import threading
import time
from unittest.mock import MagicMock

from brooklet.consumer import Consumer


class TestConsumerFollow:
    def test_follow_catches_up_then_tails(self, sample_jsonl, offsets_dir):
        """Reads existing events first before tailing for new ones."""
        events_seen = []

        def consume_in_thread():
            consumer = Consumer(
                path=str(sample_jsonl),
                mode="single-file",
                group="test",
                topic="follow",
                offsets_dir=offsets_dir,
                follow=True,
            )
            for event in consumer:
                events_seen.append(event)
                if len(events_seen) >= 4:
                    consumer.close()
                    break

        thread = threading.Thread(target=consume_in_thread)
        thread.start()

        # Give consumer time to read existing 3 events and start tailing
        time.sleep(0.5)

        # Append a new event
        with open(sample_jsonl, "a") as f:
            f.write(json.dumps({"type": "new", "message": "appended"}) + "\n")

        thread.join(timeout=5)
        assert not thread.is_alive(), "Consumer thread didn't finish"

        assert len(events_seen) == 4
        assert events_seen[0]["type"] == "start"
        assert events_seen[3]["type"] == "new"

    def test_follow_detects_appended_lines(self, empty_jsonl, offsets_dir):
        """New lines written after consumer starts are yielded."""
        events_seen = []

        def consume_in_thread():
            consumer = Consumer(
                path=str(empty_jsonl),
                mode="single-file",
                group="test",
                topic="follow-append",
                offsets_dir=offsets_dir,
                follow=True,
            )
            for event in consumer:
                events_seen.append(event)
                if len(events_seen) >= 2:
                    consumer.close()
                    break

        thread = threading.Thread(target=consume_in_thread)
        thread.start()

        # Wait for consumer to start tailing
        time.sleep(0.5)

        # Append events
        with open(empty_jsonl, "a") as f:
            f.write(json.dumps({"type": "first"}) + "\n")
            f.flush()
            time.sleep(0.3)
            f.write(json.dumps({"type": "second"}) + "\n")
            f.flush()

        thread.join(timeout=5)
        assert not thread.is_alive(), "Consumer thread didn't finish"

        assert len(events_seen) == 2
        assert events_seen[0]["type"] == "first"
        assert events_seen[1]["type"] == "second"

    def test_follow_saves_offset_on_close(self, sample_jsonl, offsets_dir):
        """Offset is persisted when follow consumer is closed."""
        from brooklet.offsets import load

        events_seen = []

        def consume_in_thread():
            consumer = Consumer(
                path=str(sample_jsonl),
                mode="single-file",
                group="test",
                topic="follow-close",
                offsets_dir=offsets_dir,
                follow=True,
            )
            for event in consumer:
                events_seen.append(event)
                if len(events_seen) >= 3:
                    consumer.close()
                    break

        thread = threading.Thread(target=consume_in_thread)
        thread.start()
        thread.join(timeout=5)

        offset = load(offsets_dir, group="test", topic="follow-close")
        assert offset > 0

    def test_glob_follow_catches_up_then_tails(self, tmp_path, offsets_dir):
        """Glob+follow reads existing files then tails for appends."""
        d = tmp_path / "sessions"
        d.mkdir()
        (d / "a.jsonl").write_text(json.dumps({"type": "a1"}) + "\n")
        (d / "b.jsonl").write_text(json.dumps({"type": "b1"}) + "\n")

        events_seen = []

        def consume_in_thread():
            consumer = Consumer(
                path=str(d / "*.jsonl"),
                mode="glob",
                group="test",
                topic="glob-follow",
                offsets_dir=offsets_dir,
                follow=True,
            )
            for event in consumer:
                events_seen.append(event)
                if len(events_seen) >= 3:
                    consumer.close()
                    break

        thread = threading.Thread(target=consume_in_thread)
        thread.start()
        time.sleep(1.0)

        # Append to existing file
        with open(d / "b.jsonl", "a") as f:
            f.write(json.dumps({"type": "b2"}) + "\n")
            f.flush()

        thread.join(timeout=5)
        assert not thread.is_alive(), "Consumer thread didn't finish"

        assert len(events_seen) == 3
        types = [e["type"] for e in events_seen]
        assert types[:2] == ["a1", "b1"]
        assert "b2" in types

    def test_glob_follow_detects_new_files(self, tmp_path, offsets_dir):
        """Glob+follow detects new files created in the directory."""
        d = tmp_path / "sessions"
        d.mkdir()
        (d / "a.jsonl").write_text(json.dumps({"type": "a1"}) + "\n")

        events_seen = []

        def consume_in_thread():
            consumer = Consumer(
                path=str(d / "*.jsonl"),
                mode="glob",
                group="test",
                topic="glob-follow-new",
                offsets_dir=offsets_dir,
                follow=True,
            )
            for event in consumer:
                events_seen.append(event)
                if len(events_seen) >= 2:
                    consumer.close()
                    break

        thread = threading.Thread(target=consume_in_thread)
        thread.start()
        time.sleep(1.0)

        # Create a new file
        (d / "c.jsonl").write_text(json.dumps({"type": "c1"}) + "\n")

        thread.join(timeout=5)
        assert not thread.is_alive(), "Consumer thread didn't finish"

        assert len(events_seen) == 2
        types = [e["type"] for e in events_seen]
        assert "a1" in types
        assert "c1" in types

    def test_glob_follow_offset_persistence(self, tmp_path, offsets_dir):
        """Glob+follow resumes from saved offset after restart."""
        from brooklet.offsets import load

        d = tmp_path / "sessions"
        d.mkdir()
        (d / "a.jsonl").write_text(json.dumps({"type": "a1"}) + "\n")
        (d / "b.jsonl").write_text(json.dumps({"type": "b1"}) + "\n")

        # First pass: batch read to set the offset
        c1 = Consumer(
            path=str(d / "*.jsonl"),
            mode="glob",
            group="test",
            topic="glob-follow-persist",
            offsets_dir=offsets_dir,
        )
        list(c1)
        c1.close()

        offset_after_batch = load(offsets_dir, "test", "glob-follow-persist")
        assert offset_after_batch > 0

        # Second pass: follow mode, should only get new events
        events_seen = []

        def consume_in_thread():
            consumer = Consumer(
                path=str(d / "*.jsonl"),
                mode="glob",
                group="test",
                topic="glob-follow-persist",
                offsets_dir=offsets_dir,
                follow=True,
            )
            for event in consumer:
                events_seen.append(event)
                if len(events_seen) >= 1:
                    consumer.close()
                    break

        thread = threading.Thread(target=consume_in_thread)
        thread.start()
        time.sleep(1.0)

        # Append new data
        with open(d / "b.jsonl", "a") as f:
            f.write(json.dumps({"type": "new_after_restart"}) + "\n")
            f.flush()

        thread.join(timeout=5)
        assert not thread.is_alive(), "Consumer thread didn't finish"

        assert len(events_seen) == 1
        assert events_seen[0]["type"] == "new_after_restart"

    def test_glob_catch_up_survives_missing_file(self, tmp_path, offsets_dir):
        """Catch-up skips files that vanish between glob and open."""
        d = tmp_path / "sessions"
        d.mkdir()
        (d / "a.jsonl").write_text(json.dumps({"type": "a1"}) + "\n")
        (d / "b.jsonl").write_text(json.dumps({"type": "b1"}) + "\n")
        (d / "c.jsonl").write_text(json.dumps({"type": "c1"}) + "\n")

        # Simulate a file list that includes a file that will be deleted
        import glob as glob_module
        from unittest.mock import patch

        real_files = sorted(glob_module.glob(str(d / "*.jsonl")))
        assert len(real_files) == 3

        # Delete b.jsonl after glob but before open
        (d / "b.jsonl").unlink()

        consumer = Consumer(
            path=str(d / "*.jsonl"),
            mode="glob",
            group="test",
            topic="glob-missing",
            offsets_dir=offsets_dir,
        )
        # Patch glob to return the original list (including deleted file)
        with patch.object(glob_module, "glob", return_value=real_files):
            events = list(consumer)
        consumer.close()

        # Should get a1 and c1, skipping the missing b.jsonl
        types = [e["type"] for e in events]
        assert "a1" in types
        assert "c1" in types
        assert "b1" not in types


class TestObserverJoinTimeout:
    """Tests for observer.join() timeout to prevent hang on shutdown."""

    def test_iterate_follow_joins_with_timeout(self, sample_jsonl, offsets_dir):
        """_iterate_follow calls observer.join() with a timeout."""
        events_seen = []

        def consume_in_thread():
            consumer = Consumer(
                path=str(sample_jsonl),
                mode="single-file",
                group="test",
                topic="join-timeout",
                offsets_dir=offsets_dir,
                follow=True,
            )
            for event in consumer:
                events_seen.append(event)
                if len(events_seen) >= 3:
                    consumer.close()
                    break

        thread = threading.Thread(target=consume_in_thread)
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive()

    def test_close_joins_with_timeout(self, sample_jsonl, offsets_dir):
        """close() calls observer.join() with a timeout."""
        consumer = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="close-timeout",
            offsets_dir=offsets_dir,
            follow=True,
        )
        # Set up a mock observer to verify join is called with timeout
        mock_observer = MagicMock()
        mock_observer.is_alive.return_value = False
        consumer._observer = mock_observer

        consumer.close()

        mock_observer.stop.assert_called_once()
        mock_observer.join.assert_called_once()
        # Verify join was called with a timeout argument
        args, kwargs = mock_observer.join.call_args
        timeout_val = kwargs.get("timeout") or (args[0] if args else None)
        assert timeout_val is not None, "observer.join() must be called with a timeout"
        assert timeout_val > 0

    def test_close_logs_warning_when_observer_hangs(
        self, sample_jsonl, offsets_dir, caplog
    ):
        """A warning is logged if observer thread is still alive after join timeout."""
        consumer = Consumer(
            path=str(sample_jsonl),
            mode="single-file",
            group="test",
            topic="hang-warning",
            offsets_dir=offsets_dir,
            follow=True,
        )
        mock_observer = MagicMock()
        mock_observer.is_alive.return_value = True  # Simulate hung thread
        consumer._observer = mock_observer

        with caplog.at_level(logging.WARNING, logger="brooklet"):
            consumer.close()

        assert any("did not stop" in r.message for r in caplog.records)

    def test_glob_follow_joins_with_timeout(self, tmp_path, offsets_dir):
        """_iterate_glob_follow calls observer.join() with a timeout."""
        d = tmp_path / "sessions"
        d.mkdir()
        (d / "a.jsonl").write_text(json.dumps({"type": "a1"}) + "\n")

        events_seen = []

        def consume_in_thread():
            consumer = Consumer(
                path=str(d / "*.jsonl"),
                mode="glob",
                group="test",
                topic="glob-join-timeout",
                offsets_dir=offsets_dir,
                follow=True,
            )
            for event in consumer:
                events_seen.append(event)
                if len(events_seen) >= 1:
                    consumer.close()
                    break

        thread = threading.Thread(target=consume_in_thread)
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive()
