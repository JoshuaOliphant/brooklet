# ABOUTME: Tests for consumer follow mode — tailing JSONL files for new events
# ABOUTME: Covers catch-up, append detection, offset persistence, and glob+follow rejection

import json
import threading
import time

import pytest

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

    def test_glob_follow_raises(self, offsets_dir):
        """Glob + follow raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="follow"):
            Consumer(
                path="/tmp/*.jsonl",
                mode="glob",
                group="test",
                topic="nope",
                offsets_dir=offsets_dir,
                follow=True,
            )
