# ABOUTME: End-to-end integration test for the full brooklet pipeline
# ABOUTME: Exercises open -> register -> consume -> append -> re-consume -> offset persistence

import json

import brooklet


class TestEndToEnd:
    def test_full_lifecycle(self, tmp_path):
        """Full lifecycle: open, register, consume, append, re-consume."""
        stream_dir = tmp_path / "streams"
        jsonl_path = tmp_path / "events.jsonl"

        # Write initial events
        with open(jsonl_path, "w") as f:
            f.write(json.dumps({"type": "login", "user": "alice"}) + "\n")
            f.write(json.dumps({"type": "click", "user": "alice"}) + "\n")

        # Open stream and register source
        s = brooklet.open(str(stream_dir))
        s.register("app-events", path=str(jsonl_path), mode="single-file")

        # First consume: get all events
        events1 = list(s.consume("app-events", group="analytics"))
        assert len(events1) == 2
        assert events1[0]["type"] == "login"
        assert events1[1]["type"] == "click"

        # Verify envelope fields
        for event in events1:
            assert "_ts" in event
            assert "_seq" in event
            assert event["_src"] == "app-events"

        # Append new data to the source file
        with open(jsonl_path, "a") as f:
            f.write(json.dumps({"type": "logout", "user": "alice"}) + "\n")

        # Re-consume: only get new events
        events2 = list(s.consume("app-events", group="analytics"))
        assert len(events2) == 1
        assert events2[0]["type"] == "logout"

        # Verify offsets survive across Stream instances
        s2 = brooklet.open(str(stream_dir))
        events3 = list(s2.consume("app-events", group="analytics"))
        assert events3 == []

        # Different group gets all events
        events4 = list(s2.consume("app-events", group="fresh-group"))
        assert len(events4) == 3

    def test_multiple_topics(self, tmp_path):
        """Register multiple topics and consume independently."""
        stream_dir = tmp_path / "multi"

        # Create two separate JSONL files
        auth_path = tmp_path / "auth.jsonl"
        with open(auth_path, "w") as f:
            f.write(json.dumps({"event": "login"}) + "\n")

        metrics_path = tmp_path / "metrics.jsonl"
        with open(metrics_path, "w") as f:
            f.write(json.dumps({"cpu": 42}) + "\n")
            f.write(json.dumps({"cpu": 55}) + "\n")

        s = brooklet.open(str(stream_dir))
        s.register("auth", path=str(auth_path), mode="single-file")
        s.register("metrics", path=str(metrics_path), mode="single-file")

        assert sorted(s.topics()) == ["auth", "metrics"]

        auth_events = list(s.consume("auth", group="test"))
        assert len(auth_events) == 1

        metrics_events = list(s.consume("metrics", group="test"))
        assert len(metrics_events) == 2

    def test_context_manager_lifecycle(self, tmp_path):
        """Consumer context manager saves offset on exit."""
        stream_dir = tmp_path / "ctx"
        jsonl_path = tmp_path / "events.jsonl"

        with open(jsonl_path, "w") as f:
            f.write(json.dumps({"type": "hello"}) + "\n")
            f.write(json.dumps({"type": "world"}) + "\n")

        s = brooklet.open(str(stream_dir))
        s.register("t", path=str(jsonl_path), mode="single-file")

        with s.consume("t", group="g") as consumer:
            events = list(consumer)
            assert len(events) == 2

        # Offset should be saved — second consume gets nothing
        events2 = list(s.consume("t", group="g"))
        assert events2 == []
