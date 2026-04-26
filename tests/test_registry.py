# ABOUTME: Tests for source registration — mapping external JSONL paths to topic names
# ABOUTME: Verifies register, retrieve, persist, glob mode, and topic listing

import pytest

from brooklet.storage.registry import Registry


class TestRegistry:
    def test_register_single_file(self, brooklet_dir):
        """Register a single-file source and retrieve it."""
        reg = Registry(brooklet_dir)
        reg.register("my-topic", path="/tmp/events.jsonl", mode="single-file")

        source = reg.get("my-topic")
        assert source["path"] == "/tmp/events.jsonl"
        assert source["mode"] == "single-file"

    def test_register_glob(self, brooklet_dir):
        """Register a glob source and retrieve it."""
        reg = Registry(brooklet_dir)
        reg.register("sessions", path="/tmp/sessions/*.jsonl", mode="glob")

        source = reg.get("sessions")
        assert source["path"] == "/tmp/sessions/*.jsonl"
        assert source["mode"] == "glob"

    def test_register_persists(self, brooklet_dir):
        """Registration survives a new Registry instance."""
        reg1 = Registry(brooklet_dir)
        reg1.register("persistent", path="/tmp/p.jsonl", mode="single-file")

        reg2 = Registry(brooklet_dir)
        source = reg2.get("persistent")
        assert source["path"] == "/tmp/p.jsonl"

    def test_invalid_mode_raises(self, brooklet_dir):
        """Rejects invalid mode values."""
        reg = Registry(brooklet_dir)
        with pytest.raises(ValueError, match="mode"):
            reg.register("bad", path="/tmp/x.jsonl", mode="invalid")

    def test_list_topics(self, brooklet_dir):
        """Returns names of all registered topics."""
        reg = Registry(brooklet_dir)
        reg.register("alpha", path="/tmp/a.jsonl", mode="single-file")
        reg.register("beta", path="/tmp/b/*.jsonl", mode="glob")

        topics = reg.list_topics()
        assert sorted(topics) == ["alpha", "beta"]

    def test_list_topics_empty(self, brooklet_dir):
        """Returns empty list when nothing is registered."""
        reg = Registry(brooklet_dir)
        assert reg.list_topics() == []

    def test_get_missing_topic_raises(self, brooklet_dir):
        """Getting an unregistered topic raises KeyError."""
        reg = Registry(brooklet_dir)
        with pytest.raises(KeyError):
            reg.get("nonexistent")

    def test_register_overwrites(self, brooklet_dir):
        """Re-registering the same name overwrites the previous source."""
        reg = Registry(brooklet_dir)
        reg.register("t", path="/tmp/old.jsonl", mode="single-file")
        reg.register("t", path="/tmp/new.jsonl", mode="single-file")

        source = reg.get("t")
        assert source["path"] == "/tmp/new.jsonl"

    def test_register_name_with_path_traversal_raises(self, brooklet_dir):
        """Topic names with path traversal are rejected."""
        reg = Registry(brooklet_dir)
        with pytest.raises(ValueError, match="path traversal"):
            reg.register("../etc", path="/tmp/x.jsonl", mode="single-file")

    def test_corrupt_sources_file_raises_with_context(self, brooklet_dir):
        """Corrupted sources.json gives actionable error message."""
        sources_path = brooklet_dir / "sources.json"
        sources_path.write_text("NOT VALID JSON{{{")

        with pytest.raises(ValueError, match="Corrupt"):
            Registry(brooklet_dir)


class TestRegistryLocalGlob:
    def test_register_local_stores_glob_pattern(self, brooklet_dir):
        """register_local with mode='glob' stores the entry with mode 'glob'."""
        reg = Registry(brooklet_dir)
        reg.register_local("events", "/tmp/streams/events/data-*.jsonl", mode="glob")

        source = reg.get("events")
        assert source["path"] == "/tmp/streams/events/data-*.jsonl"
        assert source["mode"] == "glob"

    def test_register_local_default_mode_is_single_file(self, brooklet_dir):
        """register_local without mode defaults to 'single-file' for backward compat."""
        reg = Registry(brooklet_dir)
        reg.register_local("events", "/tmp/streams/events/data.jsonl")

        source = reg.get("events")
        assert source["mode"] == "single-file"

    def test_register_local_idempotent_with_glob(self, brooklet_dir):
        """Calling register_local twice with same name+path+mode='glob' does not error."""
        reg = Registry(brooklet_dir)
        reg.register_local("logs", "/tmp/logs/data-*.jsonl", mode="glob")
        reg.register_local("logs", "/tmp/logs/data-*.jsonl", mode="glob")  # no error

        source = reg.get("logs")
        assert source["mode"] == "glob"

    def test_register_local_allows_mode_update(self, brooklet_dir):
        """Local topics can be updated from single-file to glob mode for segment rotation."""
        reg = Registry(brooklet_dir)
        reg.register_local("events", "/tmp/streams/events/data.jsonl", mode="single-file")

        # Updating path and mode for the same local topic is allowed (used during rotation)
        reg.register_local("events", "/tmp/streams/events/data-*.jsonl", mode="glob")

        source = reg.get("events")
        assert source["mode"] == "glob"
        assert source["path"] == "/tmp/streams/events/data-*.jsonl"

    def test_get_returns_correct_mode_for_local_glob(self, brooklet_dir):
        """After registering with glob mode, get() returns the correct mode."""
        reg = Registry(brooklet_dir)
        reg.register_local("metrics", "/tmp/metrics/data-*.jsonl", mode="glob")

        source = reg.get("metrics")
        assert source["path"] == "/tmp/metrics/data-*.jsonl"
        assert source["mode"] == "glob"
