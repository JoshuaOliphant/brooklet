# ABOUTME: Tests for the offset persistence module
# ABOUTME: Verifies save/load roundtrip, defaults, directory creation, and atomic writes

import pytest

from brooklet.offsets import load, save


class TestOffsets:
    def test_load_default_zero(self, offsets_dir):
        """Missing offset file returns 0."""
        result = load(offsets_dir, group="mygroup", topic="mytopic")
        assert result == 0

    def test_save_and_load_roundtrip(self, offsets_dir):
        """Save 42, load 42."""
        save(offsets_dir, group="mygroup", topic="mytopic", offset=42)
        result = load(offsets_dir, group="mygroup", topic="mytopic")
        assert result == 42

    def test_save_creates_directories(self, tmp_path):
        """Parent dirs are created on first save."""
        deep_dir = tmp_path / "a" / "b" / "c" / "offsets"
        save(deep_dir, group="g", topic="t", offset=10)
        result = load(deep_dir, group="g", topic="t")
        assert result == 10

    def test_save_overwrites_previous(self, offsets_dir):
        """Saving a new offset overwrites the old one."""
        save(offsets_dir, group="g", topic="t", offset=10)
        save(offsets_dir, group="g", topic="t", offset=20)
        result = load(offsets_dir, group="g", topic="t")
        assert result == 20

    def test_group_isolation(self, offsets_dir):
        """Different groups have independent offsets for the same topic."""
        save(offsets_dir, group="alpha", topic="events", offset=100)
        save(offsets_dir, group="beta", topic="events", offset=200)

        assert load(offsets_dir, group="alpha", topic="events") == 100
        assert load(offsets_dir, group="beta", topic="events") == 200

    def test_topic_isolation(self, offsets_dir):
        """Same group tracks different offsets per topic."""
        save(offsets_dir, group="g", topic="topic-a", offset=10)
        save(offsets_dir, group="g", topic="topic-b", offset=20)

        assert load(offsets_dir, group="g", topic="topic-a") == 10
        assert load(offsets_dir, group="g", topic="topic-b") == 20

    def test_atomic_write(self, offsets_dir):
        """File content is valid JSON after write (not corrupted)."""
        import json

        save(offsets_dir, group="g", topic="t", offset=99)
        # Read the file directly to verify it's valid JSON
        offset_file = offsets_dir / "g-t.json"
        data = json.loads(offset_file.read_text())
        assert data["offset"] == 99

    def test_atomic_write_cleanup_on_replace_failure(self, offsets_dir, monkeypatch):
        """Error handler does not raise secondary OSError on closed fd."""
        import os as os_mod

        def failing_replace(src, dst):
            raise OSError("simulated replace failure")

        monkeypatch.setattr(os_mod, "replace", failing_replace)

        with pytest.raises(OSError, match="simulated replace failure"):
            save(offsets_dir, group="g", topic="t", offset=42)

    def test_corrupt_offset_file_raises_with_context(self, offsets_dir):
        """Corrupted offset file gives actionable error message."""
        offset_file = offsets_dir / "g-t.json"
        offset_file.write_text("NOT VALID JSON{{{")

        with pytest.raises(ValueError, match="Corrupt offset file"):
            load(offsets_dir, group="g", topic="t")

    def test_name_validation_rejects_path_separators(self, offsets_dir):
        """Group/topic names with path separators are rejected."""
        with pytest.raises(ValueError, match="safe characters"):
            save(offsets_dir, group="../etc", topic="t", offset=1)

        with pytest.raises(ValueError, match="safe characters"):
            save(offsets_dir, group="g", topic="../../passwd", offset=1)

        with pytest.raises(ValueError, match="safe characters"):
            load(offsets_dir, group="../etc", topic="t")
