# ABOUTME: Tests for the offset persistence module
# ABOUTME: Verifies save/load roundtrip, defaults, directory creation, and atomic writes

import pytest

from brooklet.storage.offsets import _offset_path, load, save


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

    def test_name_validation_rejects_path_traversal(self, offsets_dir):
        """Group/topic names with path traversal are rejected."""
        with pytest.raises(ValueError, match="path traversal"):
            save(offsets_dir, group="../etc", topic="t", offset=1)

        with pytest.raises(ValueError, match="path traversal"):
            save(offsets_dir, group="g", topic="../../passwd", offset=1)

        with pytest.raises(ValueError, match="path traversal"):
            load(offsets_dir, group="../etc", topic="t")


class TestOffsetPathInjectivity:
    """Distinct valid (group, topic) identities must map to distinct offset files.

    The old scheme built ``f"{group}-{topic.replace('/', '--')}.json"``, which is
    not injective: the group/topic boundary is ambiguous ('a'+'b-c' collides with
    'a-b'+'c'), and the '/'->'--' rewrite collides topic 'a/b' with literal 'a--b'.
    Both classes are reachable because names may contain '-' and '/'.
    """

    def test_hyphen_boundary_pairs_are_independent(self, offsets_dir):
        """(group='a', topic='b-c') and (group='a-b', topic='c') stay independent."""
        save(offsets_dir, group="a", topic="b-c", offset=11)
        save(offsets_dir, group="a-b", topic="c", offset=22)

        assert load(offsets_dir, group="a", topic="b-c") == 11
        assert load(offsets_dir, group="a-b", topic="c") == 22

    def test_slash_topic_vs_literal_dashes_are_independent(self, offsets_dir):
        """Topic 'a/b' and literal topic 'a--b' stay independent under one group."""
        save(offsets_dir, group="g", topic="a/b", offset=33)
        save(offsets_dir, group="g", topic="a--b", offset=44)

        assert load(offsets_dir, group="g", topic="a/b") == 33
        assert load(offsets_dir, group="g", topic="a--b") == 44

    def test_colliding_identities_map_to_distinct_paths(self, offsets_dir):
        """The documented collision pairs resolve to distinct filenames."""
        assert _offset_path(offsets_dir, "a", "b-c") != _offset_path(
            offsets_dir, "a-b", "c"
        )
        assert _offset_path(offsets_dir, "g", "a/b") != _offset_path(
            offsets_dir, "g", "a--b"
        )

    def test_offset_path_stays_within_offsets_dir(self, offsets_dir):
        """Even path-style topics resolve to a flat file directly in offsets_dir."""
        cases = [
            ("g", "a/b/c"),
            ("scout/x", "stats/y"),
            ("g", "."),
            ("a-b", "c-d"),
        ]
        for group, topic in cases:
            resolved = _offset_path(offsets_dir, group, topic).resolve()
            assert resolved.parent == offsets_dir.resolve()

    def test_load_falls_back_to_legacy_scheme_file(self, offsets_dir):
        """An offset written under the old scheme is still readable after upgrade."""
        import json

        # Old scheme filename for (group='g', topic='topic-a') was "g-topic-a.json".
        legacy = offsets_dir / "g-topic-a.json"
        legacy.write_text(json.dumps({"offset": 55}))

        assert load(offsets_dir, group="g", topic="topic-a") == 55

    def test_save_writes_new_scheme_not_legacy(self, offsets_dir):
        """New writes use the injective encoding, not the legacy raw-hyphen name."""
        save(offsets_dir, group="g", topic="topic-a", offset=5)

        assert not (offsets_dir / "g-topic-a.json").exists()
        assert load(offsets_dir, group="g", topic="topic-a") == 5
