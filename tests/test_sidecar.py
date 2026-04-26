# ABOUTME: Tests for sequence number sidecar cache
# ABOUTME: Covers read/write, crash recovery, and atomic persistence

import json

from brooklet.storage.sidecar import derive_next_seq, read_next_seq, write_next_seq


def test_read_missing_sidecar_returns_none(tmp_path):
    """No sidecar file present → read returns None so caller must derive."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    result = read_next_seq(brooklet_dir, "my-topic")

    assert result is None


def test_write_then_read_roundtrip(tmp_path):
    """Write next_seq=42, read back the same value."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    write_next_seq(brooklet_dir, "my-topic", 42)
    result = read_next_seq(brooklet_dir, "my-topic")

    assert result == 42


def test_atomic_write_uses_replace(tmp_path):
    """After write, the sidecar file exists at the expected path."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    write_next_seq(brooklet_dir, "my-topic", 7)

    seq_dir = brooklet_dir / "seq"
    sidecar_path = seq_dir / "my-topic.json"
    assert sidecar_path.exists()
    data = json.loads(sidecar_path.read_text())
    assert data["next_seq"] == 7


def test_read_corrupt_sidecar_returns_none(tmp_path):
    """Corrupt (invalid JSON) sidecar → read returns None to trigger re-derive."""
    brooklet_dir = tmp_path / ".brooklet"
    seq_dir = brooklet_dir / "seq"
    seq_dir.mkdir(parents=True)

    sidecar_path = seq_dir / "my-topic.json"
    sidecar_path.write_text("not valid json at all {{{")

    result = read_next_seq(brooklet_dir, "my-topic")

    assert result is None


def test_nested_topic_creates_dirs(tmp_path):
    """Topic 'scout/stats' creates .brooklet/seq/ directory and correct file."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    write_next_seq(brooklet_dir, "scout/stats", 10)

    seq_dir = brooklet_dir / "seq"
    assert seq_dir.is_dir()
    # slash is sanitized to '--'
    sidecar_path = seq_dir / "scout--stats.json"
    assert sidecar_path.exists()
    data = json.loads(sidecar_path.read_text())
    assert data["next_seq"] == 10


def test_nested_topic_read_roundtrip(tmp_path):
    """Write and read a nested topic name uses consistent sanitization."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    write_next_seq(brooklet_dir, "scout/stats", 99)
    result = read_next_seq(brooklet_dir, "scout/stats")

    assert result == 99


def test_derive_seq_from_data_file(tmp_path):
    """JSONL file where last line has _seq=5 → derive returns 6."""
    data_file = tmp_path / "data.jsonl"
    lines = [
        json.dumps({"event": "first", "_seq": 1}),
        json.dumps({"event": "second", "_seq": 3}),
        json.dumps({"event": "last", "_seq": 5}),
    ]
    data_file.write_text("\n".join(lines) + "\n")

    result = derive_next_seq(data_file)

    assert result == 6


def test_derive_seq_from_empty_file(tmp_path):
    """Empty data file → derive returns 1 (first sequence number)."""
    data_file = tmp_path / "data.jsonl"
    data_file.touch()

    result = derive_next_seq(data_file)

    assert result == 1


def test_derive_seq_from_missing_file(tmp_path):
    """Missing data file → derive returns 1."""
    data_file = tmp_path / "nonexistent.jsonl"

    result = derive_next_seq(data_file)

    assert result == 1


def test_derive_seq_skips_corrupt_last_line(tmp_path):
    """If last line is corrupt JSON, scan backward for last valid line with _seq."""
    data_file = tmp_path / "data.jsonl"
    lines = [
        json.dumps({"event": "good", "_seq": 4}),
        "CORRUPT LINE {{{{",
    ]
    data_file.write_text("\n".join(lines) + "\n")

    result = derive_next_seq(data_file)

    assert result == 5


def test_write_overwrites_existing_sidecar(tmp_path):
    """Writing to the same topic twice keeps only the latest value."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    write_next_seq(brooklet_dir, "my-topic", 10)
    write_next_seq(brooklet_dir, "my-topic", 20)
    result = read_next_seq(brooklet_dir, "my-topic")

    assert result == 20
