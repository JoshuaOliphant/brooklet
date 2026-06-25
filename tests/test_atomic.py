# ABOUTME: Tests for the atomic_write_text crash-safety primitive
# ABOUTME: Verifies durable writes, parent-dir creation, and temp-file cleanup on failure

import os as os_mod

import pytest

from brooklet.storage.atomic import atomic_write_text


def test_writes_text_to_path(tmp_path):
    target = tmp_path / "out.txt"
    atomic_write_text(target, "hello world")
    assert target.read_text() == "hello world"


def test_creates_missing_parent_dirs(tmp_path):
    target = tmp_path / "a" / "b" / "c.txt"
    atomic_write_text(target, "deep")
    assert target.read_text() == "deep"


def test_overwrites_existing_file(tmp_path):
    target = tmp_path / "out.txt"
    target.write_text("old")
    atomic_write_text(target, "new")
    assert target.read_text() == "new"


def test_leaves_no_temp_files_on_success(tmp_path):
    target = tmp_path / "out.txt"
    atomic_write_text(target, "x")
    leftovers = [p for p in tmp_path.iterdir() if p.suffix == ".tmp"]
    assert leftovers == []


def test_cleans_up_temp_on_replace_failure(tmp_path, monkeypatch):
    """If os.replace fails (after fd is closed), the temp file is removed and the
    original error propagates without a secondary close-of-closed-fd error."""
    target = tmp_path / "out.txt"

    def failing_replace(src, dst):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(os_mod, "replace", failing_replace)
    with pytest.raises(OSError, match="simulated replace failure"):
        atomic_write_text(target, "x")

    leftovers = [p for p in tmp_path.iterdir() if p.suffix == ".tmp"]
    assert leftovers == []


def test_cleans_up_temp_on_write_failure(tmp_path, monkeypatch):
    """If os.write fails (before fd is closed), the fd is closed and the temp
    file removed during cleanup."""
    target = tmp_path / "out.txt"

    def boom_write(fd, data):
        raise OSError("simulated write failure")

    monkeypatch.setattr(os_mod, "write", boom_write)
    with pytest.raises(OSError, match="simulated write failure"):
        atomic_write_text(target, "x")

    leftovers = [p for p in tmp_path.iterdir() if p.suffix == ".tmp"]
    assert leftovers == []
