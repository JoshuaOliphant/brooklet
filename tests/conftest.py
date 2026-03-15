# ABOUTME: Shared test fixtures for brooklet tests
# ABOUTME: Provides tmp directories, sample JSONL files, and stream instances

import json

import pytest


@pytest.fixture
def tmp_stream_dir(tmp_path):
    """A temporary directory for stream data."""
    return tmp_path / "streams"


@pytest.fixture
def sample_jsonl(tmp_path):
    """Create a sample JSONL file with test events."""
    path = tmp_path / "events.jsonl"
    events = [
        {"type": "start", "message": "hello"},
        {"type": "data", "value": 42},
        {"type": "end", "message": "goodbye"},
    ]
    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return path


@pytest.fixture
def empty_jsonl(tmp_path):
    """Create an empty JSONL file."""
    path = tmp_path / "empty.jsonl"
    path.touch()
    return path


@pytest.fixture
def brooklet_dir(tmp_path):
    """A temporary .brooklet metadata directory."""
    d = tmp_path / ".brooklet"
    d.mkdir()
    return d


@pytest.fixture
def offsets_dir(brooklet_dir):
    """A temporary offsets directory inside .brooklet."""
    d = brooklet_dir / "offsets"
    d.mkdir()
    return d
