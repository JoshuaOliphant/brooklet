# ABOUTME: Shared test fixtures for brooklet tests
# ABOUTME: Provides tmp directories, sample JSONL files, and stream instances

import json

import pytest

from tests.scout_helpers import make_session_event, write_session_file


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


# ---------------------------------------------------------------------------
# Scout fixtures: Claude Code session JSONL data
# ---------------------------------------------------------------------------


@pytest.fixture
def session_dir(tmp_path):
    """Create a directory with 5 sample Claude Code session JSONL files."""
    sessions = tmp_path / "sessions"
    sessions.mkdir()

    # Session 1: opus model, 4 events
    write_session_file(sessions, "aaaa1111-0000-0000-0000-000000000001", [
        make_session_event("user", "aaaa1111-0000-0000-0000-000000000001",
                           "2026-03-15T02:00:00Z"),
        make_session_event("assistant", "aaaa1111-0000-0000-0000-000000000001",
                           "2026-03-15T02:01:00Z",
                           content=[
                               {"type": "text", "text": "Let me read."},
                               {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
                           ]),
        make_session_event("assistant", "aaaa1111-0000-0000-0000-000000000001",
                           "2026-03-15T02:02:00Z",
                           content=[
                               {"type": "tool_use", "id": "t2", "name": "Bash", "input": {}},
                               {"type": "tool_use", "id": "t3", "name": "Read", "input": {}},
                           ]),
        make_session_event("user", "aaaa1111-0000-0000-0000-000000000001",
                           "2026-03-15T02:03:00Z"),
    ])

    # Session 2: sonnet model, 3 events
    write_session_file(sessions, "bbbb2222-0000-0000-0000-000000000002", [
        make_session_event("user", "bbbb2222-0000-0000-0000-000000000002",
                           "2026-03-15T03:00:00Z"),
        make_session_event("assistant", "bbbb2222-0000-0000-0000-000000000002",
                           "2026-03-15T03:05:00Z",
                           model="claude-sonnet-4-6",
                           content=[
                               {"type": "tool_use", "id": "t4", "name": "Edit", "input": {}},
                           ]),
        make_session_event("user", "bbbb2222-0000-0000-0000-000000000002",
                           "2026-03-15T03:10:00Z"),
    ])

    # Session 3: opus model, 2 events
    write_session_file(sessions, "cccc3333-0000-0000-0000-000000000003", [
        make_session_event("user", "cccc3333-0000-0000-0000-000000000003",
                           "2026-03-15T04:00:00Z"),
        make_session_event("assistant", "cccc3333-0000-0000-0000-000000000003",
                           "2026-03-15T04:30:00Z"),
    ])

    # Session 4: opus, 3 events with progress
    write_session_file(sessions, "dddd4444-0000-0000-0000-000000000004", [
        make_session_event("user", "dddd4444-0000-0000-0000-000000000004",
                           "2026-03-15T05:00:00Z"),
        make_session_event("progress", "dddd4444-0000-0000-0000-000000000004",
                           "2026-03-15T05:01:00Z"),
        make_session_event("assistant", "dddd4444-0000-0000-0000-000000000004",
                           "2026-03-15T05:02:00Z",
                           content=[
                               {"type": "tool_use", "id": "t5", "name": "Write", "input": {}},
                               {"type": "tool_use", "id": "t6", "name": "Read", "input": {}},
                           ]),
    ])

    # Session 5: sonnet, 2 events
    write_session_file(sessions, "eeee5555-0000-0000-0000-000000000005", [
        make_session_event("user", "eeee5555-0000-0000-0000-000000000005",
                           "2026-03-15T06:00:00Z"),
        make_session_event("assistant", "eeee5555-0000-0000-0000-000000000005",
                           "2026-03-15T06:20:00Z",
                           model="claude-sonnet-4-6",
                           content=[
                               {"type": "tool_use", "id": "t7", "name": "Bash", "input": {}},
                           ]),
    ])

    return sessions
