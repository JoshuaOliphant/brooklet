# ABOUTME: Unit tests for scout analytics parsing and aggregation (pure functions)
# ABOUTME: Tests parse_session_event, aggregate_session, and SessionStats

import json

from brooklet.contrib.scout import (
    SessionStats,
    _parse_file_events,
    aggregate_session,
    parse_session_event,
    scan_sessions,
)


class TestParseSessionEvent:
    """Test parse_session_event — extracts stats from a single JSONL event."""

    def test_parse_assistant_event_extracts_tokens(self):
        event = {
            "type": "assistant",
            "timestamp": "2026-03-15T02:47:26.556Z",
            "sessionId": "abc-123",
            "message": {
                "model": "claude-opus-4-6",
                "role": "assistant",
                "content": [{"type": "text", "text": "hello"}],
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cache_read_input_tokens": 30,
                    "cache_creation_input_tokens": 20,
                },
            },
        }
        result = parse_session_event(event)
        assert result is not None
        assert result["type"] == "assistant"
        assert result["tokens"]["input"] == 100
        assert result["tokens"]["output"] == 50
        assert result["tokens"]["cache_read"] == 30
        assert result["tokens"]["cache_create"] == 20
        assert result["model"] == "claude-opus-4-6"
        assert result["timestamp"] == "2026-03-15T02:47:26.556Z"

    def test_parse_assistant_event_extracts_tools(self):
        event = {
            "type": "assistant",
            "timestamp": "2026-03-15T02:48:00Z",
            "sessionId": "abc-123",
            "message": {
                "model": "claude-opus-4-6",
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Let me read that."},
                    {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
                    {"type": "tool_use", "id": "t2", "name": "Bash", "input": {}},
                    {"type": "tool_use", "id": "t3", "name": "Read", "input": {}},
                ],
                "usage": {
                    "input_tokens": 50,
                    "output_tokens": 25,
                },
            },
        }
        result = parse_session_event(event)
        assert result["tools"] == {"Read": 2, "Bash": 1}

    def test_parse_user_event_counts_only(self):
        event = {
            "type": "user",
            "timestamp": "2026-03-15T02:46:00Z",
            "sessionId": "abc-123",
        }
        result = parse_session_event(event)
        assert result is not None
        assert result["type"] == "user"
        assert result["tokens"] == {}
        assert result["tools"] == {}

    def test_parse_progress_event_counts_only(self):
        event = {
            "type": "progress",
            "timestamp": "2026-03-15T02:46:30Z",
            "sessionId": "abc-123",
        }
        result = parse_session_event(event)
        assert result is not None
        assert result["type"] == "progress"
        assert result["tokens"] == {}
        assert result["tools"] == {}

    def test_parse_malformed_event_returns_none(self):
        result = parse_session_event({})
        assert result is None

    def test_parse_event_missing_usage_returns_zero_tokens(self):
        event = {
            "type": "assistant",
            "timestamp": "2026-03-15T02:48:00Z",
            "sessionId": "abc-123",
            "message": {
                "model": "claude-opus-4-6",
                "role": "assistant",
                "content": [{"type": "text", "text": "hello"}],
            },
        }
        result = parse_session_event(event)
        assert result is not None
        assert result["tokens"] == {}

    def test_parse_event_missing_content_returns_no_tools(self):
        event = {
            "type": "assistant",
            "timestamp": "2026-03-15T02:48:00Z",
            "sessionId": "abc-123",
            "message": {
                "model": "claude-opus-4-6",
                "role": "assistant",
                "usage": {"input_tokens": 10, "output_tokens": 5},
            },
        }
        result = parse_session_event(event)
        assert result is not None
        assert result["tools"] == {}


class TestAggregateSession:
    """Test aggregate_session — combines parsed events into SessionStats."""

    def test_aggregate_empty_list(self):
        stats = aggregate_session("test-session", [])
        assert stats.session_id == "test-session"
        assert stats.event_count == 0
        assert stats.duration_s == 0.0
        assert stats.model is None
        assert stats.tokens == {"input": 0, "output": 0, "cache_read": 0, "cache_create": 0}
        assert stats.tools == {}

    def test_aggregate_mixed_events(self):
        events = [
            {
                "type": "user",
                "timestamp": "2026-03-15T02:46:00.000Z",
                "tokens": {},
                "tools": {},
                "model": None,
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:47:00.000Z",
                "tokens": {"input": 100, "output": 50, "cache_read": 30, "cache_create": 20},
                "tools": {"Read": 2, "Bash": 1},
                "model": "claude-opus-4-6",
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:48:00.000Z",
                "tokens": {"input": 200, "output": 100, "cache_read": 50, "cache_create": 10},
                "tools": {"Read": 1, "Edit": 3},
                "model": "claude-opus-4-6",
            },
        ]
        stats = aggregate_session("sess-1", events)
        assert stats.session_id == "sess-1"
        assert stats.event_count == 3
        assert stats.duration_s == 120.0  # 2 minutes
        assert stats.model == "claude-opus-4-6"
        assert stats.tokens == {"input": 300, "output": 150, "cache_read": 80, "cache_create": 30}
        assert stats.tools == {"Read": 3, "Bash": 1, "Edit": 3}

    def test_aggregate_uses_first_model(self):
        events = [
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:47:00Z",
                "tokens": {"input": 10, "output": 5},
                "tools": {},
                "model": "claude-sonnet-4-6",
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:48:00Z",
                "tokens": {"input": 10, "output": 5},
                "tools": {},
                "model": "claude-opus-4-6",
            },
        ]
        stats = aggregate_session("sess-2", events)
        assert stats.model == "claude-sonnet-4-6"

    def test_aggregate_single_event_zero_duration(self):
        events = [
            {
                "type": "user",
                "timestamp": "2026-03-15T02:47:00Z",
                "tokens": {},
                "tools": {},
                "model": None,
            },
        ]
        stats = aggregate_session("sess-3", events)
        assert stats.duration_s == 0.0
        assert stats.event_count == 1


class TestSessionStats:
    """Test SessionStats dataclass methods."""

    def test_to_dict(self):
        stats = SessionStats(
            session_id="abc-123",
            event_count=10,
            duration_s=120.0,
            model="claude-opus-4-6",
            tokens={"input": 100, "output": 50, "cache_read": 30, "cache_create": 20},
            tools={"Read": 5, "Bash": 3},
            start_time="2026-03-15T02:47:00Z",
        )
        d = stats.to_dict()
        assert d["session_id"] == "abc-123"
        assert d["event_count"] == 10
        assert d["duration_s"] == 120.0
        assert d["model"] == "claude-opus-4-6"
        assert d["tokens"]["input"] == 100
        assert d["tools"]["Read"] == 5


class TestParseFileEvents:
    """Test _parse_file_events — file-level parsing with error handling."""

    def test_malformed_jsonl_skips_bad_lines(self, tmp_path):
        """Corrupt lines are skipped; valid lines before and after are parsed."""
        path = tmp_path / "session.jsonl"
        path.write_text(
            json.dumps({"type": "user", "timestamp": "2026-03-15T02:00:00Z", "sessionId": "x"})
            + "\n"
            + "THIS IS NOT JSON\n"
            + json.dumps({"type": "user", "timestamp": "2026-03-15T02:01:00Z", "sessionId": "x"})
            + "\n"
        )
        events = _parse_file_events(str(path))
        assert len(events) == 2

    def test_nonexistent_file_returns_empty(self, tmp_path, capsys):
        """A missing file returns empty list and prints a warning."""
        events = _parse_file_events(str(tmp_path / "does-not-exist.jsonl"))
        assert events == []
        captured = capsys.readouterr()
        assert "Warning" in captured.err

    def test_empty_file_returns_empty(self, tmp_path):
        """An empty file returns an empty list."""
        path = tmp_path / "empty.jsonl"
        path.touch()
        events = _parse_file_events(str(path))
        assert events == []


class TestScanSessions:
    """Test scan_sessions edge cases."""

    def test_nonexistent_directory(self, tmp_path, capsys):
        """Non-existent path prints error and yields nothing."""
        stats = list(scan_sessions(str(tmp_path / "nope")))
        assert stats == []
        captured = capsys.readouterr()
        assert "not a directory" in captured.err

    def test_empty_directory(self, tmp_path, capsys):
        """Empty directory prints message and yields nothing."""
        empty = tmp_path / "empty_sessions"
        empty.mkdir()
        stats = list(scan_sessions(str(empty)))
        assert stats == []
        captured = capsys.readouterr()
        assert "No sessions found" in captured.err

    def test_current_no_sessions(self, tmp_path, capsys):
        """--current with no sessions prints message and yields nothing."""
        empty = tmp_path / "empty_sessions"
        empty.mkdir()
        stats = list(scan_sessions(str(empty), current=True))
        assert stats == []
        captured = capsys.readouterr()
        assert "No active session found" in captured.err
