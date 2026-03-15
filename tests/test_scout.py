# ABOUTME: Unit tests for claude analytics parsing and aggregation
# ABOUTME: Tests parsing, aggregation, window mode, and active duration

import json
import os
import time

from brooklet.contrib.claude_analytics import (
    SessionStats,
    _parse_file_events,
    aggregate_session,
    parse_session_event,
    scan_sessions,
)
from tests.scout_helpers import make_session_event, write_session_file


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

    def test_current_multiple_active_sessions(self, tmp_path):
        """--current with default window returns all files modified within 30 minutes."""
        sessions = tmp_path / "sessions"
        sessions.mkdir()

        now = time.time()
        # File 1: modified 10 minutes ago (within window)
        f1 = write_session_file(sessions, "session-recent-1", [
            make_session_event("user", "session-recent-1", "2026-03-15T02:00:00Z"),
            make_session_event("assistant", "session-recent-1", "2026-03-15T02:01:00Z"),
        ])
        os.utime(f1, (now - 600, now - 600))

        # File 2: modified 5 minutes ago (within window)
        f2 = write_session_file(sessions, "session-recent-2", [
            make_session_event("user", "session-recent-2", "2026-03-15T03:00:00Z"),
            make_session_event("assistant", "session-recent-2", "2026-03-15T03:01:00Z"),
        ])
        os.utime(f2, (now - 300, now - 300))

        # File 3: modified 2 hours ago (outside window)
        f3 = write_session_file(sessions, "session-old", [
            make_session_event("user", "session-old", "2026-03-15T00:00:00Z"),
            make_session_event("assistant", "session-old", "2026-03-15T00:01:00Z"),
        ])
        os.utime(f3, (now - 7200, now - 7200))

        stats = list(scan_sessions(str(sessions), current=True))
        assert len(stats) == 2
        session_ids = {s.session_id for s in stats}
        assert "session-recent-1" in session_ids
        assert "session-recent-2" in session_ids

    def test_current_window_zero_single_file(self, tmp_path):
        """--window 0 returns only the single most recent file (backward compat)."""
        sessions = tmp_path / "sessions"
        sessions.mkdir()

        now = time.time()
        f1 = write_session_file(sessions, "session-a", [
            make_session_event("user", "session-a", "2026-03-15T02:00:00Z"),
            make_session_event("assistant", "session-a", "2026-03-15T02:01:00Z"),
        ])
        os.utime(f1, (now - 60, now - 60))

        f2 = write_session_file(sessions, "session-b", [
            make_session_event("user", "session-b", "2026-03-15T03:00:00Z"),
            make_session_event("assistant", "session-b", "2026-03-15T03:01:00Z"),
        ])
        os.utime(f2, (now - 10, now - 10))

        stats = list(scan_sessions(str(sessions), current=True, window_minutes=0))
        assert len(stats) == 1
        assert stats[0].session_id == "session-b"

    def test_current_all_files_outside_window(self, tmp_path, capsys):
        """--current with all files older than the window prints message."""
        sessions = tmp_path / "sessions"
        sessions.mkdir()

        old_time = time.time() - 7200  # 2 hours ago
        f1 = write_session_file(sessions, "session-old", [
            make_session_event("user", "session-old", "2026-03-15T00:00:00Z"),
            make_session_event("assistant", "session-old", "2026-03-15T00:01:00Z"),
        ])
        os.utime(f1, (old_time, old_time))

        stats = list(scan_sessions(str(sessions), current=True, window_minutes=5))
        assert stats == []
        captured = capsys.readouterr()
        assert "No active session found" in captured.err

    def test_current_follow_detects_new_files(self, tmp_path):
        """Follow mode detects new session files appearing in the directory."""
        import threading

        sessions = tmp_path / "sessions"
        sessions.mkdir()

        # Start with one file
        write_session_file(sessions, "session-initial", [
            make_session_event("user", "session-initial", "2026-03-15T02:00:00Z"),
            make_session_event("assistant", "session-initial", "2026-03-15T02:01:00Z"),
        ])

        collected: list = []

        def collect_stats():
            for stats in scan_sessions(
                str(sessions), current=True, follow=True, window_minutes=30
            ):
                collected.append(stats)
                if len(collected) >= 2:
                    break

        thread = threading.Thread(target=collect_stats, daemon=True)
        thread.start()

        # Wait for initial parse, then add a new file
        time.sleep(3)
        write_session_file(sessions, "session-new", [
            make_session_event("user", "session-new", "2026-03-15T03:00:00Z"),
            make_session_event("assistant", "session-new", "2026-03-15T03:01:00Z"),
        ])

        thread.join(timeout=10)
        assert len(collected) >= 2
        session_ids = {s.session_id for s in collected if not s.removed}
        assert "session-initial" in session_ids
        assert "session-new" in session_ids

    def test_current_follow_yields_removal_on_file_age_out(self, tmp_path):
        """Follow mode yields removed=True when a file falls outside the window."""
        import threading

        sessions = tmp_path / "sessions"
        sessions.mkdir()

        # Create a file that's just barely inside a 1-second window
        f1 = write_session_file(sessions, "session-ephemeral", [
            make_session_event("user", "session-ephemeral", "2026-03-15T02:00:00Z"),
            make_session_event("assistant", "session-ephemeral", "2026-03-15T02:01:00Z"),
        ])

        collected: list = []

        def collect_stats():
            # Use a very small window so the file ages out quickly
            for stats in scan_sessions(
                str(sessions), current=True, follow=True, window_minutes=1
            ):
                collected.append(stats)
                if stats.removed:
                    break

        thread = threading.Thread(target=collect_stats, daemon=True)
        thread.start()

        # Wait for initial parse, then age the file out of the window
        time.sleep(3)
        old_time = time.time() - 120  # 2 minutes ago, outside 1-min window
        os.utime(f1, (old_time, old_time))

        thread.join(timeout=10)
        removed = [s for s in collected if s.removed]
        assert len(removed) >= 1
        assert removed[0].session_id == "session-ephemeral"


class TestActiveDuration:
    """Test active_duration_s — idle gap detection in aggregate_session."""

    def test_aggregate_active_duration_with_gaps(self):
        """Events with a 10min gap → active_duration < duration."""
        events = [
            {
                "type": "user",
                "timestamp": "2026-03-15T02:00:00.000Z",
                "tokens": {},
                "tools": {},
                "model": None,
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:01:00.000Z",
                "tokens": {"input": 100, "output": 50},
                "tools": {},
                "model": "claude-opus-4-6",
            },
            # 10 minute gap (600s > IDLE_GAP_THRESHOLD of 300s)
            {
                "type": "user",
                "timestamp": "2026-03-15T02:11:00.000Z",
                "tokens": {},
                "tools": {},
                "model": None,
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:12:00.000Z",
                "tokens": {"input": 100, "output": 50},
                "tools": {},
                "model": "claude-opus-4-6",
            },
        ]
        stats = aggregate_session("sess-gap", events)
        # Wall-clock: 12 minutes = 720s
        assert stats.duration_s == 720.0
        # Active: 60s (0:00-0:01) + 60s (0:11-0:12) = 120s
        # The 600s gap is excluded
        assert stats.active_duration_s == 120.0

    def test_aggregate_no_gaps_continuous(self):
        """Continuous events → active_duration ≈ duration."""
        events = [
            {
                "type": "user",
                "timestamp": "2026-03-15T02:00:00.000Z",
                "tokens": {},
                "tools": {},
                "model": None,
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:01:00.000Z",
                "tokens": {"input": 100, "output": 50},
                "tools": {},
                "model": "claude-opus-4-6",
            },
            {
                "type": "user",
                "timestamp": "2026-03-15T02:02:00.000Z",
                "tokens": {},
                "tools": {},
                "model": None,
            },
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:03:00.000Z",
                "tokens": {"input": 100, "output": 50},
                "tools": {},
                "model": "claude-opus-4-6",
            },
        ]
        stats = aggregate_session("sess-continuous", events)
        # Wall-clock = active time = 3 minutes = 180s (no idle gaps)
        assert stats.duration_s == 180.0
        assert stats.active_duration_s == 180.0

    def test_aggregate_all_idle(self):
        """Huge gaps between every event → active_duration ≈ 0."""
        events = [
            {
                "type": "user",
                "timestamp": "2026-03-15T02:00:00.000Z",
                "tokens": {},
                "tools": {},
                "model": None,
            },
            # 30 minute gap
            {
                "type": "assistant",
                "timestamp": "2026-03-15T02:30:00.000Z",
                "tokens": {"input": 100, "output": 50},
                "tools": {},
                "model": "claude-opus-4-6",
            },
            # 30 minute gap
            {
                "type": "user",
                "timestamp": "2026-03-15T03:00:00.000Z",
                "tokens": {},
                "tools": {},
                "model": None,
            },
        ]
        stats = aggregate_session("sess-idle", events)
        # Wall-clock: 60 minutes = 3600s
        assert stats.duration_s == 3600.0
        # Active: 0s (all gaps exceed threshold)
        assert stats.active_duration_s == 0.0

    def test_to_dict_includes_active_duration(self):
        """SessionStats.to_dict() includes active_duration_s."""
        stats = SessionStats(
            session_id="abc-123",
            event_count=10,
            duration_s=120.0,
            active_duration_s=90.0,
            model="claude-opus-4-6",
        )
        d = stats.to_dict()
        assert d["active_duration_s"] == 90.0

    def test_aggregate_empty_events_zero_active_duration(self):
        """Empty event list → active_duration_s is 0."""
        stats = aggregate_session("empty", [])
        assert stats.active_duration_s == 0.0
