# ABOUTME: Scout analytics module — scans Claude Code session JSONL files for usage stats
# ABOUTME: Exercises brooklet consume() with glob mode and produce() for JSONL output

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import brooklet

# ---------------------------------------------------------------------------
# Layer 1: Parsing (pure functions, no I/O)
# ---------------------------------------------------------------------------

TOKEN_FIELDS = {
    "input_tokens": "input",
    "output_tokens": "output",
    "cache_read_input_tokens": "cache_read",
    "cache_creation_input_tokens": "cache_create",
}


def parse_session_event(event: dict) -> dict | None:
    """Extract stats from a single JSONL event.

    Returns a normalized dict with type, timestamp, model, tokens, and tools.
    Returns None if the event lacks a recognizable type.
    """
    event_type = event.get("type")
    if event_type not in ("assistant", "user", "progress"):
        return None

    timestamp = event.get("timestamp")
    result: dict = {
        "type": event_type,
        "timestamp": timestamp,
        "model": None,
        "tokens": {},
        "tools": {},
    }

    if event_type == "assistant":
        message = event.get("message", {})
        result["model"] = message.get("model")

        # Extract token usage
        usage = message.get("usage", {})
        if usage:
            tokens = {}
            for raw_key, friendly_key in TOKEN_FIELDS.items():
                val = usage.get(raw_key, 0)
                if val:
                    tokens[friendly_key] = val
            result["tokens"] = tokens

        # Extract tool calls from content blocks
        content = message.get("content", [])
        tool_counts: dict[str, int] = {}
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                name = block.get("name", "unknown")
                tool_counts[name] = tool_counts.get(name, 0) + 1
        result["tools"] = tool_counts

    return result


@dataclass
class SessionStats:
    """Aggregated statistics for a single Claude Code session."""

    session_id: str
    event_count: int = 0
    duration_s: float = 0.0
    model: str | None = None
    tokens: dict[str, int] = field(
        default_factory=lambda: {"input": 0, "output": 0, "cache_read": 0, "cache_create": 0}
    )
    tools: dict[str, int] = field(default_factory=dict)
    start_time: str | None = None

    def to_dict(self) -> dict:
        """Convert to a plain dict for JSONL serialization."""
        return {
            "session_id": self.session_id,
            "event_count": self.event_count,
            "duration_s": self.duration_s,
            "model": self.model,
            "tokens": dict(self.tokens),
            "tools": dict(self.tools),
            "start_time": self.start_time,
        }


def aggregate_session(session_id: str, events: list[dict]) -> SessionStats:
    """Aggregate parsed events into per-session statistics.

    Args:
        session_id: The session identifier (typically from filename).
        events: List of dicts returned by parse_session_event().
    """
    stats = SessionStats(session_id=session_id)
    stats.event_count = len(events)

    if not events:
        return stats

    # Collect timestamps for duration calculation
    timestamps = []
    model = None

    for ev in events:
        ts = ev.get("timestamp")
        if ts:
            timestamps.append(ts)

        # First model wins
        if model is None and ev.get("model"):
            model = ev["model"]

        # Accumulate tokens
        for key, val in ev.get("tokens", {}).items():
            stats.tokens[key] = stats.tokens.get(key, 0) + val

        # Accumulate tools
        for tool_name, count in ev.get("tools", {}).items():
            stats.tools[tool_name] = stats.tools.get(tool_name, 0) + count

    stats.model = model

    # Duration: max - min timestamp
    if len(timestamps) >= 2:
        parsed = []
        for ts in timestamps:
            if not isinstance(ts, str):
                continue
            try:
                # Handle both Z and +00:00 suffixes
                parsed.append(datetime.fromisoformat(ts.replace("Z", "+00:00")))
            except ValueError:
                continue
        if len(parsed) >= 2:
            stats.duration_s = (max(parsed) - min(parsed)).total_seconds()

    if timestamps:
        stats.start_time = timestamps[0]

    return stats


# ---------------------------------------------------------------------------
# Layer 2: Consumer integration (uses brooklet API)
# ---------------------------------------------------------------------------


def _session_id_from_path(filepath: str) -> str:
    """Extract session ID from a JSONL filename.

    Claude Code session files are named like: <uuid>.jsonl
    """
    name = Path(filepath).stem
    return name


def _parse_file_events(filepath: str) -> list[dict]:
    """Parse all events from a single session JSONL file."""
    events = []
    skipped_lines = 0
    total_lines = 0
    try:
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total_lines += 1
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    skipped_lines += 1
                    continue
                parsed = parse_session_event(raw)
                if parsed is not None:
                    events.append(parsed)
    except OSError as e:
        print(f"Warning: skipping {filepath}: {e}", file=sys.stderr)
    if skipped_lines and total_lines:
        print(
            f"Warning: {filepath}: {skipped_lines}/{total_lines} lines failed JSON parsing",
            file=sys.stderr,
        )
    return events


def scan_sessions(
    path: str,
    follow: bool = False,
    current: bool = False,
) -> Iterator[SessionStats]:
    """Scan session files and yield per-session stats.

    Uses brooklet's glob consumer for offset tracking. Each session JSONL
    file is a separate unit — events are naturally grouped by file.

    Args:
        path: Directory containing session JSONL files.
        follow: If True, tail for new sessions via glob+follow.
        current: If True, only process the most recently modified session.
    """
    session_dir = Path(path)
    if not session_dir.is_dir():
        print(f"Error: {path} is not a directory", file=sys.stderr)
        return

    if current:
        # Find the most recently modified .jsonl file
        def _safe_mtime(p: Path) -> float:
            try:
                return p.stat().st_mtime
            except OSError:
                return 0.0

        jsonl_files = sorted(session_dir.glob("*.jsonl"), key=_safe_mtime)
        if not jsonl_files:
            print("No active session found", file=sys.stderr)
            return
        target = jsonl_files[-1]
        session_id = _session_id_from_path(str(target))

        if follow:
            # Use brooklet single-file follow for live tailing
            stream = brooklet.open(str(session_dir))
            stream.register("current-session", str(target), "single-file")
            events = []
            with stream.consume("current-session", group="scout-current", follow=True) as consumer:
                for raw_event in consumer:
                    parsed = parse_session_event(raw_event)
                    if parsed is not None:
                        events.append(parsed)
                        yield aggregate_session(session_id, events)
        else:
            events = _parse_file_events(str(target))
            if events:
                yield aggregate_session(session_id, events)
        return

    # Glob mode: process all session files
    jsonl_files = sorted(session_dir.glob("*.jsonl"))

    if not jsonl_files:
        print("No sessions found", file=sys.stderr)
        return

    if follow:
        # Use brooklet glob+follow for tailing
        glob_pattern = str(session_dir / "*.jsonl")
        stream = brooklet.open(str(session_dir))
        stream.register("sessions", glob_pattern, "glob")

        current_file = None
        current_events: list[dict] = []

        with stream.consume("sessions", group="scout-follow", follow=True) as consumer:
            for raw_event in consumer:
                # Determine which session this event belongs to
                src = raw_event.get("_src", "")
                session_id = _session_id_from_path(src) if src else "unknown"

                if current_file is None:
                    current_file = session_id

                if session_id != current_file and current_events:
                    yield aggregate_session(current_file, current_events)
                    current_events = []
                    current_file = session_id

                parsed = parse_session_event(raw_event)
                if parsed is not None:
                    current_events.append(parsed)

            # Yield remaining events
            if current_events and current_file:
                yield aggregate_session(current_file, current_events)
    else:
        # Batch mode: read each file directly for simplicity and reliability
        for filepath in jsonl_files:
            session_id = _session_id_from_path(str(filepath))
            events = _parse_file_events(str(filepath))
            if events:
                yield aggregate_session(session_id, events)


# ---------------------------------------------------------------------------
# Layer 3: Output renderers
# ---------------------------------------------------------------------------


@dataclass
class CumulativeStats:
    """Running totals across all processed sessions."""

    session_count: int = 0
    total_events: int = 0
    total_duration_s: float = 0.0
    tokens: dict[str, int] = field(
        default_factory=lambda: {"input": 0, "output": 0, "cache_read": 0, "cache_create": 0}
    )
    tools: Counter = field(default_factory=Counter)
    models: Counter = field(default_factory=Counter)

    def update(self, stats: SessionStats) -> None:
        """Add a session's stats to the running totals."""
        self.session_count += 1
        self.total_events += stats.event_count
        self.total_duration_s += stats.duration_s
        for key, val in stats.tokens.items():
            self.tokens[key] = self.tokens.get(key, 0) + val
        self.tools.update(stats.tools)
        if stats.model:
            self.models[stats.model] += 1


def _format_duration(seconds: float) -> str:
    """Format seconds into a human-readable duration string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60
    return f"{hours:.1f}h"


def _format_number(n: int) -> str:
    """Format a number with comma separators."""
    return f"{n:,}"


def render_session_block(stats: SessionStats) -> str:
    """Render a single session stats block as plain text."""
    lines = []
    ts_display = stats.start_time[:16].replace("T", " ") if stats.start_time else "unknown"
    lines.append(f"--- session {stats.session_id[:8]} ({ts_display}) ---")
    lines.append(
        f"  events: {stats.event_count}  "
        f"duration: {_format_duration(stats.duration_s)}  "
        f"model: {stats.model or 'unknown'}"
    )

    token_parts = []
    for key in ("input", "output", "cache_read", "cache_create"):
        val = stats.tokens.get(key, 0)
        if val:
            token_parts.append(f"{key}={_format_number(val)}")
    if token_parts:
        lines.append(f"  tokens: {' '.join(token_parts)}")

    if stats.tools:
        sorted_tools = sorted(stats.tools.items(), key=lambda x: x[1], reverse=True)
        tool_parts = [f"{name}={count}" for name, count in sorted_tools[:10]]
        lines.append(f"  tools: {' '.join(tool_parts)}")

    return "\n".join(lines)


def render_cumulative_block(cumulative: CumulativeStats) -> str:
    """Render cumulative totals as plain text."""
    lines = []
    lines.append(f"\n=== cumulative totals ({cumulative.session_count} sessions) ===")

    token_parts = []
    for key in ("input", "output", "cache_read", "cache_create"):
        val = cumulative.tokens.get(key, 0)
        if val:
            token_parts.append(f"{key}={_format_number(val)}")
    if token_parts:
        lines.append(f"  tokens: {' '.join(token_parts)}")

    if cumulative.models:
        model_parts = [f"{model}={count}" for model, count in cumulative.models.most_common()]
        lines.append(f"  models: {' '.join(model_parts)}")

    if cumulative.tools:
        sorted_tools = cumulative.tools.most_common(10)
        tool_parts = [f"{name}={count}" for name, count in sorted_tools]
        lines.append(f"  tools: {' '.join(tool_parts)}")

    if cumulative.session_count > 0:
        avg_events = cumulative.total_events / cumulative.session_count
        avg_duration = cumulative.total_duration_s / cumulative.session_count
        lines.append(
            f"  sessions: avg={avg_events:.0f} events, "
            f"avg={_format_duration(avg_duration)} duration"
        )

    return "\n".join(lines)


def render_streaming(stats_iter: Iterator[SessionStats], output_file=None) -> str:
    """Print session blocks + cumulative totals to stdout.

    Returns the full output as a string (useful for testing).
    """
    cumulative = CumulativeStats()
    output_lines = []

    for stats in stats_iter:
        cumulative.update(stats)
        block = render_session_block(stats)
        output_lines.append(block)
        if output_file is None:
            print(block)

    total_block = render_cumulative_block(cumulative)
    output_lines.append(total_block)
    if output_file is None:
        print(total_block)

    return "\n".join(output_lines)


def render_rich(stats_iter: Iterator[SessionStats]) -> None:
    """Live-updating rich dashboard. Requires rich optional dependency."""
    try:
        from rich.console import Console
        from rich.live import Live
        from rich.table import Table
    except ImportError:
        print("Error: rich is required for --rich mode. Install with: uv add brooklet[rich]",
              file=sys.stderr)
        sys.exit(1)

    console = Console()
    cumulative = CumulativeStats()
    sessions: list[SessionStats] = []

    def build_table() -> Table:
        table = Table(title="Scout Analytics Dashboard")
        table.add_column("Session", style="cyan", no_wrap=True)
        table.add_column("Events", justify="right")
        table.add_column("Duration", justify="right")
        table.add_column("Model", style="green")
        table.add_column("Input Tokens", justify="right")
        table.add_column("Output Tokens", justify="right")
        table.add_column("Top Tools")

        for s in sessions:
            top_tools = ", ".join(
                f"{name}({c})" for name, c in
                sorted(s.tools.items(), key=lambda x: x[1], reverse=True)[:3]
            )
            table.add_row(
                s.session_id[:8],
                str(s.event_count),
                _format_duration(s.duration_s),
                s.model or "?",
                _format_number(s.tokens.get("input", 0)),
                _format_number(s.tokens.get("output", 0)),
                top_tools,
            )

        # Totals row
        if sessions:
            table.add_section()
            table.add_row(
                f"TOTAL ({cumulative.session_count})",
                str(cumulative.total_events),
                _format_duration(cumulative.total_duration_s),
                ", ".join(f"{m}={c}" for m, c in cumulative.models.most_common()),
                _format_number(cumulative.tokens.get("input", 0)),
                _format_number(cumulative.tokens.get("output", 0)),
                ", ".join(f"{n}({c})" for n, c in cumulative.tools.most_common(5)),
            )

        return table

    # Track session positions for in-place updates (--current --follow)
    session_index: dict[str, int] = {}

    with Live(build_table(), console=console, refresh_per_second=4) as live:
        for stats in stats_iter:
            if stats.session_id in session_index:
                # Replace existing entry — same session with updated stats
                idx = session_index[stats.session_id]
                sessions[idx] = stats
                # Rebuild cumulative from scratch since we replaced
                cumulative = CumulativeStats()
                for s in sessions:
                    cumulative.update(s)
            else:
                # New session
                cumulative.update(stats)
                session_index[stats.session_id] = len(sessions)
                sessions.append(stats)
            live.update(build_table())


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for brooklet-scout."""
    parser = argparse.ArgumentParser(
        prog="brooklet-scout",
        description="Scan Claude Code session JSONL files and report analytics.",
    )
    parser.add_argument(
        "path",
        help="Path to Claude Code project directory containing session JSONL files",
    )
    parser.add_argument(
        "--current",
        action="store_true",
        help="Only process the most recently modified session",
    )
    parser.add_argument(
        "--follow",
        action="store_true",
        help="Tail for new sessions/events",
    )
    parser.add_argument(
        "--rich",
        action="store_true",
        help="Display a live-updating rich dashboard",
    )
    parser.add_argument(
        "--output",
        metavar="TOPIC",
        help="Produce session stats as JSONL events to a brooklet topic",
    )

    args = parser.parse_args(argv)

    stats_iter = scan_sessions(
        path=args.path,
        follow=args.follow,
        current=args.current,
    )

    # If --output is specified, wrap the iterator to produce events
    if args.output:
        stream = brooklet.open(args.path)
        original_iter = stats_iter

        def producing_iter():
            for stats in original_iter:
                stream.produce(args.output, stats.to_dict(), source="scout")
                yield stats

        stats_iter = producing_iter()

    if args.rich:
        render_rich(stats_iter)
    else:
        render_streaming(stats_iter)
