# ABOUTME: pytest-reportlog adapter — consumes pytest JSONL and produces run stats
# ABOUTME: Exercises brooklet register/consume/produce with a non-Claude-Code source

from __future__ import annotations

import argparse
import glob as glob_module
import hashlib
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import brooklet

RECOGNIZED_REPORT_TYPES = {"SessionStart", "CollectReport", "TestReport", "SessionFinish"}


# ---------------------------------------------------------------------------
# Layer 1: Parsing (pure functions, no I/O)
# ---------------------------------------------------------------------------


def parse_test_event(event: dict) -> dict | None:
    """Extract fields from a single pytest-reportlog JSONL event.

    Returns a normalized dict with report_type, nodeid, outcome, when,
    duration, and longrepr. Returns None if the event lacks a recognized
    $report_type.
    """
    report_type = event.get("$report_type")
    if report_type not in RECOGNIZED_REPORT_TYPES:
        return None

    raw_duration = event.get("duration")
    duration = float(raw_duration) if isinstance(raw_duration, (int, float)) else 0.0

    return {
        "report_type": report_type,
        "nodeid": event.get("nodeid"),
        "outcome": event.get("outcome"),
        "when": event.get("when"),
        "duration": duration,
        "longrepr": event.get("longrepr"),
    }


def is_test_result(parsed: dict) -> bool:
    """Return True if the parsed event is an actual test execution result.

    Filters to TestReport events in the "call" phase, excluding
    setup/teardown lifecycle events and session-level reports.
    """
    return parsed.get("report_type") == "TestReport" and parsed.get("when") == "call"


@dataclass
class RunStats:
    """Aggregated statistics for a single pytest test run."""

    run_id: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errored: int = 0
    duration_s: float = 0.0
    slowest: list[dict] = field(default_factory=list)
    failures: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to a plain dict for JSONL serialization."""
        return {
            "run_id": self.run_id,
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "errored": self.errored,
            "duration_s": self.duration_s,
            "slowest": list(self.slowest),
            "failures": list(self.failures),
        }


def aggregate_run(run_id: str, events: list[dict]) -> RunStats:
    """Aggregate raw pytest-reportlog events into per-run statistics.

    Parses events, filters to test results (TestReport + call phase),
    then counts outcomes, sums duration, and collects slowest/failures.

    Args:
        run_id: Identifier for this run (typically filename stem).
        events: List of raw event dicts from the JSONL file.
    """
    stats = RunStats(run_id=run_id)

    parsed_results = []
    for raw in events:
        parsed = parse_test_event(raw)
        if parsed is not None and is_test_result(parsed):
            parsed_results.append(parsed)

    stats.total = len(parsed_results)

    for result in parsed_results:
        outcome = result["outcome"]
        if outcome == "passed":
            stats.passed += 1
        elif outcome == "failed":
            stats.failed += 1
            stats.failures.append({
                "nodeid": result["nodeid"],
                "longrepr": result.get("longrepr") or "",
            })
        elif outcome == "skipped":
            stats.skipped += 1
        elif outcome == "error":
            stats.errored += 1

        stats.duration_s += result.get("duration", 0.0) or 0.0

    # Slowest 5 tests by duration (descending)
    by_duration = sorted(parsed_results, key=lambda r: r.get("duration", 0.0), reverse=True)
    stats.slowest = [
        {"nodeid": r["nodeid"], "duration": r["duration"]}
        for r in by_duration[:5]
    ]

    return stats


# ---------------------------------------------------------------------------
# Layer 2: Consumer integration (uses brooklet API)
# ---------------------------------------------------------------------------


def _run_id_from_path(filepath: str) -> str:
    """Extract run ID from a JSONL filename (stem)."""
    return Path(filepath).stem


def _topic_for_path(path: str) -> str:
    """Derive a unique topic name from a file path or glob pattern.

    Uses a short hash of the absolute path to avoid offset collisions
    when different files are consumed from the same stream directory.
    """
    abs_path = str(Path(path).resolve())
    short_hash = hashlib.sha256(abs_path.encode()).hexdigest()[:8]
    return f"pytest/{short_hash}"


def scan_runs(
    path: str,
    mode: str = "single-file",
    follow: bool = False,
    stream: brooklet.Stream | None = None,
) -> Iterator[RunStats]:
    """Scan pytest report log(s) and yield per-run statistics.

    Uses brooklet's consumer API for offset tracking and follow mode.

    Args:
        path: File path (single-file) or glob pattern (glob mode).
        mode: Either "single-file" or "glob".
        follow: If True, tail for new events.
        stream: Optional brooklet Stream to use. Created automatically if not provided.

    Raises:
        ValueError: If mode is not "single-file" or "glob".
        FileNotFoundError: If path does not exist in single-file mode.
    """
    valid_modes = {"single-file", "glob"}
    if mode not in valid_modes:
        msg = f"mode must be one of {valid_modes}, got {mode!r}"
        raise ValueError(msg)

    if mode == "single-file" and not Path(path).exists():
        msg = f"Report log not found: {path}"
        raise FileNotFoundError(msg)

    if stream is None:
        parent_dir = str(Path(path).parent)
        stream = brooklet.open(parent_dir)

    if mode == "single-file":
        topic = _topic_for_path(path)
        stream.register(topic, path, mode)
        if follow:
            # Follow mode: iterate incrementally, yield stats on each batch
            with stream.consume(topic, group="pytest-analytics", follow=True) as consumer:
                run_id = _run_id_from_path(path)
                events: list[dict] = []
                for event in consumer:
                    events.append(event)
                    # Yield updated stats after each event
                    yield aggregate_run(run_id, events)
        else:
            events = list(stream.consume(topic, group="pytest-analytics"))
            if events:
                run_id = _run_id_from_path(path)
                yield aggregate_run(run_id, events)
    elif mode == "glob":
        # Each file is a separate run. Register and consume each file individually
        # so per-run stats stay independent.
        filepaths = sorted(glob_module.glob(path))
        for filepath in filepaths:
            run_id = _run_id_from_path(filepath)
            per_file_topic = _topic_for_path(filepath)
            stream.register(per_file_topic, filepath, "single-file")
            events = list(
                stream.consume(per_file_topic, group="pytest-analytics")
            )
            if events:
                yield aggregate_run(run_id, events)


# ---------------------------------------------------------------------------
# Layer 3: Output renderers and CLI
# ---------------------------------------------------------------------------


def _format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds / 60:.1f}m"


def render_run_block(stats: RunStats) -> str:
    """Render a single run's stats as plain text."""
    lines = []
    lines.append(f"--- run {stats.run_id} ---")
    lines.append(
        f"  {stats.total} tests: "
        f"{stats.passed} passed, {stats.failed} failed, "
        f"{stats.skipped} skipped, {stats.errored} errored"
    )
    lines.append(f"  duration: {_format_duration(stats.duration_s)}")

    if stats.slowest:
        lines.append("  slowest:")
        for entry in stats.slowest[:5]:
            lines.append(f"    {_format_duration(entry['duration'])}  {entry['nodeid']}")

    if stats.failures:
        lines.append("  failures:")
        for entry in stats.failures:
            short = (entry["longrepr"] or "").split("\n")[0][:120]
            lines.append(f"    FAIL {entry['nodeid']}")
            if short:
                lines.append(f"         {short}")

    return "\n".join(lines)


def render_cumulative(runs: list[RunStats]) -> str:
    """Render aggregate totals across all runs."""
    if not runs:
        return "No runs processed."

    total = sum(r.total for r in runs)
    passed = sum(r.passed for r in runs)
    failed = sum(r.failed for r in runs)
    skipped = sum(r.skipped for r in runs)
    errored = sum(r.errored for r in runs)
    duration = sum(r.duration_s for r in runs)

    lines = []
    lines.append(f"\n=== {len(runs)} runs totals ===")
    lines.append(
        f"  {total} tests: "
        f"{passed} passed, {failed} failed, "
        f"{skipped} skipped, {errored} errored"
    )
    lines.append(f"  duration: {_format_duration(duration)}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for brooklet-pytest."""
    parser = argparse.ArgumentParser(
        prog="brooklet-pytest",
        description="Consume pytest-reportlog JSONL and report test analytics.",
    )
    parser.add_argument(
        "path",
        help="Path to report log file or glob pattern",
    )
    parser.add_argument(
        "--glob",
        action="store_true",
        help="Treat path as a glob pattern (each file is a separate run)",
    )
    parser.add_argument(
        "--follow",
        action="store_true",
        help="Tail for new test events",
    )
    parser.add_argument(
        "--output",
        metavar="TOPIC",
        help="Produce run stats as JSONL events to a brooklet topic",
    )

    args = parser.parse_args(argv)
    mode = "glob" if args.glob else "single-file"

    if not args.glob and not Path(args.path).exists():
        parser.error(f"File not found: {args.path}")

    try:
        parent_dir = str(Path(args.path).resolve().parent)
        stream = brooklet.open(parent_dir)

        stats_iter = scan_runs(path=args.path, mode=mode, follow=args.follow, stream=stream)

        if args.output:
            original_iter = stats_iter

            def producing_iter():
                for stats in original_iter:
                    try:
                        stream.produce(args.output, stats.to_dict(), source="pytest-analytics")
                    except (OSError, ValueError, TypeError) as e:
                        print(
                            f"Warning: failed to produce run {stats.run_id} "
                            f"to topic {args.output!r}: {e}",
                            file=sys.stderr,
                        )
                    yield stats

            stats_iter = producing_iter()

        runs: list[RunStats] = []
        for stats in stats_iter:
            runs.append(stats)
            print(render_run_block(stats))
        print(render_cumulative(runs))
    except KeyboardInterrupt:
        print(file=sys.stderr)
        if runs:
            print(render_cumulative(runs))
    except BrokenPipeError:
        pass
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
