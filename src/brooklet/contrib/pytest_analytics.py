# ABOUTME: pytest-reportlog adapter — consumes pytest JSONL and produces run stats
# ABOUTME: Exercises brooklet register/consume/produce with a non-Claude-Code source

from __future__ import annotations

import glob as glob_module
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

    return {
        "report_type": report_type,
        "nodeid": event.get("nodeid"),
        "outcome": event.get("outcome"),
        "when": event.get("when"),
        "duration": event.get("duration", 0.0),
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


def scan_runs(
    path: str,
    mode: str = "single-file",
    follow: bool = False,
) -> Iterator[RunStats]:
    """Scan pytest report log(s) and yield per-run statistics.

    Uses brooklet's consumer API for offset tracking and follow mode.

    Args:
        path: File path (single-file) or glob pattern (glob mode).
        mode: Either "single-file" or "glob".
        follow: If True, tail for new events.
    """
    parent_dir = str(Path(path).parent)
    stream = brooklet.open(parent_dir)
    topic = "pytest/results"

    stream.register(topic, path, mode)

    if mode == "single-file":
        # One file = one run. Collect all events, aggregate.
        events = list(stream.consume(topic, group="pytest-analytics", follow=follow))
        if events:
            run_id = _run_id_from_path(path)
            yield aggregate_run(run_id, events)
    elif mode == "glob":
        # Each file is a separate run. Register and consume each file individually
        # so per-run stats stay independent.
        filepaths = sorted(glob_module.glob(path))
        for filepath in filepaths:
            run_id = _run_id_from_path(filepath)
            per_file_topic = f"pytest/results/{run_id}"
            stream.register(per_file_topic, filepath, "single-file")
            events = list(
                stream.consume(per_file_topic, group="pytest-analytics", follow=follow)
            )
            if events:
                yield aggregate_run(run_id, events)
