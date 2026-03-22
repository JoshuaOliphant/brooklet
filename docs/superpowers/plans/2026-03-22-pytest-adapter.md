# pytest Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `contrib/pytest_analytics.py` — a brooklet adapter that consumes pytest-reportlog JSONL and produces per-run summary stats.

**Architecture:** 3-layer pattern mirroring `claude_analytics.py`: Layer 1 (pure parsing), Layer 2 (brooklet consumer integration), Layer 3 (CLI output). Single contrib file.

**Tech Stack:** Python 3.12+, brooklet core APIs (`register`, `consume`, `produce`), pytest-bdd for acceptance tests, dataclasses for stats.

**Spec:** `docs/superpowers/specs/2026-03-22-pytest-adapter-design.md`

**Beads:** brooklet-d23 (Part 1: AC-1–5), brooklet-cnu (AC-6)

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `src/brooklet/contrib/pytest_analytics.py` | Adapter module (all 3 layers) |
| Create | `tests/pytest_fixtures.py` | Hardcoded JSONL fixture data + helpers |
| Create | `tests/test_pytest_analytics.py` | Unit tests for parsing + aggregation |
| Create | `tests/bdd/features/pytest_adapter.feature` | BDD acceptance tests (AC-1–6) |
| Create | `tests/bdd/steps/test_pytest_adapter.py` | BDD step definitions |
| Modify | `pyproject.toml` | Script entry + test markers |

---

### Task 1: Test Fixtures Module

**Files:**
- Create: `tests/pytest_fixtures.py`

These fixtures are used by every subsequent task. Create them first.

- [ ] **Step 1: Create `tests/pytest_fixtures.py` with hardcoded JSONL data**

```python
# ABOUTME: Hardcoded pytest-reportlog JSONL fixtures for testing
# ABOUTME: Provides realistic test data without depending on pytest-reportlog

import json
from pathlib import Path


def _line(obj: dict) -> str:
    return json.dumps(obj) + "\n"


# --- Single run: 5 tests (3 pass, 1 fail, 1 skip) ---

SINGLE_RUN_EVENTS = [
    {"$report_type": "SessionStart", "exitstatus": None},
    {"$report_type": "CollectReport", "nodeid": "", "outcome": "passed", "result": []},
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_add",
        "outcome": "passed",
        "when": "setup",
        "duration": 0.0001,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_add",
        "outcome": "passed",
        "when": "call",
        "duration": 0.0032,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_add",
        "outcome": "passed",
        "when": "teardown",
        "duration": 0.0001,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_subtract",
        "outcome": "passed",
        "when": "call",
        "duration": 0.0150,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_multiply",
        "outcome": "passed",
        "when": "call",
        "duration": 0.0510,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_divide",
        "outcome": "failed",
        "when": "call",
        "duration": 0.0024,
        "longrepr": "AssertionError: assert 1 / 0 == 0\nZeroDivisionError",
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_power",
        "outcome": "skipped",
        "when": "call",
        "duration": 0.0001,
    },
    {"$report_type": "SessionFinish", "exitstatus": 1},
]

# --- All-pass run: 3 tests ---

ALL_PASS_EVENTS = [
    {"$report_type": "SessionStart", "exitstatus": None},
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_utils.py::test_strip",
        "outcome": "passed",
        "when": "call",
        "duration": 0.001,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_utils.py::test_join",
        "outcome": "passed",
        "when": "call",
        "duration": 0.002,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_utils.py::test_split",
        "outcome": "passed",
        "when": "call",
        "duration": 0.003,
    },
    {"$report_type": "SessionFinish", "exitstatus": 0},
]

# --- Empty run: session lifecycle only ---

EMPTY_RUN_EVENTS = [
    {"$report_type": "SessionStart", "exitstatus": None},
    {"$report_type": "SessionFinish", "exitstatus": 5},
]

# --- Multi-run: 3 separate runs with varying outcomes (for glob mode) ---

MULTI_RUN_EVENTS = {
    "run-001": [
        {"$report_type": "SessionStart", "exitstatus": None},
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_a",
            "outcome": "passed",
            "when": "call",
            "duration": 0.010,
        },
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_b",
            "outcome": "failed",
            "when": "call",
            "duration": 0.020,
            "longrepr": "AssertionError: expected True",
        },
        {"$report_type": "SessionFinish", "exitstatus": 1},
    ],
    "run-002": [
        {"$report_type": "SessionStart", "exitstatus": None},
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_a",
            "outcome": "passed",
            "when": "call",
            "duration": 0.012,
        },
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_b",
            "outcome": "passed",
            "when": "call",
            "duration": 0.018,
        },
        {"$report_type": "SessionFinish", "exitstatus": 0},
    ],
    "run-003": [
        {"$report_type": "SessionStart", "exitstatus": None},
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_a",
            "outcome": "passed",
            "when": "call",
            "duration": 0.011,
        },
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_b",
            "outcome": "skipped",
            "when": "call",
            "duration": 0.001,
        },
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_c",
            "outcome": "passed",
            "when": "call",
            "duration": 0.005,
        },
        {"$report_type": "SessionFinish", "exitstatus": 0},
    ],
}


def write_run_file(directory: Path, name: str, events: list[dict]) -> Path:
    """Write a list of events to a JSONL file in the given directory.

    Args:
        directory: Parent directory to write the file into.
        name: Filename without extension (e.g. "run-001").
        events: List of event dicts to serialize.

    Returns:
        Path to the written file.
    """
    path = directory / f"{name}.jsonl"
    with open(path, "w") as f:
        for event in events:
            f.write(_line(event))
    return path
```

- [ ] **Step 2: Verify fixtures are importable**

Run: `uv run python -c "from tests.pytest_fixtures import SINGLE_RUN_EVENTS; print(len(SINGLE_RUN_EVENTS))"`
Expected: `10`

- [ ] **Step 3: Commit**

```bash
git add tests/pytest_fixtures.py
git commit -m "test: add hardcoded pytest-reportlog JSONL fixtures"
```

---

### Task 2: Layer 1 — Parsing Functions (TDD)

**Files:**
- Create: `src/brooklet/contrib/pytest_analytics.py` (initial — Layer 1 only)
- Create: `tests/test_pytest_analytics.py`

- [ ] **Step 1: Write failing tests for `parse_test_event`**

Create `tests/test_pytest_analytics.py`:

```python
# ABOUTME: Unit tests for pytest analytics parsing and aggregation
# ABOUTME: Tests Layer 1 (pure functions) and Layer 2 (consumer integration)

from brooklet.contrib.pytest_analytics import parse_test_event


class TestParseTestEvent:
    """Test parse_test_event — extracts fields from pytest-reportlog events."""

    def test_parse_test_report(self):
        event = {
            "$report_type": "TestReport",
            "nodeid": "tests/test_math.py::test_add",
            "outcome": "passed",
            "when": "call",
            "duration": 0.0032,
        }
        result = parse_test_event(event)
        assert result is not None
        assert result["report_type"] == "TestReport"
        assert result["nodeid"] == "tests/test_math.py::test_add"
        assert result["outcome"] == "passed"
        assert result["when"] == "call"
        assert result["duration"] == 0.0032

    def test_parse_session_start(self):
        event = {"$report_type": "SessionStart", "exitstatus": None}
        result = parse_test_event(event)
        assert result is not None
        assert result["report_type"] == "SessionStart"

    def test_parse_session_finish(self):
        event = {"$report_type": "SessionFinish", "exitstatus": 0}
        result = parse_test_event(event)
        assert result is not None
        assert result["report_type"] == "SessionFinish"

    def test_returns_none_for_unrecognized_type(self):
        event = {"$report_type": "UnknownThing", "data": "whatever"}
        result = parse_test_event(event)
        assert result is None

    def test_returns_none_for_missing_report_type(self):
        event = {"nodeid": "foo", "outcome": "passed"}
        result = parse_test_event(event)
        assert result is None

    def test_missing_fields_default_to_none(self):
        event = {"$report_type": "TestReport"}
        result = parse_test_event(event)
        assert result is not None
        assert result["nodeid"] is None
        assert result["outcome"] is None
        assert result["when"] is None
        assert result["duration"] == 0.0
        assert result["longrepr"] is None

    def test_parse_failed_test_with_longrepr(self):
        event = {
            "$report_type": "TestReport",
            "nodeid": "tests/test_math.py::test_divide",
            "outcome": "failed",
            "when": "call",
            "duration": 0.002,
            "longrepr": "ZeroDivisionError: division by zero",
        }
        result = parse_test_event(event)
        assert result["longrepr"] == "ZeroDivisionError: division by zero"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pytest_analytics.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'brooklet.contrib.pytest_analytics'`

- [ ] **Step 3: Write `parse_test_event` implementation**

Create `src/brooklet/contrib/pytest_analytics.py`:

```python
# ABOUTME: pytest-reportlog adapter — consumes pytest JSONL and produces run stats
# ABOUTME: Exercises brooklet register/consume/produce with a non-Claude-Code source

from __future__ import annotations

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pytest_analytics.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Write failing tests for `is_test_result`**

Append to `tests/test_pytest_analytics.py`:

```python
from brooklet.contrib.pytest_analytics import is_test_result


class TestIsTestResult:
    """Test is_test_result — filters to actual test execution results."""

    def test_test_report_call_is_true(self):
        parsed = {"report_type": "TestReport", "when": "call"}
        assert is_test_result(parsed) is True

    def test_test_report_setup_is_false(self):
        parsed = {"report_type": "TestReport", "when": "setup"}
        assert is_test_result(parsed) is False

    def test_test_report_teardown_is_false(self):
        parsed = {"report_type": "TestReport", "when": "teardown"}
        assert is_test_result(parsed) is False

    def test_session_start_is_false(self):
        parsed = {"report_type": "SessionStart", "when": None}
        assert is_test_result(parsed) is False

    def test_collect_report_is_false(self):
        parsed = {"report_type": "CollectReport", "when": None}
        assert is_test_result(parsed) is False
```

- [ ] **Step 6: Run tests, verify `is_test_result` tests fail**

Run: `uv run pytest tests/test_pytest_analytics.py::TestIsTestResult -v`
Expected: FAIL — `ImportError: cannot import name 'is_test_result'`

- [ ] **Step 7: Implement `is_test_result`**

Add to `src/brooklet/contrib/pytest_analytics.py` after `parse_test_event`:

```python
def is_test_result(parsed: dict) -> bool:
    """Return True if the parsed event is an actual test execution result.

    Filters to TestReport events in the "call" phase, excluding
    setup/teardown lifecycle events and session-level reports.
    """
    return parsed.get("report_type") == "TestReport" and parsed.get("when") == "call"
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/test_pytest_analytics.py -v`
Expected: All 12 tests PASS

- [ ] **Step 9: Commit**

```bash
git add src/brooklet/contrib/pytest_analytics.py tests/test_pytest_analytics.py
git commit -m "feat: add pytest-reportlog parsing layer (parse_test_event, is_test_result)"
```

---

### Task 3: Layer 1 — RunStats and Aggregation (TDD)

**Files:**
- Modify: `src/brooklet/contrib/pytest_analytics.py`
- Modify: `tests/test_pytest_analytics.py`

- [ ] **Step 1: Write failing tests for `RunStats` and `aggregate_run`**

Append to `tests/test_pytest_analytics.py`:

```python
from brooklet.contrib.pytest_analytics import RunStats, aggregate_run
from tests.pytest_fixtures import (
    ALL_PASS_EVENTS,
    EMPTY_RUN_EVENTS,
    SINGLE_RUN_EVENTS,
)


class TestAggregateRun:
    """Test aggregate_run — aggregates parsed events into RunStats."""

    def test_single_run_counts(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        assert stats.total == 5
        assert stats.passed == 3
        assert stats.failed == 1
        assert stats.skipped == 1
        assert stats.errored == 0

    def test_single_run_duration(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        # Sum of call-phase durations: 0.0032 + 0.0150 + 0.0510 + 0.0024 + 0.0001
        assert abs(stats.duration_s - 0.0717) < 0.0001

    def test_single_run_slowest(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        assert len(stats.slowest) == 5
        assert stats.slowest[0]["nodeid"] == "tests/test_math.py::test_multiply"
        assert stats.slowest[0]["duration"] == 0.0510

    def test_single_run_failures(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        assert len(stats.failures) == 1
        assert stats.failures[0]["nodeid"] == "tests/test_math.py::test_divide"
        assert "ZeroDivisionError" in stats.failures[0]["longrepr"]

    def test_all_pass_run(self):
        stats = aggregate_run("clean", ALL_PASS_EVENTS)
        assert stats.total == 3
        assert stats.passed == 3
        assert stats.failed == 0
        assert stats.failures == []

    def test_empty_run(self):
        stats = aggregate_run("empty", EMPTY_RUN_EVENTS)
        assert stats.total == 0
        assert stats.passed == 0
        assert stats.duration_s == 0.0
        assert stats.slowest == []
        assert stats.failures == []

    def test_run_id_is_set(self):
        stats = aggregate_run("my-run-id", SINGLE_RUN_EVENTS)
        assert stats.run_id == "my-run-id"


class TestRunStatsToDict:
    """Test RunStats.to_dict — serialization for JSONL output."""

    def test_to_dict_roundtrip(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        d = stats.to_dict()
        assert d["run_id"] == "test-run"
        assert d["total"] == 5
        assert d["passed"] == 3
        assert d["failed"] == 1
        assert d["skipped"] == 1
        assert isinstance(d["slowest"], list)
        assert isinstance(d["failures"], list)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pytest_analytics.py::TestAggregateRun -v`
Expected: FAIL — `ImportError: cannot import name 'RunStats'`

- [ ] **Step 3: Implement `RunStats` and `aggregate_run`**

Add to `src/brooklet/contrib/pytest_analytics.py`:

```python
from dataclasses import dataclass, field


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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pytest_analytics.py -v`
Expected: All 21 tests PASS

- [ ] **Step 5: Run linter**

Run: `uv run ruff check src/brooklet/contrib/pytest_analytics.py tests/test_pytest_analytics.py tests/pytest_fixtures.py`
Expected: Clean

- [ ] **Step 6: Commit**

```bash
git add src/brooklet/contrib/pytest_analytics.py tests/test_pytest_analytics.py
git commit -m "feat: add RunStats dataclass and aggregate_run for pytest analytics"
```

---

### Task 4: Layer 2 — Consumer Integration: scan_runs (TDD)

**Files:**
- Modify: `src/brooklet/contrib/pytest_analytics.py`
- Modify: `tests/test_pytest_analytics.py`

- [ ] **Step 1: Write failing tests for `scan_runs` single-file mode**

Append to `tests/test_pytest_analytics.py`:

```python
from brooklet.contrib.pytest_analytics import scan_runs
from tests.pytest_fixtures import SINGLE_RUN_EVENTS, write_run_file


class TestScanRunsSingleFile:
    """Test scan_runs in single-file mode."""

    def test_single_file_yields_one_run(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "results", SINGLE_RUN_EVENTS)

        runs = list(scan_runs(str(reports_dir / "results.jsonl"), mode="single-file"))
        assert len(runs) == 1
        assert runs[0].run_id == "results"
        assert runs[0].total == 5
        assert runs[0].passed == 3

    def test_single_file_run_id_from_filename(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "my-test-run", ALL_PASS_EVENTS)

        runs = list(scan_runs(str(reports_dir / "my-test-run.jsonl"), mode="single-file"))
        assert runs[0].run_id == "my-test-run"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pytest_analytics.py::TestScanRunsSingleFile -v`
Expected: FAIL — `ImportError: cannot import name 'scan_runs'`

- [ ] **Step 3: Implement `scan_runs` for single-file mode**

Add to `src/brooklet/contrib/pytest_analytics.py`:

```python
from collections.abc import Iterator
from pathlib import Path

import brooklet


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
        # Each file is a separate run. Group events by _src envelope field.
        runs: dict[str, list[dict]] = {}
        for event in stream.consume(topic, group="pytest-analytics", follow=follow):
            src = event.get("_src", "")
            run_id = _run_id_from_path(src) if src else "unknown"
            runs.setdefault(run_id, []).append(event)

        for run_id in sorted(runs.keys()):
            yield aggregate_run(run_id, runs[run_id])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pytest_analytics.py::TestScanRunsSingleFile -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Write failing tests for `scan_runs` glob mode**

Append to `tests/test_pytest_analytics.py`:

```python
from tests.pytest_fixtures import MULTI_RUN_EVENTS


class TestScanRunsGlob:
    """Test scan_runs in glob mode — each file is a separate run."""

    def test_glob_yields_one_run_per_file(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        for name, events in MULTI_RUN_EVENTS.items():
            write_run_file(reports_dir, name, events)

        runs = list(scan_runs(str(reports_dir / "run-*.jsonl"), mode="glob"))
        assert len(runs) == 3

    def test_glob_runs_in_lexicographic_order(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        for name, events in MULTI_RUN_EVENTS.items():
            write_run_file(reports_dir, name, events)

        runs = list(scan_runs(str(reports_dir / "run-*.jsonl"), mode="glob"))
        assert [r.run_id for r in runs] == ["run-001", "run-002", "run-003"]

    def test_glob_run_stats_are_independent(self, tmp_path):
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        for name, events in MULTI_RUN_EVENTS.items():
            write_run_file(reports_dir, name, events)

        runs = list(scan_runs(str(reports_dir / "run-*.jsonl"), mode="glob"))
        # run-001: 1 pass, 1 fail
        assert runs[0].passed == 1
        assert runs[0].failed == 1
        # run-002: 2 pass
        assert runs[1].passed == 2
        assert runs[1].failed == 0
        # run-003: 2 pass, 1 skip
        assert runs[2].passed == 2
        assert runs[2].skipped == 1
```

- [ ] **Step 6: Run tests to verify glob tests pass**

Run: `uv run pytest tests/test_pytest_analytics.py::TestScanRunsGlob -v`
Expected: All 3 tests PASS (implementation from step 3 handles glob mode)

- [ ] **Step 7: Commit**

```bash
git add src/brooklet/contrib/pytest_analytics.py tests/test_pytest_analytics.py
git commit -m "feat: add scan_runs consumer integration for pytest analytics"
```

---

### Task 5: Layer 3 — Output Rendering and CLI (TDD)

**Files:**
- Modify: `src/brooklet/contrib/pytest_analytics.py`
- Modify: `tests/test_pytest_analytics.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Write failing tests for `render_run_block`**

Append to `tests/test_pytest_analytics.py`:

```python
from brooklet.contrib.pytest_analytics import render_run_block


class TestRenderRunBlock:
    """Test render_run_block — plain text summary of a single run."""

    def test_render_includes_run_id(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        output = render_run_block(stats)
        assert "test-run" in output

    def test_render_includes_counts(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        output = render_run_block(stats)
        assert "5 tests" in output
        assert "3 passed" in output
        assert "1 failed" in output
        assert "1 skipped" in output

    def test_render_includes_failures(self):
        stats = aggregate_run("test-run", SINGLE_RUN_EVENTS)
        output = render_run_block(stats)
        assert "test_divide" in output

    def test_render_all_pass_no_failures_section(self):
        stats = aggregate_run("clean", ALL_PASS_EVENTS)
        output = render_run_block(stats)
        assert "3 passed" in output
        assert "FAIL" not in output
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pytest_analytics.py::TestRenderRunBlock -v`
Expected: FAIL — `ImportError: cannot import name 'render_run_block'`

- [ ] **Step 3: Implement rendering and CLI**

Add to `src/brooklet/contrib/pytest_analytics.py`:

```python
import argparse
import sys


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
            # First line of longrepr only
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

    stats_iter = scan_runs(path=args.path, mode=mode, follow=args.follow)

    # If --output, wrap iterator to produce events to derived topic
    if args.output:
        parent_dir = str(Path(args.path).parent)
        stream = brooklet.open(parent_dir)
        original_iter = stats_iter

        def producing_iter():
            for stats in original_iter:
                stream.produce(args.output, stats.to_dict(), source="pytest-analytics")
                yield stats

        stats_iter = producing_iter()

    runs = []
    try:
        for stats in stats_iter:
            runs.append(stats)
            print(render_run_block(stats))
        print(render_cumulative(runs))
    except KeyboardInterrupt:
        pass
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pytest_analytics.py -v`
Expected: All tests PASS

- [ ] **Step 5: Add CLI entry point and test markers to `pyproject.toml`**

Add to `[project.scripts]` section:
```toml
brooklet-pytest = "brooklet.contrib.pytest_analytics:main"
```

Add to `[tool.pytest.ini_options]` markers list:
```toml
    "ac-pt-1: AC-PT-1 Register a single pytest report log",
    "ac-pt-2: AC-PT-2 Register a directory of report logs",
    "ac-pt-3: AC-PT-3 Filter to test outcomes only",
    "ac-pt-4: AC-PT-4 Incremental consumption across runs",
    "ac-pt-5: AC-PT-5 Follow mode tails active test run",
    "ac-pt-6: AC-PT-6 Summary stats per run",
```

- [ ] **Step 6: Run linter on all files**

Run: `uv run ruff check src/brooklet/contrib/pytest_analytics.py tests/test_pytest_analytics.py tests/pytest_fixtures.py`
Expected: Clean

- [ ] **Step 7: Commit**

```bash
git add src/brooklet/contrib/pytest_analytics.py tests/test_pytest_analytics.py pyproject.toml
git commit -m "feat: add render_run_block, CLI entry point, and brooklet-pytest script"
```

---

### Task 6: BDD Acceptance Tests (AC-1 through AC-6)

**Files:**
- Create: `tests/bdd/features/pytest_adapter.feature`
- Create: `tests/bdd/steps/test_pytest_adapter.py`

- [ ] **Step 1: Create BDD feature file**

Create `tests/bdd/features/pytest_adapter.feature`:

```gherkin
# ABOUTME: BDD feature file for pytest adapter — registration, consumption, stats
# ABOUTME: Generated from acceptance criteria AC-PT-1 through AC-PT-6

Feature: pytest report log adapter
    Consume pytest-reportlog JSONL through brooklet's API,
    filter to test results, track offsets, and produce run summaries.

    Background:
        Given a stream opened at an empty directory

    @ac-pt-1
    Scenario: Register a single pytest report log
        Given a pytest report log at "results.jsonl" with 5 test events
        When the user registers it as "pytest/results" in single-file mode
        Then "pytest/results" appears in the stream topics list
        And consuming "pytest/results" with group "test" yields test events with envelope fields

    @ac-pt-2
    Scenario: Register a directory of report logs
        Given 3 pytest report log files matching "run-*.jsonl"
        When the user registers them as "pytest/history" in glob mode
        Then consuming "pytest/history" with group "test" yields events from all 3 files

    @ac-pt-3
    Scenario: Filter to test outcomes only
        Given a pytest report log containing SessionStart, CollectReport, TestReport, and SessionFinish events
        When the consumer filters with is_test_result
        Then only TestReport events with when "call" are yielded

    @ac-pt-4
    Scenario: Incremental consumption across runs
        Given 2 pytest report log files "run-001.jsonl" and "run-002.jsonl" registered as glob
        And a consumer has read all events from both files
        When a new "run-003.jsonl" appears
        And the consumer reads again
        Then only events from "run-003.jsonl" are yielded

    @ac-pt-5
    Scenario: Follow mode tails active test run
        Given a pytest report log registered in single-file mode with follow enabled
        When new test events are appended to the file
        Then the consumer yields the new events

    @ac-pt-6
    Scenario: Summary stats per run
        Given a pytest report log with 3 passed, 1 failed, and 1 skipped test
        When aggregate_run processes the events
        Then the RunStats contains total 5 passed 3 failed 1 skipped 1
        And the slowest list has 5 entries sorted by duration descending
        And the failures list contains the failed test with longrepr
```

- [ ] **Step 2: Create BDD step definitions**

Create `tests/bdd/steps/test_pytest_adapter.py`:

```python
# ABOUTME: Step definitions for pytest adapter BDD tests
# ABOUTME: Covers AC-PT-1 through AC-PT-6

import json
from pathlib import Path

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

import brooklet
from brooklet.contrib.pytest_analytics import (
    aggregate_run,
    is_test_result,
    parse_test_event,
)
from tests.pytest_fixtures import (
    MULTI_RUN_EVENTS,
    SINGLE_RUN_EVENTS,
    write_run_file,
)

scenarios("../features/pytest_adapter.feature")


# ---------------------------------------------------------------------------
# Shared state container
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx():
    """Mutable context dict for sharing state between steps."""
    return {
        "consumed_events": [],
        "filtered_events": [],
        "run_stats": None,
        "raw_events": None,
        "live_path": None,
    }


# ---------------------------------------------------------------------------
# Background — reuse stream fixture from bdd/conftest.py
# ---------------------------------------------------------------------------


@given("a stream opened at an empty directory", target_fixture="stream")
def given_stream_at_empty_dir(stream):
    """Reuse the stream fixture from conftest."""
    return stream


# ---------------------------------------------------------------------------
# AC-PT-1: Register a single pytest report log
# ---------------------------------------------------------------------------


@given(
    parsers.parse('a pytest report log at "{filename}" with {count:d} test events'),
    target_fixture="report_file",
)
def given_report_log(stream_dir, filename, count):
    """Create a pytest report log file in the stream directory."""
    return write_run_file(stream_dir, Path(filename).stem, SINGLE_RUN_EVENTS)


@when(parsers.parse('the user registers it as "{topic}" in single-file mode'))
def when_register_single_file(stream, report_file, topic):
    stream.register(topic, str(report_file), "single-file")


@then(parsers.parse('"{topic}" appears in the stream topics list'))
def then_topic_in_list(stream, topic):
    assert topic in stream.topics()


@then(parsers.parse('consuming "{topic}" with group "{group}" yields test events with envelope fields'))
def then_consume_yields_events_with_envelope(stream, topic, group):
    events = list(stream.consume(topic, group=group))
    assert len(events) > 0
    for event in events:
        assert "_ts" in event
        assert "_seq" in event


# ---------------------------------------------------------------------------
# AC-PT-2: Register a directory of report logs
# ---------------------------------------------------------------------------


@given(parsers.parse('{count:d} pytest report log files matching "{pattern}"'))
def given_multi_report_logs(stream_dir, count, pattern):
    for name, events in MULTI_RUN_EVENTS.items():
        write_run_file(stream_dir, name, events)


@when(parsers.parse('the user registers them as "{topic}" in glob mode'))
def when_register_glob(stream, stream_dir, topic):
    pattern = str(stream_dir / "run-*.jsonl")
    stream.register(topic, pattern, "glob")


@then(parsers.parse('consuming "{topic}" with group "{group}" yields events from all {count:d} files'))
def then_glob_yields_all_events(stream, topic, group, count):
    events = list(stream.consume(topic, group=group))
    sources = {e.get("_src", "") for e in events}
    assert len(sources) >= count, f"Expected events from {count}+ files, got {len(sources)}"


# ---------------------------------------------------------------------------
# AC-PT-3: Filter to test outcomes only
# ---------------------------------------------------------------------------


@given("a pytest report log containing SessionStart, CollectReport, TestReport, and SessionFinish events")
def given_mixed_events(ctx):
    ctx["raw_events"] = SINGLE_RUN_EVENTS


@when("the consumer filters with is_test_result")
def when_filter_test_results(ctx):
    filtered = []
    for raw in ctx["raw_events"]:
        parsed = parse_test_event(raw)
        if parsed is not None and is_test_result(parsed):
            filtered.append(parsed)
    ctx["filtered_events"] = filtered


@then('only TestReport events with when "call" are yielded')
def then_only_call_events(ctx):
    for event in ctx["filtered_events"]:
        assert event["report_type"] == "TestReport"
        assert event["when"] == "call"
    # SINGLE_RUN has 5 call-phase TestReports
    assert len(ctx["filtered_events"]) == 5


# ---------------------------------------------------------------------------
# AC-PT-4: Incremental consumption across runs
# ---------------------------------------------------------------------------


@given('2 pytest report log files "run-001.jsonl" and "run-002.jsonl" registered as glob')
def given_two_run_files(stream, stream_dir):
    write_run_file(stream_dir, "run-001", MULTI_RUN_EVENTS["run-001"])
    write_run_file(stream_dir, "run-002", MULTI_RUN_EVENTS["run-002"])
    pattern = str(stream_dir / "run-*.jsonl")
    stream.register("pytest/incremental", pattern, "glob")


@given("a consumer has read all events from both files")
def given_consumer_read_both(stream, ctx):
    events = list(stream.consume("pytest/incremental", group="incr-test"))
    ctx["first_read_count"] = len(events)
    assert ctx["first_read_count"] > 0


@when(parsers.parse('a new "{filename}" appears'))
def when_new_file_appears(stream_dir, filename):
    name = Path(filename).stem
    write_run_file(stream_dir, name, MULTI_RUN_EVENTS.get(name, MULTI_RUN_EVENTS["run-003"]))


@when("the consumer reads again")
def when_consumer_reads_again(stream, ctx):
    events = list(stream.consume("pytest/incremental", group="incr-test"))
    ctx["consumed_events"] = events


@then(parsers.parse('only events from "{filename}" are yielded'))
def then_only_new_events(ctx, filename):
    assert len(ctx["consumed_events"]) > 0
    expected_stem = Path(filename).stem
    for event in ctx["consumed_events"]:
        src = event.get("_src", "")
        assert expected_stem in Path(src).stem


# ---------------------------------------------------------------------------
# AC-PT-5: Follow mode (verifies appended events are consumable)
# ---------------------------------------------------------------------------


@given("a pytest report log registered in single-file mode with follow enabled")
def given_follow_report(stream, stream_dir, ctx):
    path = write_run_file(stream_dir, "live-run", SINGLE_RUN_EVENTS[:2])
    stream.register("pytest/live", str(path), "single-file")
    ctx["live_path"] = path


@when("new test events are appended to the file")
def when_new_events_appended(ctx):
    new_event = {
        "$report_type": "TestReport",
        "nodeid": "tests/test_new.py::test_appended",
        "outcome": "passed",
        "when": "call",
        "duration": 0.005,
    }
    with open(ctx["live_path"], "a") as f:
        f.write(json.dumps(new_event) + "\n")


@then("the consumer yields the new events")
def then_new_events_yielded(stream):
    events = list(stream.consume("pytest/live", group="follow-test"))
    nodeids = [e.get("nodeid") for e in events if e.get("nodeid")]
    assert "tests/test_new.py::test_appended" in nodeids


# ---------------------------------------------------------------------------
# AC-PT-6: Summary stats per run
# ---------------------------------------------------------------------------


@given(parsers.parse("a pytest report log with {p:d} passed, {f:d} failed, and {s:d} skipped test"))
def given_run_for_stats(ctx, p, f, s):
    ctx["raw_events"] = SINGLE_RUN_EVENTS


@when("aggregate_run processes the events")
def when_aggregate_run(ctx):
    ctx["run_stats"] = aggregate_run("stats-test", ctx["raw_events"])


@then(parsers.parse("the RunStats contains total {t:d} passed {p:d} failed {f:d} skipped {s:d}"))
def then_stats_match(ctx, t, p, f, s):
    stats = ctx["run_stats"]
    assert stats.total == t
    assert stats.passed == p
    assert stats.failed == f
    assert stats.skipped == s


@then(parsers.parse("the slowest list has {count:d} entries sorted by duration descending"))
def then_slowest_sorted(ctx, count):
    stats = ctx["run_stats"]
    assert len(stats.slowest) == count
    durations = [e["duration"] for e in stats.slowest]
    assert durations == sorted(durations, reverse=True)


@then("the failures list contains the failed test with longrepr")
def then_failures_have_longrepr(ctx):
    stats = ctx["run_stats"]
    assert len(stats.failures) >= 1
    assert stats.failures[0]["nodeid"] is not None
    assert len(stats.failures[0]["longrepr"]) > 0
```

- [ ] **Step 3: Run BDD tests**

Run: `uv run pytest tests/bdd/steps/test_pytest_adapter.py -v`
Expected: All 6 scenarios PASS

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS (existing + new)

- [ ] **Step 5: Run linter on all new/modified files**

Run: `uv run ruff check .`
Expected: Clean

- [ ] **Step 6: Commit**

```bash
git add tests/bdd/features/pytest_adapter.feature tests/bdd/steps/test_pytest_adapter.py
git commit -m "test: add BDD acceptance tests for pytest adapter (AC-PT-1 through AC-PT-6)"
```

---

### Task 7: Integration Test — Produce and Consume Roundtrip

**Files:**
- Modify: `tests/test_pytest_analytics.py`

- [ ] **Step 1: Write integration test**

Append to `tests/test_pytest_analytics.py`:

```python
import brooklet as bl


class TestIntegration:
    """Integration tests — full register → consume → produce roundtrip."""

    def test_register_consume_has_envelope_fields(self, tmp_path):
        """AC-1 integration: registered file yields events with _ts, _seq."""
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "integration", SINGLE_RUN_EVENTS)

        stream = bl.open(str(reports_dir))
        stream.register("pytest/int", str(reports_dir / "integration.jsonl"), "single-file")
        events = list(stream.consume("pytest/int", group="int-test"))

        assert len(events) > 0
        for event in events:
            assert "_ts" in event
            assert "_seq" in event

    def test_produce_summary_to_derived_topic(self, tmp_path):
        """AC-6 integration: produce stats to a derived topic, then consume them."""
        reports_dir = tmp_path / "reports"
        reports_dir.mkdir()
        write_run_file(reports_dir, "prod-test", SINGLE_RUN_EVENTS)

        stream = bl.open(str(reports_dir))
        runs = list(scan_runs(str(reports_dir / "prod-test.jsonl"), mode="single-file"))
        assert len(runs) == 1

        # Produce summary to derived topic
        stream.produce("pytest/summaries", runs[0].to_dict(), source="pytest-analytics")

        # Consume the derived topic
        summaries = list(stream.consume("pytest/summaries", group="verify"))
        assert len(summaries) == 1
        assert summaries[0]["total"] == 5
        assert summaries[0]["passed"] == 3
```

- [ ] **Step 2: Run integration tests**

Run: `uv run pytest tests/test_pytest_analytics.py::TestIntegration -v`
Expected: All 2 tests PASS

- [ ] **Step 3: Run full test suite one final time**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add tests/test_pytest_analytics.py
git commit -m "test: add integration tests for pytest adapter roundtrip"
```

---

### Task 8: Final Verification and Beads Cleanup

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 2: Run linter**

Run: `uv run ruff check .`
Expected: Clean

- [ ] **Step 3: Verify CLI entry point works**

Run: `uv run brooklet-pytest --help`
Expected: Shows usage with path, --glob, --follow, --output args

- [ ] **Step 4: Close beads issues**

```bash
bd close brooklet-d23 brooklet-cnu
```

- [ ] **Step 5: Push to remote**

```bash
git push
```
