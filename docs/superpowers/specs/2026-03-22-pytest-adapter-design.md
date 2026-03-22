# Brooklet pytest Adapter — Design Spec

**Date**: 2026-03-22
**Scope**: AC-1 through AC-6 (registration, consumption, summary stats)
**Beads**: brooklet-d23 (Part 1), brooklet-cnu (AC-6)
**Status**: Approved

## Overview

A contrib module that consumes pytest-reportlog JSONL output through brooklet's existing `register()` / `consume()` API, then provides analytics on test runs. This is brooklet's second adapter (after Claude Code scout), validating the three-layer architecture with a non-Claude-Code source.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Package structure | Option B: `contrib/pytest_analytics.py` | Mirrors scout pattern; pluggy system not built yet |
| Test fixtures | Hardcoded JSONL | Zero dependency on pytest-reportlog; full control over edge cases |
| Module structure | Single file | Scout proves pattern works at this scale; extract when dashboard lands |
| Parametrized tests | Treat separately by nodeid | Different nodeids, different behaviors; grouping is v2 |
| Format handling | Parse defensively | Check `$report_type` existence, default missing fields to None/0 |
| Flaky detection heuristic | Simple flake rate (future) | `outcomes_differing_from_majority / total_runs` |

## pytest-reportlog Event Schema

Each JSONL line contains:

```json
{
  "$report_type": "TestReport|SessionStart|CollectReport|SessionFinish",
  "nodeid": "tests/test_core.py::test_produce",
  "outcome": "passed|failed|skipped|error",
  "when": "setup|call|teardown",
  "duration": 0.0032,
  "longrepr": "...",
  "keywords": {},
  "sections": []
}
```

Key filter for test results: `$report_type == "TestReport"` and `when == "call"`.

## Architecture: 3-Layer Pattern

Follows the same structure as `claude_analytics.py`:

### Layer 1: Parsing (pure functions, no I/O)

```python
def parse_test_event(event: dict) -> dict | None
```
- Extracts: `report_type`, `nodeid`, `outcome`, `when`, `duration`, `longrepr`
- Returns None for unrecognized events
- Defensive: missing fields default to None/0

```python
def is_test_result(event: dict) -> bool
```
- True when `report_type == "TestReport"` and `when == "call"`
- Used by consumers to filter lifecycle noise (AC-3)

```python
@dataclass
class RunStats:
    run_id: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errored: int = 0
    duration_s: float = 0.0
    slowest: list[dict] = field(default_factory=list)   # [{"nodeid": str, "duration": float}], sorted desc, max 5
    failures: list[dict] = field(default_factory=list)   # [{"nodeid": str, "longrepr": str}]
```

```python
def aggregate_run(run_id: str, events: list[dict]) -> RunStats
```
- `run_id`: filename stem of the source JSONL file (mirrors scout's `_session_id_from_path` pattern)
- Filters to test results via `is_test_result()`
- Counts outcomes, sums duration
- Tracks top 5 slowest tests as `[{"nodeid": str, "duration": float}]`, sorted descending
- Collects failure details as `[{"nodeid": str, "longrepr": str}]`

### Layer 2: Consumer Integration (uses brooklet API)

```python
def scan_runs(
    path: str,
    mode: str = "single-file",
    follow: bool = False,
) -> Iterator[RunStats]
```
- Opens a brooklet stream, registers the pytest source
- Consumes events, groups by run boundaries:
  - **single-file mode**: one file = one run (entire file contents are a single run)
  - **glob mode**: each file is a separate run; uses `_src` envelope field to group events by source file (lexicographic order)
- `run_id` derived from filename stem (e.g., `run-001.jsonl` → `run-001`)
- Yields `RunStats` per completed run

### Layer 3: Output / CLI

```python
def render_run_block(stats: RunStats) -> str
```
- Plain text summary of a single run

```python
def render_cumulative(runs: list[RunStats]) -> str
```
- Aggregate totals across all processed runs

```python
def main(argv=None) -> None
```
- CLI entry point: `brooklet-pytest`
- Args: `path` (report log path or glob), `--glob`, `--follow`, `--output TOPIC`
- `--output` produces RunStats as events to a derived brooklet topic via `stream.produce()`

## Acceptance Criteria Coverage

### AC-1: Register a single pytest report log
- `stream.register("pytest/results", path="...", mode="single-file")`
- Topic appears in `stream.topics()`
- Consuming yields test events with envelope fields

### AC-2: Register a directory of report logs (multi-run)
- `stream.register("pytest/history", path="reports/run-*.jsonl", mode="glob")`
- Events from all files yielded in lexicographic order

### AC-3: Filter to test outcomes only
- `is_test_result()` filters to `TestReport` + `when == "call"`
- Only actual test execution results yielded

### AC-4: Incremental consumption across runs
- Consumer reads run-001, run-002 → offsets saved
- New run-003 appears → only new events yielded
- Exercises brooklet's glob offset tracking

### AC-5: Follow mode tails active test run
- `follow=True` on consumer
- New lines appended → yielded in real-time
- Exercises brooklet's watchdog-based follow

### AC-6: Summary stats per run
- `aggregate_run()` produces RunStats with:
  - total, passed, failed, skipped, errored
  - total duration
  - slowest 5 tests (nodeid + duration)
  - failure details (nodeid + longrepr)
- `--output` flag produces summary event to derived topic

## Test Strategy

### Fixtures
Hardcoded JSONL strings representing realistic pytest-reportlog output:
- `FIXTURE_SINGLE_RUN`: SessionStart + CollectReport + 5 TestReports (3 pass, 1 fail, 1 skip) + SessionFinish
- `FIXTURE_MULTI_RUN`: 3 separate run files with varying outcomes
- `FIXTURE_EMPTY`: SessionStart + SessionFinish with no tests
- `FIXTURE_ALL_PASS`: Clean run with no failures

### Unit Tests (`tests/test_pytest_analytics.py`)
- `parse_test_event`: recognized types, unrecognized types, missing fields
- `is_test_result`: TestReport+call=True, TestReport+setup=False, SessionStart=False
- `aggregate_run`: counts, duration, slowest 5, failures, empty run
- `RunStats.to_dict`: serialization roundtrip

### BDD Features (`tests/bdd/features/pytest_adapter.feature`)
- AC-1 through AC-6 as scenarios
- Each scenario creates temp files, registers, consumes, asserts

### Integration Tests
- Register fixture file → consume → verify envelope fields present
- Produce summary → consume derived topic → verify stats

## Files

| Action | Path | Purpose |
|--------|------|---------|
| Create | `src/brooklet/contrib/pytest_analytics.py` | Adapter module |
| Create | `tests/test_pytest_analytics.py` | Unit tests |
| Create | `tests/bdd/features/pytest_adapter.feature` | BDD acceptance tests |
| Create | `tests/bdd/steps/test_pytest_adapter.py` | BDD step definitions |
| Modify | `pyproject.toml` | Script entry + test markers |

## Out of Scope (tracked in beads)

- Flaky test detection (brooklet-cs0, AC-7)
- Duration trend detection (brooklet-cfq, AC-8)
- Cross-project aggregation (brooklet-0gb, AC-9)
- Live Rich dashboard (brooklet-yh5, AC-10/11)
- pluggy hookspecs / plugin system
- pytest plugin for auto-writing report logs
