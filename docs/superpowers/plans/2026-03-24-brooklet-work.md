# brooklet-work Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `brooklet-work`, a personal brooklet plugin that analyzes ServiceNow work-tracker JSONL data with `brooklet work scan`.

**Architecture:** Standalone Python package at `~/Dropbox/python_workspace/brooklet-work/` with pluggy entry point. 3-layer pattern: parsing (pure functions) → consumer integration (brooklet API) → rendering (text + rich). Discovered by brooklet via `[project.entry-points.brooklet]`.

**Tech Stack:** Python 3.12+, brooklet (dependency), typer, pluggy, rich, uv

**Spec:** `brooklet/docs/superpowers/specs/2026-03-24-brooklet-work-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `pyproject.toml` | Package config, brooklet entry point, dependencies |
| `src/brooklet_work/__init__.py` | WorkPlugin class with hookimpl |
| `src/brooklet_work/parsing.py` | Layer 1: parse_work_event(), WorkStats, aggregate_period() |
| `src/brooklet_work/consumer.py` | Layer 2: scan_work() iterator, _parse_file_events() |
| `src/brooklet_work/rendering.py` | Layer 3: text renderers + rich dashboard |
| `tests/conftest.py` | Test fixtures: sample commit/activity JSONL data |
| `tests/test_parsing.py` | Parsing and aggregation tests |
| `tests/test_consumer.py` | Consumer integration tests |
| `tests/test_rendering.py` | Renderer output tests |
| `tests/test_plugin.py` | End-to-end CLI tests via CliRunner |

---

### Task 1: Scaffold the repo

**Files:**
- Create: `~/Dropbox/python_workspace/brooklet-work/pyproject.toml`
- Create: `~/Dropbox/python_workspace/brooklet-work/src/brooklet_work/__init__.py`
- Create: `~/Dropbox/python_workspace/brooklet-work/.python-version`
- Create: `~/Dropbox/python_workspace/brooklet-work/.gitignore`

- [ ] **Step 1: Create the project with uv**

```bash
cd ~/Dropbox/python_workspace
uv init brooklet-work --lib --package
cd brooklet-work
```

- [ ] **Step 2: Set Python version**

```bash
uv python pin 3.12
```

- [ ] **Step 3: Configure pyproject.toml**

Replace the generated `pyproject.toml` with:

```toml
# ABOUTME: Build configuration for brooklet-work — personal work tracker analytics plugin
# ABOUTME: Registers as a brooklet CLI plugin via pluggy entry points

[project]
name = "brooklet-work"
version = "0.1.0"
description = "Brooklet plugin for ServiceNow work tracker analytics"
requires-python = ">=3.12"
license = {text = "MIT"}
authors = [{name = "Joshua Oliphant"}]

dependencies = [
    "brooklet>=0.1.1",
    "typer>=0.9",
    "rich>=14.0",
]

[project.entry-points.brooklet]
work = "brooklet_work:WorkPlugin"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/brooklet_work"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP", "B", "C4", "SIM"]

[tool.pytest.ini_options]
testpaths = ["tests"]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "ruff>=0.9",
]
```

- [ ] **Step 4: Create __init__.py stub**

Create `src/brooklet_work/__init__.py`:

```python
# ABOUTME: brooklet-work — personal work tracker analytics plugin for brooklet
# ABOUTME: Registers WorkPlugin via pluggy entry point for brooklet CLI discovery

__version__ = "0.1.0"


class WorkPlugin:
    """Pluggy plugin that registers work tracker CLI commands."""

    pass
```

- [ ] **Step 5: Add .gitignore**

```
__pycache__/
*.py[oc]
build/
dist/
*.egg-info
.venv
.dolt/
*.db
```

- [ ] **Step 6: Install dependencies**

```bash
uv sync
```

- [ ] **Step 7: Initialize git and commit**

```bash
git init
git add .
git commit -m "feat: scaffold brooklet-work plugin repo"
```

- [ ] **Step 8: Move spec from brooklet repo**

```bash
mkdir -p docs/superpowers/specs
cp ~/Dropbox/python_workspace/brooklet/docs/superpowers/specs/2026-03-24-brooklet-work-design.md docs/superpowers/specs/
git rm ~/Dropbox/python_workspace/brooklet/docs/superpowers/specs/2026-03-24-brooklet-work-design.md  # remove from brooklet
git -C ~/Dropbox/python_workspace/brooklet commit -m "docs: move brooklet-work spec to its own repo"
git add docs/
git commit -m "docs: add design spec from brooklet repo"
```

---

### Task 2: Test fixtures

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/work_fixtures.py`

- [ ] **Step 1: Create test fixtures with sample data**

Create `tests/__init__.py` (empty).

Create `tests/work_fixtures.py`:

```python
# ABOUTME: Hardcoded work-tracker JSONL fixtures for testing
# ABOUTME: Provides realistic commit and activity event data

import json
from pathlib import Path


def _line(obj: dict) -> str:
    return json.dumps(obj) + "\n"


COMMIT_EVENTS = [
    {
        "type": "commit",
        "timestamp": "2026-03-12T18:56:41Z",
        "repo": "mailapi",
        "branch": "main",
        "hash": "d3fa1a6",
        "message": "feat: add host_pool parameter",
        "author": "joshua.oliphant",
        "files_changed": 2,
        "insertions": 10,
        "deletions": 0,
        "commit_type": "feat",
        "scope": None,
        "category": "servicenow_core",
        "enriched": False,
    },
    {
        "type": "commit",
        "timestamp": "2026-03-12T16:46:42Z",
        "repo": "ansible-hermes",
        "branch": "test/deploy-integration",
        "hash": "3361044",
        "message": "fix: hard-exclude broker-occupied hosts",
        "author": "joshua.oliphant",
        "files_changed": 1,
        "insertions": 15,
        "deletions": 0,
        "commit_type": "fix",
        "scope": None,
        "category": "infrastructure",
        "enriched": False,
    },
    {
        "type": "commit",
        "timestamp": "2026-03-13T10:00:00Z",
        "repo": "snowk8s-mcp",
        "branch": "main",
        "hash": "abc1234",
        "message": "test: add integration tests",
        "author": "joshua.oliphant",
        "files_changed": 3,
        "insertions": 45,
        "deletions": 5,
        "commit_type": "test",
        "scope": None,
        "category": "mcp_servers",
        "enriched": False,
    },
    {
        "type": "commit",
        "timestamp": "2026-03-20T14:30:00Z",
        "repo": "mailapi",
        "branch": "feat/new-endpoint",
        "hash": "def5678",
        "message": "feat: add DKIM validation endpoint",
        "author": "joshua.oliphant",
        "files_changed": 4,
        "insertions": 80,
        "deletions": 3,
        "commit_type": "feat",
        "scope": None,
        "category": "servicenow_core",
        "enriched": True,
    },
]

ACTIVITY_EVENTS = [
    {
        "type": "activity",
        "timestamp": "2026-03-12T19:00:00Z",
        "activity_type": "other",
        "description": "Deployed test cluster on hermes20-25",
        "project": "ansible-hermes",
        "people": [],
        "category": "infrastructure",
        "enriched": False,
    },
    {
        "type": "activity",
        "timestamp": "2026-03-23T17:00:00Z",
        "activity_type": "review",
        "description": "Reviewed MR !158 from Joseph and Pratima",
        "project": "ansible-hermes",
        "people": ["joseph.struth", "pratima.shetty"],
        "category": "infrastructure",
        "enriched": True,
        "impact": "medium",
    },
]

# Events from a different month for multi-file testing
FEBRUARY_EVENTS = [
    {
        "type": "commit",
        "timestamp": "2026-02-15T09:00:00Z",
        "repo": "mailapi",
        "branch": "main",
        "hash": "feb1111",
        "message": "docs: update API reference",
        "author": "joshua.oliphant",
        "files_changed": 1,
        "insertions": 20,
        "deletions": 5,
        "commit_type": "docs",
        "scope": None,
        "category": "servicenow_core",
        "enriched": False,
    },
    {
        "type": "commit",
        "timestamp": "2026-02-28T17:00:00Z",
        "repo": "snowk8s-mcp",
        "branch": "main",
        "hash": "feb2222",
        "message": "chore: update dependencies",
        "author": "joshua.oliphant",
        "files_changed": 2,
        "insertions": 30,
        "deletions": 25,
        "commit_type": "chore",
        "scope": None,
        "category": "mcp_servers",
        "enriched": False,
    },
]

UNRECOGNIZED_EVENT = {"type": "unknown", "data": "something"}

ALL_MARCH_EVENTS = COMMIT_EVENTS + ACTIVITY_EVENTS


def write_work_file(directory: Path, name: str, events: list[dict]) -> Path:
    """Write events to a JSONL file."""
    path = directory / f"{name}.jsonl"
    with open(path, "w") as f:
        for event in events:
            f.write(_line(event))
    return path
```

Create `tests/conftest.py`:

```python
# ABOUTME: Shared test fixtures for brooklet-work tests
# ABOUTME: Provides temporary directories with sample JSONL work tracker data

import pytest

from tests.work_fixtures import (
    ALL_MARCH_EVENTS,
    FEBRUARY_EVENTS,
    write_work_file,
)


@pytest.fixture
def work_dir(tmp_path):
    """Directory with two months of sample work tracker JSONL."""
    raw = tmp_path / "raw"
    raw.mkdir()
    write_work_file(raw, "2026-02", FEBRUARY_EVENTS)
    write_work_file(raw, "2026-03", ALL_MARCH_EVENTS)
    return raw


@pytest.fixture
def single_month_dir(tmp_path):
    """Directory with one month of sample data."""
    raw = tmp_path / "raw"
    raw.mkdir()
    write_work_file(raw, "2026-03", ALL_MARCH_EVENTS)
    return raw
```

- [ ] **Step 2: Verify fixtures load**

Run: `uv run python -c "from tests.work_fixtures import ALL_MARCH_EVENTS; print(len(ALL_MARCH_EVENTS))"`
Expected: `6`

- [ ] **Step 3: Commit**

```bash
git add tests/
git commit -m "test: add work tracker JSONL fixtures"
```

---

### Task 3: Layer 1 — Parsing

**Files:**
- Create: `src/brooklet_work/parsing.py`
- Create: `tests/test_parsing.py`

- [ ] **Step 1: Write failing tests for parsing**

Create `tests/test_parsing.py`:

```python
# ABOUTME: Tests for work tracker event parsing and aggregation
# ABOUTME: Verifies parse_work_event, WorkStats, and aggregate_period

from tests.work_fixtures import (
    ACTIVITY_EVENTS,
    ALL_MARCH_EVENTS,
    COMMIT_EVENTS,
    UNRECOGNIZED_EVENT,
)

from brooklet_work.parsing import WorkStats, aggregate_period, parse_work_event, period_key


def test_parse_commit_event():
    result = parse_work_event(COMMIT_EVENTS[0])
    assert result is not None
    assert result["type"] == "commit"
    assert result["repo"] == "mailapi"
    assert result["category"] == "servicenow_core"
    assert result["commit_type"] == "feat"
    assert result["activity_type"] is None
    assert result["files_changed"] == 2
    assert result["insertions"] == 10
    assert result["deletions"] == 0
    assert result["enriched"] is False
    assert result["people"] == []


def test_parse_activity_event():
    result = parse_work_event(ACTIVITY_EVENTS[1])
    assert result is not None
    assert result["type"] == "activity"
    assert result["repo"] == "ansible-hermes"
    assert result["activity_type"] == "review"
    assert result["commit_type"] is None
    assert result["people"] == ["joseph.struth", "pratima.shetty"]
    assert result["enriched"] is True


def test_parse_unrecognized_returns_none():
    assert parse_work_event(UNRECOGNIZED_EVENT) is None


def test_parse_activity_uses_project_as_repo():
    """Activity events have 'project' not 'repo' — parser normalizes to 'repo'."""
    result = parse_work_event(ACTIVITY_EVENTS[0])
    assert result["repo"] == "ansible-hermes"


def test_period_key_month():
    assert period_key("2026-03-12T18:56:41Z", "month") == "2026-03"


def test_period_key_week():
    assert period_key("2026-03-12T18:56:41Z", "week") == "2026-W11"


def test_period_key_day():
    assert period_key("2026-03-12T18:56:41Z", "day") == "2026-03-12"


def test_aggregate_period_counts():
    parsed = [parse_work_event(e) for e in ALL_MARCH_EVENTS]
    parsed = [p for p in parsed if p is not None]
    stats = aggregate_period("2026-03", parsed)

    assert stats.period == "2026-03"
    assert stats.total_events == 6
    assert stats.commits == 4
    assert stats.activities == 2
    assert stats.by_type["feat"] == 2
    assert stats.by_type["fix"] == 1
    assert stats.by_type["test"] == 1
    assert stats.by_repo["mailapi"] == 2
    assert stats.by_repo["ansible-hermes"] == 3  # 1 commit + 2 activities
    assert stats.by_category["servicenow_core"] == 2
    assert stats.by_category["infrastructure"] == 3
    assert stats.files_changed == 10
    assert stats.insertions == 150
    assert stats.deletions == 8
    assert stats.enriched_count == 2
    assert stats.collaborators["joseph.struth"] == 1
    assert stats.collaborators["pratima.shetty"] == 1


def test_work_stats_to_dict():
    stats = WorkStats(
        period="2026-03",
        total_events=6,
        commits=4,
        activities=2,
        by_type={"feat": 2},
        by_category={"servicenow_core": 2},
        by_repo={"mailapi": 2},
        files_changed=10,
        insertions=150,
        deletions=8,
        enriched_count=2,
        collaborators={"joseph.struth": 1},
    )
    d = stats.to_dict()
    assert d["period"] == "2026-03"
    assert d["commits"] == 4
    assert d["by_type"]["feat"] == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_parsing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'brooklet_work.parsing'`

- [ ] **Step 3: Implement parsing.py**

Create `src/brooklet_work/parsing.py`:

```python
# ABOUTME: Work tracker event parsing — pure functions, no I/O
# ABOUTME: Normalizes commit and activity events, aggregates into WorkStats per period

from dataclasses import dataclass, field
from datetime import datetime

RECOGNIZED_TYPES = {"commit", "activity"}


def parse_work_event(event: dict) -> dict | None:
    """Normalize a raw work tracker event into a common shape.

    Returns None for unrecognized event types.
    """
    event_type = event.get("type")
    if event_type not in RECOGNIZED_TYPES:
        return None

    # Activities use "project" instead of "repo"
    repo = event.get("repo") or event.get("project") or "unknown"

    return {
        "type": event_type,
        "timestamp": event.get("timestamp"),
        "repo": repo,
        "category": event.get("category", "unknown"),
        "commit_type": event.get("commit_type") if event_type == "commit" else None,
        "activity_type": event.get("activity_type") if event_type == "activity" else None,
        "files_changed": event.get("files_changed", 0) or 0,
        "insertions": event.get("insertions", 0) or 0,
        "deletions": event.get("deletions", 0) or 0,
        "enriched": event.get("enriched", False),
        "people": event.get("people", []),
    }


def period_key(timestamp: str, period: str) -> str:
    """Derive a period grouping key from an ISO timestamp.

    Args:
        timestamp: ISO 8601 timestamp string.
        period: One of "month", "week", "day".
    """
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if period == "month":
        return f"{dt.year}-{dt.month:02d}"
    elif period == "week":
        iso_year, iso_week, _ = dt.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    elif period == "day":
        return f"{dt.year}-{dt.month:02d}-{dt.day:02d}"
    msg = f"Unknown period: {period!r}"
    raise ValueError(msg)


@dataclass
class WorkStats:
    """Aggregated statistics for a time period."""

    period: str
    total_events: int = 0
    commits: int = 0
    activities: int = 0
    by_type: dict[str, int] = field(default_factory=dict)
    by_category: dict[str, int] = field(default_factory=dict)
    by_repo: dict[str, int] = field(default_factory=dict)
    files_changed: int = 0
    insertions: int = 0
    deletions: int = 0
    enriched_count: int = 0
    collaborators: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to a plain dict for JSONL serialization."""
        return {
            "period": self.period,
            "total_events": self.total_events,
            "commits": self.commits,
            "activities": self.activities,
            "by_type": dict(self.by_type),
            "by_category": dict(self.by_category),
            "by_repo": dict(self.by_repo),
            "files_changed": self.files_changed,
            "insertions": self.insertions,
            "deletions": self.deletions,
            "enriched_count": self.enriched_count,
            "collaborators": dict(self.collaborators),
        }


def aggregate_period(period: str, events: list[dict]) -> WorkStats:
    """Aggregate parsed events into stats for a time period."""
    stats = WorkStats(period=period)
    stats.total_events = len(events)

    for ev in events:
        if ev["type"] == "commit":
            stats.commits += 1
            ct = ev.get("commit_type")
            if ct:
                stats.by_type[ct] = stats.by_type.get(ct, 0) + 1
        elif ev["type"] == "activity":
            stats.activities += 1

        repo = ev["repo"]
        stats.by_repo[repo] = stats.by_repo.get(repo, 0) + 1

        category = ev["category"]
        stats.by_category[category] = stats.by_category.get(category, 0) + 1

        stats.files_changed += ev.get("files_changed", 0)
        stats.insertions += ev.get("insertions", 0)
        stats.deletions += ev.get("deletions", 0)

        if ev.get("enriched"):
            stats.enriched_count += 1

        for person in ev.get("people", []):
            stats.collaborators[person] = stats.collaborators.get(person, 0) + 1

    return stats
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_parsing.py -v`
Expected: All PASS

- [ ] **Step 5: Run ruff**

Run: `uv run ruff check src/brooklet_work/parsing.py tests/test_parsing.py && uv run ruff format src/brooklet_work/parsing.py tests/test_parsing.py`

- [ ] **Step 6: Commit**

```bash
git add src/brooklet_work/parsing.py tests/test_parsing.py
git commit -m "feat: add work event parsing and WorkStats aggregation"
```

---

### Task 4: Layer 2 — Consumer integration

**Files:**
- Create: `src/brooklet_work/consumer.py`
- Create: `tests/test_consumer.py`

- [ ] **Step 1: Write failing tests for consumer**

Create `tests/test_consumer.py`:

```python
# ABOUTME: Tests for work tracker consumer integration
# ABOUTME: Verifies scan_work iterator with period grouping and filtering

from brooklet_work.consumer import scan_work


def test_scan_work_monthly(work_dir):
    """scan_work groups events by month."""
    periods = list(scan_work(str(work_dir), period="month"))
    assert len(periods) == 2
    months = [p.period for p in periods]
    assert "2026-02" in months
    assert "2026-03" in months


def test_scan_work_weekly(work_dir):
    """scan_work groups events by ISO week."""
    periods = list(scan_work(str(work_dir), period="week"))
    weeks = [p.period for p in periods]
    assert len(weeks) >= 2  # Feb and March span multiple weeks


def test_scan_work_daily(single_month_dir):
    """scan_work groups events by day."""
    periods = list(scan_work(str(single_month_dir), period="day"))
    days = [p.period for p in periods]
    assert "2026-03-12" in days
    assert "2026-03-20" in days


def test_scan_work_filter_repo(work_dir):
    """scan_work filters by repo."""
    periods = list(scan_work(str(work_dir), repo="mailapi"))
    total = sum(p.total_events for p in periods)
    assert total == 3  # 2 mailapi commits in March + 1 in Feb


def test_scan_work_filter_category(work_dir):
    """scan_work filters by category."""
    periods = list(scan_work(str(work_dir), category="mcp_servers"))
    total = sum(p.total_events for p in periods)
    assert total == 2  # 1 snowk8s commit in March + 1 in Feb


def test_scan_work_filter_commit_type(work_dir):
    """scan_work filters by commit type."""
    periods = list(scan_work(str(work_dir), commit_type="feat"))
    total = sum(p.total_events for p in periods)
    assert total == 2  # 2 feat commits in March


def test_scan_work_empty_dir(tmp_path):
    """scan_work yields nothing for empty directory."""
    empty = tmp_path / "empty"
    empty.mkdir()
    periods = list(scan_work(str(empty)))
    assert periods == []


def test_scan_work_periods_sorted(work_dir):
    """Periods are yielded in chronological order."""
    periods = list(scan_work(str(work_dir), period="month"))
    keys = [p.period for p in periods]
    assert keys == sorted(keys)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_consumer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement consumer.py**

Create `src/brooklet_work/consumer.py`:

```python
# ABOUTME: Work tracker consumer integration — scan_work() iterator
# ABOUTME: Reads JSONL files, groups by period, applies filters, yields WorkStats

import glob as glob_module
import json
import sys
from collections.abc import Iterator
from pathlib import Path

from brooklet_work.parsing import WorkStats, aggregate_period, parse_work_event, period_key


def _parse_file_events(filepath: str) -> list[dict]:
    """Parse all events from a JSONL file.

    Reads directly without brooklet offset tracking (batch mode).
    Skips malformed JSON lines with a warning to stderr.
    """
    events = []
    skipped_lines = 0
    total_lines = 0
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                skipped_lines += 1
                continue
    if skipped_lines and total_lines:
        print(
            f"Warning: {filepath}: {skipped_lines}/{total_lines} lines failed JSON parsing",
            file=sys.stderr,
        )
    return events


def _matches_filters(
    event: dict,
    repo: str | None,
    category: str | None,
    commit_type: str | None,
) -> bool:
    """Check if a parsed event matches the active filters."""
    if repo and event.get("repo") != repo:
        return False
    if category and event.get("category") != category:
        return False
    if commit_type and event.get("commit_type") != commit_type:
        return False
    return True


def scan_work(
    path: str,
    period: str = "month",
    repo: str | None = None,
    category: str | None = None,
    commit_type: str | None = None,
) -> Iterator[WorkStats]:
    """Scan work tracker JSONL files and yield stats per period.

    Batch mode: reads all *.jsonl files directly, groups events by
    period key, yields WorkStats in chronological order.

    Args:
        path: Directory containing monthly JSONL files.
        period: Grouping period — "month", "week", or "day".
        repo: Filter to events from this repo only.
        category: Filter to events in this category only.
        commit_type: Filter to commits of this type only.
    """
    jsonl_dir = Path(path)
    if not jsonl_dir.is_dir():
        return

    filepaths = sorted(glob_module.glob(str(jsonl_dir / "*.jsonl")))
    if not filepaths:
        return

    # Collect all events, parse, filter, and group by period
    buckets: dict[str, list[dict]] = {}

    for filepath in filepaths:
        raw_events = _parse_file_events(filepath)
        for raw in raw_events:
            parsed = parse_work_event(raw)
            if parsed is None:
                continue
            if not _matches_filters(parsed, repo, category, commit_type):
                continue
            ts = parsed.get("timestamp")
            if not ts:
                continue
            key = period_key(ts, period)
            if key not in buckets:
                buckets[key] = []
            buckets[key].append(parsed)

    # Yield in chronological order
    for key in sorted(buckets):
        yield aggregate_period(key, buckets[key])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_consumer.py -v`
Expected: All PASS

- [ ] **Step 5: Run ruff**

Run: `uv run ruff check src/brooklet_work/consumer.py tests/test_consumer.py && uv run ruff format src/brooklet_work/consumer.py tests/test_consumer.py`

- [ ] **Step 6: Commit**

```bash
git add src/brooklet_work/consumer.py tests/test_consumer.py
git commit -m "feat: add scan_work consumer with period grouping and filters"
```

---

### Task 5: Layer 3 — Rendering

**Files:**
- Create: `src/brooklet_work/rendering.py`
- Create: `tests/test_rendering.py`

- [ ] **Step 1: Write failing tests for rendering**

Create `tests/test_rendering.py`:

```python
# ABOUTME: Tests for work tracker text renderers
# ABOUTME: Verifies render_period_block and render_cumulative output

from brooklet_work.parsing import WorkStats
from brooklet_work.rendering import render_cumulative, render_period_block


def _sample_stats():
    return WorkStats(
        period="2026-03",
        total_events=6,
        commits=4,
        activities=2,
        by_type={"feat": 2, "fix": 1, "test": 1},
        by_category={"servicenow_core": 2, "infrastructure": 3, "mcp_servers": 1},
        by_repo={"mailapi": 2, "ansible-hermes": 3, "snowk8s-mcp": 1},
        files_changed=10,
        insertions=150,
        deletions=8,
        enriched_count=2,
        collaborators={"joseph.struth": 1, "pratima.shetty": 1},
    )


def test_render_period_block_contains_period():
    output = render_period_block(_sample_stats())
    assert "2026-03" in output


def test_render_period_block_contains_counts():
    output = render_period_block(_sample_stats())
    assert "4 commits" in output
    assert "2 activities" in output


def test_render_period_block_contains_types():
    output = render_period_block(_sample_stats())
    assert "feat=2" in output
    assert "fix=1" in output


def test_render_period_block_contains_repos():
    output = render_period_block(_sample_stats())
    assert "mailapi" in output
    assert "ansible-hermes" in output


def test_render_period_block_contains_changes():
    output = render_period_block(_sample_stats())
    assert "+150" in output
    assert "-8" in output


def test_render_period_block_contains_collaborators():
    output = render_period_block(_sample_stats())
    assert "joseph.struth" in output


def test_render_period_block_contains_enriched():
    output = render_period_block(_sample_stats())
    assert "2/6" in output


def test_render_cumulative():
    stats_list = [_sample_stats(), _sample_stats()]
    output = render_cumulative(stats_list)
    assert "2 periods" in output
    assert "12" in output  # total events


def test_render_cumulative_empty():
    output = render_cumulative([])
    assert "No data" in output
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_rendering.py -v`
Expected: FAIL

- [ ] **Step 3: Implement rendering.py**

Create `src/brooklet_work/rendering.py`:

```python
# ABOUTME: Work tracker output renderers — text and rich dashboard
# ABOUTME: Formats WorkStats as human-readable text blocks

from brooklet_work.parsing import WorkStats


def _format_number(n: int) -> str:
    """Format a number with comma separators."""
    return f"{n:,}"


def render_period_block(stats: WorkStats) -> str:
    """Render a single period's stats as plain text."""
    lines = []
    lines.append(f"--- {stats.period} ({stats.commits} commits, {stats.activities} activities) ---")

    # By commit type (sorted by count descending)
    if stats.by_type:
        type_parts = [f"{t}={c}" for t, c in sorted(stats.by_type.items(), key=lambda x: x[1], reverse=True)]
        lines.append(f"  by type:  {' '.join(type_parts)}")

    # By repo (top 10, sorted by count descending)
    if stats.by_repo:
        repo_parts = [f"{r}={c}" for r, c in sorted(stats.by_repo.items(), key=lambda x: x[1], reverse=True)[:10]]
        lines.append(f"  by repo:  {' '.join(repo_parts)}")

    # By category (sorted by count descending)
    if stats.by_category:
        cat_parts = [f"{c}={n}" for c, n in sorted(stats.by_category.items(), key=lambda x: x[1], reverse=True)]
        lines.append(f"  by category:  {' '.join(cat_parts)}")

    # Code changes
    lines.append(
        f"  changes:  +{_format_number(stats.insertions)} "
        f"-{_format_number(stats.deletions)} "
        f"across {_format_number(stats.files_changed)} files"
    )

    # Enrichment ratio
    lines.append(f"  enriched: {stats.enriched_count}/{stats.total_events}")

    # Collaborators
    if stats.collaborators:
        collab_parts = [f"{name}({c})" for name, c in sorted(stats.collaborators.items(), key=lambda x: x[1], reverse=True)]
        lines.append(f"  collaborators: {', '.join(collab_parts)}")

    return "\n".join(lines)


def render_cumulative(periods: list[WorkStats]) -> str:
    """Render aggregate totals across all periods."""
    if not periods:
        return "No data processed."

    total_events = sum(p.total_events for p in periods)
    total_commits = sum(p.commits for p in periods)
    total_activities = sum(p.activities for p in periods)
    total_insertions = sum(p.insertions for p in periods)
    total_deletions = sum(p.deletions for p in periods)
    total_files = sum(p.files_changed for p in periods)

    lines = []
    lines.append(f"\n=== {len(periods)} periods totals ===")
    lines.append(
        f"  {_format_number(total_events)} events: "
        f"{_format_number(total_commits)} commits, "
        f"{_format_number(total_activities)} activities"
    )
    lines.append(
        f"  changes: +{_format_number(total_insertions)} "
        f"-{_format_number(total_deletions)} "
        f"across {_format_number(total_files)} files"
    )

    return "\n".join(lines)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_rendering.py -v`
Expected: All PASS

- [ ] **Step 5: Run ruff**

Run: `uv run ruff check src/brooklet_work/rendering.py tests/test_rendering.py && uv run ruff format src/brooklet_work/rendering.py tests/test_rendering.py`

- [ ] **Step 6: Commit**

```bash
git add src/brooklet_work/rendering.py tests/test_rendering.py
git commit -m "feat: add work tracker text renderers"
```

---

### Task 6: Plugin wiring — WorkPlugin with Typer commands

**Files:**
- Modify: `src/brooklet_work/__init__.py`
- Create: `tests/test_plugin.py`

- [ ] **Step 1: Write failing tests for the plugin**

Create `tests/test_plugin.py`:

```python
# ABOUTME: End-to-end CLI tests for brooklet-work plugin
# ABOUTME: Verifies WorkPlugin registers commands and scan produces output

import typer
from typer.testing import CliRunner

from brooklet_work import WorkPlugin

runner = CliRunner()


def test_work_plugin_registers_commands():
    """WorkPlugin adds 'work' subcommand group."""
    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["work", "--help"])
    assert result.exit_code == 0
    assert "scan" in result.output


def test_work_scan_with_path(work_dir):
    """brooklet work scan produces output for sample data."""
    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["work", "scan", str(work_dir)])
    assert result.exit_code == 0
    assert "2026-03" in result.output
    assert "commits" in result.output


def test_work_scan_period_week(work_dir):
    """--period week groups by ISO week."""
    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["work", "scan", str(work_dir), "--period", "week"])
    assert result.exit_code == 0
    assert "W" in result.output


def test_work_scan_filter_repo(work_dir):
    """--repo filters to a single repo."""
    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["work", "scan", str(work_dir), "--repo", "mailapi"])
    assert result.exit_code == 0
    assert "mailapi" in result.output


def test_work_scan_no_path_no_env(monkeypatch):
    """Errors when no path and no BROOKLET_WORK_DIR."""
    monkeypatch.delenv("BROOKLET_WORK_DIR", raising=False)
    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["work", "scan"])
    assert result.exit_code != 0


def test_work_scan_env_var(work_dir, monkeypatch):
    """BROOKLET_WORK_DIR env var provides the default path."""
    monkeypatch.setenv("BROOKLET_WORK_DIR", str(work_dir))
    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["work", "scan"])
    assert result.exit_code == 0
    assert "2026-03" in result.output


def test_work_scan_follow_not_implemented(work_dir):
    """--follow exits with error since it's not yet implemented."""
    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["work", "scan", str(work_dir), "--follow"])
    assert result.exit_code != 0
    assert "not yet implemented" in result.output.lower()


def test_work_scan_dashboard_not_implemented(work_dir):
    """--dashboard exits with error since it's not yet implemented."""
    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["work", "scan", str(work_dir), "--dashboard"])
    assert result.exit_code != 0
    assert "not yet implemented" in result.output.lower()


def test_work_scan_output_produces_to_topic(work_dir):
    """--output produces WorkStats to a brooklet topic."""
    import brooklet

    app = typer.Typer()
    plugin = WorkPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, [
        "work", "scan", str(work_dir), "--output", "work/stats",
    ])
    assert result.exit_code == 0

    # Verify events were produced to the topic
    stream = brooklet.open(work_dir)
    topics = stream.topics()
    assert "work/stats" in topics
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_plugin.py -v`
Expected: FAIL — WorkPlugin has no `brooklet_commands`

- [ ] **Step 3: Implement WorkPlugin**

Replace `src/brooklet_work/__init__.py`:

```python
# ABOUTME: brooklet-work — personal work tracker analytics plugin for brooklet
# ABOUTME: Registers WorkPlugin via pluggy entry point for brooklet CLI discovery

import os
from typing import Annotated

import typer

import brooklet
from brooklet.plugins import hookimpl
from brooklet_work.consumer import scan_work
from brooklet_work.rendering import render_cumulative, render_period_block

__version__ = "0.1.0"


class WorkPlugin:
    """Pluggy plugin that registers work tracker CLI commands."""

    @hookimpl
    def brooklet_commands(self, cli):
        work_app = typer.Typer(help="Work tracker analytics")

        @work_app.command()
        def scan(
            path: Annotated[str | None, typer.Argument(help="Path to directory of JSONL files.")] = None,
            period: Annotated[str, typer.Option(help="Grouping period: month, week, or day.")] = "month",
            repo: Annotated[str | None, typer.Option(help="Filter to this repo.")] = None,
            category: Annotated[str | None, typer.Option(help="Filter to this category.")] = None,
            commit_type: Annotated[str | None, typer.Option("--type", help="Filter to this commit type.")] = None,
            follow: Annotated[bool, typer.Option("--follow", help="Tail for new events.")] = False,
            dashboard: Annotated[bool, typer.Option("--dashboard", help="Rich live dashboard.")] = False,
            output: Annotated[str | None, typer.Option(help="Produce stats to a brooklet topic.")] = None,
        ) -> None:
            """Scan work tracker JSONL files and report analytics."""
            # Resolve path
            resolved = path or os.environ.get("BROOKLET_WORK_DIR")
            if not resolved:
                typer.echo(
                    "Error: No path provided. Set BROOKLET_WORK_DIR or pass a path argument.",
                    err=True,
                )
                raise typer.Exit(code=1)

            if follow:
                typer.echo("Error: --follow is not yet implemented.", err=True)
                raise typer.Exit(code=1)

            if dashboard:
                typer.echo("Error: --dashboard is not yet implemented.", err=True)
                raise typer.Exit(code=1)

            stats_iter = scan_work(
                path=resolved,
                period=period,
                repo=repo,
                category=category,
                commit_type=commit_type,
            )

            # Wrap iterator to produce stats to a brooklet topic if --output
            if output:
                stream = brooklet.open(resolved)
                original_iter = stats_iter

                def producing_iter():
                    for stats in original_iter:
                        try:
                            stream.produce(output, stats.to_dict(), source="brooklet-work")
                        except (OSError, ValueError, TypeError) as e:
                            typer.echo(
                                f"Warning: failed to produce {stats.period} to {output!r}: {e}",
                                err=True,
                            )
                        yield stats

                stats_iter = producing_iter()

            periods = []
            for stats in stats_iter:
                periods.append(stats)
                typer.echo(render_period_block(stats))
            typer.echo(render_cumulative(periods))

        cli.add_typer(work_app, name="work")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_plugin.py -v`
Expected: All PASS

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 6: Run ruff on everything**

Run: `uv run ruff check src/ tests/ && uv run ruff format src/ tests/`

- [ ] **Step 7: Commit**

```bash
git add src/brooklet_work/__init__.py tests/test_plugin.py
git commit -m "feat: add WorkPlugin with Typer scan command"
```

---

### Task 7: Integration test with brooklet CLI

**Files:**
- Modify: `tests/test_plugin.py`

- [ ] **Step 1: Install plugin into brooklet's environment and verify discovery**

From the brooklet repo, install brooklet-work as editable:

```bash
cd ~/Dropbox/python_workspace/brooklet
uv add --editable ../brooklet-work --group dev
```

- [ ] **Step 2: Verify brooklet discovers the plugin**

```bash
uv run brooklet --help
```

Expected: `work` appears in the help output alongside scout and pytest.

- [ ] **Step 3: Test against real data**

```bash
uv run brooklet work scan ~/Dropbox/python_workspace/second_brain/areas/ServiceNow/work-tracker/raw/
uv run brooklet work scan ~/Dropbox/python_workspace/second_brain/areas/ServiceNow/work-tracker/raw/ --period week
uv run brooklet work scan ~/Dropbox/python_workspace/second_brain/areas/ServiceNow/work-tracker/raw/ --repo mailapi --type feat
```

- [ ] **Step 4: Commit any fixes from integration testing**

```bash
git add -A && git commit -m "fix: integration testing adjustments"
```

(Only if needed — skip if everything works.)

---

### Task 8: Close beads tasks

**Files:** None (beads CLI only)

- [ ] **Step 1: Close completed beads tasks**

```bash
bd close brooklet-5de brooklet-1t8 brooklet-2e7 brooklet-atu
```

These correspond to:
- `brooklet-5de`: parsing layer
- `brooklet-1t8`: consumer integration
- `brooklet-2e7`: output renderers
- `brooklet-atu`: WorkPlugin with Typer commands
