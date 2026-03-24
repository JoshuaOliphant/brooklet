# brooklet-work — Work Tracker Analytics Plugin

**Date**: 2026-03-24
**Status**: Proposed
**Relates to**: brooklet CLI plugin system (Typer + pluggy)

## Overview

A personal brooklet plugin that analyzes ServiceNow work-tracker JSONL data — git commits and manual activity entries across ~20 repos spanning 15+ months. Installs as a separate package, auto-discovered by brooklet via pluggy entry points. Provides `brooklet work scan` with configurable time grouping, filtering, and live dashboard.

## Goals

- Analyze commit velocity, repo breakdown, category distribution, and collaboration patterns
- Configurable period grouping: day, week, month
- Filter by repo, category, or commit type
- Follow mode for live updates as new commits land
- Rich dashboard for visual overview

## Non-Goals

- LLM-powered enrichment (deferred to follow-up spec)
- Promotion evidence rollup (deferred — depends on enrichment)
- Publishing to PyPI (personal plugin, not shared)

## Data Shape

Source: `~/Dropbox/python_workspace/second_brain/areas/ServiceNow/work-tracker/raw/*.jsonl`

Monthly JSONL files (e.g., `2026-03.jsonl`), ~2,800 events across 15 months.

### Event Types

**Commit** (~98% of events):
```json
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
  "scope": null,
  "category": "servicenow_core",
  "enriched": false
}
```

**Activity** (~2% of events):
```json
{
  "type": "activity",
  "timestamp": "2026-03-12T19:00:00Z",
  "activity_type": "review",
  "description": "Resolved merge conflict in MR !166...",
  "project": "ansible-hermes",
  "people": ["joseph.struth"],
  "enriched": true,
  "impact": "medium",
  "ic4_markers": {...},
  "learnings": "..."
}
```

### Key Fields

| Field | Values | Present On |
|-------|--------|-----------|
| `type` | `commit`, `activity` | Both |
| `timestamp` | ISO 8601 | Both |
| `repo` / `project` | ~20 repos | Both |
| `category` | `servicenow_core`, `mcp_servers`, `infrastructure`, `personal`, `ai_tools`, etc. | Both |
| `commit_type` | `feat`, `fix`, `test`, `docs`, `chore`, `refactor`, `ci` | Commits |
| `activity_type` | `review`, `other` | Activities |
| `files_changed`, `insertions`, `deletions` | int | Commits |
| `enriched` | bool | Both |
| `people` | list of names | Activities |

## Architecture

### Package Structure

```
brooklet-work/
├── pyproject.toml
├── src/brooklet_work/
│   ├── __init__.py         # WorkPlugin class + hookimpl
│   ├── parsing.py          # Layer 1: parse_work_event(), WorkStats, aggregate_period()
│   ├── consumer.py         # Layer 2: scan_work() iterator
│   └── rendering.py        # Layer 3: text + rich renderers
└── tests/
    ├── conftest.py         # Fixtures: sample commit/activity JSONL data
    ├── test_parsing.py
    ├── test_consumer.py
    ├── test_rendering.py
    └── test_plugin.py      # End-to-end via CliRunner
```

Package name: `brooklet-work`. Import name: `brooklet_work`.

### Layer 1: Parsing (`parsing.py`)

Pure functions, no I/O.

**`parse_work_event(event: dict) -> dict | None`** — Normalizes commit and activity events into a common shape. Returns `None` for unrecognized event types.

Normalized output:
```python
{
    "type": "commit" | "activity",
    "timestamp": "2026-03-12T18:56:41Z",
    "repo": "mailapi",            # from "repo" (commits) or "project" (activities)
    "category": "servicenow_core",
    "commit_type": "feat",        # commits only, None for activities
    "activity_type": "review",    # activities only, None for commits
    "files_changed": 2,           # 0 for activities
    "insertions": 10,             # 0 for activities
    "deletions": 0,               # 0 for activities
    "enriched": False,
    "people": [],
}
```

**`WorkStats` dataclass:**
```python
@dataclass
class WorkStats:
    period: str                    # "2026-03", "2026-W12", "2026-03-23"
    total_events: int
    commits: int
    activities: int
    by_type: dict[str, int]        # {"feat": 12, "fix": 10, ...}
    by_category: dict[str, int]    # {"servicenow_core": 361, ...}
    by_repo: dict[str, int]        # {"mailapi": 353, ...}
    files_changed: int
    insertions: int
    deletions: int
    enriched_count: int
    collaborators: dict[str, int]  # {"joseph.struth": 2, ...}

    def to_dict(self) -> dict: ...
```

**`aggregate_period(period: str, events: list[dict]) -> WorkStats`** — Accumulates parsed events into a WorkStats for the given period key.

**`_parse_file_events(filepath: str) -> list[dict]`** — Reads a JSONL file, parses each line, skips malformed JSON with a warning to stderr (counting skipped lines), returns list of raw event dicts. Matches the error handling pattern in scout and pytest adapters.

### Layer 2: Consumer Integration (`consumer.py`)

Uses brooklet API for offset tracking and glob consumption.

**`scan_work(path, period, follow, repo, category, commit_type) -> Iterator[WorkStats]`**

```python
def scan_work(
    path: str,
    period: str = "month",
    follow: bool = False,
    repo: str | None = None,
    category: str | None = None,
    commit_type: str | None = None,
) -> Iterator[WorkStats]:
```

**Period grouping:**
- `month` — key derived from timestamp: `"2026-03"`
- `week` — ISO week: `"2026-W12"`
- `day` — date: `"2026-03-23"`

Events are collected from all files, then grouped by period key using a dict of accumulators. This collect-then-group approach handles out-of-order events and cross-file period boundaries (e.g., a week spanning two monthly files). After all events are consumed, periods are yielded in sorted order.

**Filtering:** Events are checked against optional `repo`, `category`, and `commit_type` filters before accumulation. Simple equality match.

**Brooklet integration:**
- Opens a brooklet stream with `.brooklet/` metadata alongside the raw data
- Batch mode: reads files directly via `_parse_file_events()` — no brooklet offset tracking, matching the pattern used by scout and pytest adapters in batch mode
- Follow mode: registers `path/*.jsonl` as a glob source, uses brooklet consumer API with offset tracking to tail for new events
- Follow mode targets the file matching the current system month (`YYYY-MM.jsonl`). At month boundaries, switches to the new month's file when it appears on disk.
- Mode is always glob (directory of `*.jsonl` files) — no single-file mode needed

### Layer 3: Rendering (`rendering.py`)

**`render_period_block(stats: WorkStats) -> str`** — Plain text output for one period:
```
--- 2026-03 (392 commits, 6 activities) ---
  by type:  test=301  chore=39  feat=12  fix=10  docs=8
  by repo:  mailapi=353  marketplace=15  snowk8s-mcp=6
  by category:  servicenow_core=361  personal=16  mcp_servers=11
  changes:  +1,234 -567 across 412 files
  enriched: 4/398 (1%)
  collaborators: joseph.struth(2), pratima.shetty(1)
```

**`render_cumulative(periods: list[WorkStats]) -> str`** — Totals across all periods.

**`render_rich(stats_iter: Iterator[WorkStats]) -> None`** — Rich live table with columns: Period, Commits, Activities, Top Type, Top Repo, +/-, Collaborators. Updates live in follow mode.

### Plugin Wiring (`__init__.py`)

```python
class WorkPlugin:
    @hookimpl
    def brooklet_commands(self, cli):
        work_app = typer.Typer(help="Work tracker analytics")

        @work_app.command()
        def scan(
            path: Annotated[str | None, typer.Argument(...)] = None,
            period: Annotated[str, typer.Option(...)] = "month",
            repo: Annotated[str | None, typer.Option(...)] = None,
            category: Annotated[str | None, typer.Option(...)] = None,
            commit_type: Annotated[str | None, typer.Option("--type", ...)] = None,
            follow: Annotated[bool, typer.Option(...)] = False,
            dashboard: Annotated[bool, typer.Option(...)] = False,
            output: Annotated[str | None, typer.Option(...)] = None,
        ): ...

        cli.add_typer(work_app, name="work")
```

Entry point in `pyproject.toml`:
```toml
[project.entry-points.brooklet]
work = "brooklet_work:WorkPlugin"
```

## CLI Usage

```bash
# Default: monthly stats, all data
brooklet work scan

# With env var set (BROOKLET_WORK_DIR)
export BROOKLET_WORK_DIR=~/Dropbox/.../work-tracker/raw/
brooklet work scan

# Explicit path
brooklet work scan ~/Dropbox/.../work-tracker/raw/

# Weekly breakdown
brooklet work scan --period week

# Filter to feature work on MCP servers
brooklet work scan --category mcp_servers --type feat

# Single repo
brooklet work scan --repo mailapi --period day

# Live dashboard
brooklet work scan --follow --dashboard

# Produce stats for downstream consumers (one WorkStats.to_dict() per period)
brooklet work scan --output work/monthly-stats
```

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `brooklet` | `>=0.1.1` | Core library + plugin hookimpl |
| `typer` | `>=0.9` | CLI commands |
| `rich` | `>=14.0` | Dashboard rendering |

Dev dependencies: `pytest`, `ruff`.

## Data Path Resolution

1. If `path` CLI argument provided, use it
2. Else if `BROOKLET_WORK_DIR` env var set, use it
3. Else error: "No work tracker path provided. Set BROOKLET_WORK_DIR or pass a path argument."

## File Changes

### New Repo: `brooklet-work/`

| File | Purpose |
|------|---------|
| `pyproject.toml` | Package config, brooklet entry point, dependencies |
| `src/brooklet_work/__init__.py` | WorkPlugin, hookimpl |
| `src/brooklet_work/parsing.py` | Event parsing, WorkStats, aggregation |
| `src/brooklet_work/consumer.py` | scan_work() iterator with brooklet API |
| `src/brooklet_work/rendering.py` | Text + rich renderers |
| `tests/conftest.py` | Test fixtures |
| `tests/test_parsing.py` | Parsing + aggregation tests |
| `tests/test_consumer.py` | Consumer integration tests |
| `tests/test_rendering.py` | Renderer output tests |
| `tests/test_plugin.py` | End-to-end CLI tests |

### No Changes to brooklet core

The plugin is discovered via entry points — no modifications to brooklet's `plugins.py` or any other file.

## Testing Strategy

**Fixtures:** Hardcoded sample data in `tests/conftest.py` — a mix of commit and activity events across multiple months, repos, categories, and commit types. Same pattern as brooklet's `pytest_fixtures.py`.

**`test_parsing.py`:** parse_work_event for commits, activities, unrecognized types. WorkStats aggregation with known inputs.

**`test_consumer.py`:** scan_work with sample JSONL files on disk. Period grouping (day/week/month). Filtering by repo/category/type. Offset tracking across runs.

**`test_rendering.py`:** render_period_block and render_cumulative produce expected text output.

**`test_plugin.py`:** End-to-end via Typer CliRunner — `brooklet work scan <path>` produces output. Filters work. `--period` flag changes grouping.

## Deferred Work

- **Enrichment consumer pipeline** — LLM-powered classification of impact, learnings, IC4 markers (beads: brooklet-57u)
- **Promotion evidence rollup** — aggregation by IC4 capability (beads: brooklet-hcx)
- **Rich dashboard** — included in design but could be deferred if implementation gets complex
