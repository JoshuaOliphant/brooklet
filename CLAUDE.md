# Brooklet — The SQLite of Event Streaming

Lightweight JSONL-based event streaming library. Adds consumer coordination (offsets, tailing, topic discovery) on top of append-only JSONL files that other tools already produce.

## Architecture

Brooklet is a **consumer coordination layer**, not a message broker. External tools write JSONL; brooklet reads it with offset tracking.

Source layout uses three subpackages so directory names communicate intent (the "namespaces are one honking great idea" principle applied to layout — directory paths are an interface for both humans and LLM tools).

- `__init__.py` — Public API: `brooklet.open(path)`

#### `core/` — primitives + main code paths
- `core/envelope.py` — Metadata injection (_ts, _seq, _src): `wrap()` on read, `serialize()` on write, `SeqTracker` for high-water-mark fallback _seq across a topic's read
- `core/types.py` — Shared type definitions (Mode, Event, offset dataclasses, SourceDef, EnvelopeMeta) and `BrookletWriteLockError`
- `core/stream.py` — Orchestrator: `register()`, `produce()`, `consume()`, `read()`, `topics()`. Segment rotation + sidecar + flock
- `core/consumer.py` — Batch and follow-mode iterators over JSONL files. `Consumer` is a thin mode-dispatcher over two internal strategy units: `_GlobCatchUp` (glob-mode catch-up, owns its own active-file/offset coordination state) and `_SingleFileReader` (single-file batch + follow/tailing, owns the open handle and running offset). Both expose an `.offset`/`.events()` shape; `Consumer._persist_offset()` is the single shared "save, report-don't-raise on OSError, save-before-assign" contract used by both teardown paths.

#### `storage/` — persistence layer (everything under `.brooklet/`)
- `storage/offsets.py` — Byte offset persistence per consumer group
- `storage/sidecar.py` — Sequence number sidecar cache for O(1) next-seq lookups with crash recovery
- `storage/locking.py` — Topic-level file locking via `fcntl.flock(LOCK_EX|LOCK_NB)` for single-writer enforcement
- `storage/registry.py` — Maps topic names to sources; supports external (registered) and local (produced)
- `storage/segments.py` — Single source of truth for the `data-NNNN.jsonl` segment-file naming convention shared by producer and consumer
- `storage/atomic.py` — `atomic_write_text()`: the crash-safe temp-file-then-`os.replace` write behind every JSON document under `.brooklet/`
- `storage/names.py` — `validate_safe_name()`: the path-traversal / unsafe-character guard for topic and group names (shared by registry and offsets)

#### `cli/` — Typer app and plugin loading
- `cli/app.py` — Unified CLI entry point; Typer app with core commands (`register`, `topics`, `produce`, `consume`, `watch`, `cat`) and plugin loading. The `brooklet` script is declared in `pyproject.toml` as `brooklet.cli.app:main`.
- `cli/__init__.py` — Lazily exposes `app`, `main`, `_watch_impl` via `__getattr__`, so `cli.plugins` can be imported without pulling in the whole app module
- `cli/plugins.py` — Plugin system using pluggy for CLI extensibility (`hookimpl` lives here)
- `cli/watch_format.py` — One-line-per-event formatter for `brooklet watch`

### Contrib Adapters (3-layer pattern: parsing → consumer integration → CLI)
- `contrib/claude_analytics.py` — Claude Code session analytics (`brooklet scout scan`)
- `contrib/pytest_analytics.py` — pytest-reportlog test run analytics (`brooklet pytest scan`)
- `contrib/otel.py` — Optional OpenTelemetry instrumentation *of brooklet itself* (tracing + metrics); no-op `_NoOpTracer`/`_NoOpMeter` without the `otel` dependency group installed
- `contrib/otel_consumer.py` — The mirror image of `otel.py`: consumes OTLP trace/metric/log JSONL that Vector wrote, rather than emitting telemetry (`brooklet otel traces|metrics|logs`)
- `contrib/topic_tee.py` — `tee_to_topic()`: shared passthrough sink for scan commands' `--output` mode (produce each stat to a topic, warn-not-raise on failure)
- `contrib/cli_options.py` — Shared `--stream-dir` Typer option definitions: `StreamDirOption` for adapters with an `--output` flag (scout, pytest) and `StreamDirOptionFollowOnly` for adapters without one (otel), so adapters reference one of two definitions instead of each retyping the `Annotated[Path | None, ...]` shape

### Key Decisions
- `produce()` is in core — consumers that transform and re-emit need a clean write path (DEC-011)
- Unified topic namespace with auto-registration — `produce()` auto-registers local topics (DEC-012)
- Source registration maps arbitrary external JSONL paths to topic names (DEC-007)
- Size-based segment rotation with flock single-writer enforcement (DEC-014)
- Thin envelope with `_ts`, `_seq`, `_src` auto-injected on both read and write (DEC-004)
- `_seq` is topic-monotonic — assigned once at produce time, preserved on read (DEC-015)
- watchdog for filesystem watching in follow mode (DEC-008)
- Python 3.12+ minimum (DEC-009)
- Path-style topic names (`scout/stats`) create nested directories
- Stream directory resolution: `--stream-dir` flag > `BROOKLET_DIR` env > current directory
- Full decision records at `docs/decisions/`

### Data Layout
```
<stream_dir>/
├── <topic>/
│   ├── data-0001.jsonl       # Segment 1 (local topics)
│   ├── data-0002.jsonl       # Segment 2
│   └── data-0003.jsonl       # Active segment
├── <parent>/<child>/
│   └── data-0001.jsonl       # Path-style nested topics
└── .brooklet/
    ├── sources.json          # Registry (external + local sources)
    ├── seq/
    │   └── <topic>.json      # {"next_seq": N} — sidecar cache
    ├── locks/
    │   └── <topic>.lock      # flock target for single-writer
    └── offsets/
        └── <group>-<topic>.json  # {"offset": N} per (group, topic)
```

Filenames under `.brooklet/` are flattened so path-style topics stay in one
directory, but `seq/`+`locks/` and `offsets/` use different schemes:

- `seq/` and `locks/` replace `/` with `--` (topic `scout/stats` → `scout--stats`).
- `offsets/` percent-escapes each field before joining them with a single `-`
  (`/` → `%2F`, `-` → `%2D`), which makes the (group, topic) → filename mapping
  injective — reserving `-` as the delimiter is what keeps the group/topic
  boundary unambiguous. Reads fall back to the older non-injective
  `<group>-<topic with / → -->.json` name when no encoded file exists, so
  existing consumers aren't rewound to zero.

## Dev Commands

```bash
uv run pytest -v              # Run tests
uv run pytest -v --tb=short   # Run tests with short traceback
uv run ruff check .           # Lint
uv run ruff format .          # Format

uv run pytest --cov=src/brooklet --cov-report=term-missing   # Coverage
```

`contrib/otel.py` only reaches full coverage with the optional OTel SDK
installed (`uv sync --group otel`); without that group its SDK-dependent
branches are unexercised. Note `otel` is a dependency group, not an extra —
`uv sync --extra otel` silently installs nothing.

## Conventions
- All `.py` files start with 2-line `ABOUTME:` comment
- TDD: tests first, then minimal implementation
- Simple over clever — readability is the priority
- Before adding new tests or fixtures, check for existing ones in `tests/conftest.py`, `tests/pytest_fixtures.py`, `tests/scout_helpers.py`, and `tests/otel_helpers.py` to avoid duplication

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

```bash
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file
rm -rf directory            # NOT: rm -r directory
```

Other commands: `apt-get -y`, `HOMEBREW_NO_AUTO_UPDATE=1 brew`, `scp -o BatchMode=yes`.

## Task Tracking — GitHub Issues

This project tracks work as **GitHub issues** on
https://github.com/JoshuaOliphant/brooklet/issues, using the `gh` CLI or the
GitHub MCP tools.

Beads and Forge are both retired here. Ignore any session guidance that says to
track work with beads, `bd`, TodoWrite, or markdown checklists — for this project
that guidance is superseded. `.beads/` is a read-only archive of historical
issues; do not create or update issues in it.

- Open an issue before starting non-trivial work, and cite its number in the
  commit message.
- Label every issue with its kind (`bug`, `enhancement`, `documentation`) and
  state the priority (P1–P4) in the body, as a `**Priority:** P2` line near the
  top. GitHub has no priority field and this repo has no priority labels.
- Issue bodies must stand alone. Cite concrete `file.py:line` references so a
  reader with no session history can act on one.
- Use GitHub's own linking for ordering and structure — `Blocked by #7` in the
  body, task lists, and sub-issues where they help.

### Tracker history

Work moved beads → Forge → GitHub. Two archived documents explain the trail:

- `docs/forge/beads-migration.md` — the 43 beads issues triaged before the Forge
  move (14 carried forward, 3 already done, 25 discarded), with the reasoning for
  each discard. Read it before wondering where an old `brooklet-xxx` id went.
- `docs/forge/platform-notes.md` — what was learned about the Forge platform.
  Archived; it does not describe current practice.

**Issues opened on Forge have not been migrated to GitHub.** Roughly 21 (#1–#21
in Forge's numbering) were open at the time of the move and are not represented
on GitHub; GitHub's own numbering starts fresh and already reuses those numbers
for unrelated PRs and issues. Never read a Forge issue number as a GitHub one.
`scripts/forge_issue.py` is retained solely to export that backlog, and needs a
Forge token (`sf auth git-credential joshua-oliphant/brooklet` plus `git config
forge.repo joshua-oliphant/brooklet`) to run.

## Forge (retired)

This project trialled Forge (smol.ai) as its issue tracker. It has been retired:
Forge is alpha, and the gaps that mattered — a CI runner that simulates steps
rather than executing them, a container runner with no Python, an allowlisted
wiki, silently dropped workflow keys — did not close.

The one remote is GitHub, and it always was:

```
$ git remote -v
origin  https://github.com/JoshuaOliphant/brooklet.git (fetch)
origin  https://github.com/JoshuaOliphant/brooklet.git (push)
```

There is no `forge` remote and no mirroring. `git push origin main` pushes to
GitHub, and issues live there too, so code and tracker are no longer split
across hosts.

The detailed platform findings are archived in `docs/forge/platform-notes.md`.
Two leftovers are inert but still in the tree:

- `.claude/hooks/smolforge-transcript.py` — published session transcripts to
  Forge commits. Off by default, not wired into `.claude/settings.json`, and
  non-functional without a Forge token.
- `scripts/forge_issue.py` and `scripts/forge_check_updates.py` — kept so the
  un-migrated Forge backlog can still be exported. See Task Tracking above.

## Harness Engineering

This project uses Claude Code harness engineering.

- **Hooks** enforce quality gates automatically (lint on edit, tests on stop)
- **Rules** in `.claude/rules/` load context only when touching relevant files
- **Convention tests** in `tests/test_conventions.py` enforce ABOUTME mechanically
- **Decision records** live in `docs/decisions/` (DEC-NNN format)

## Agent skills

### Issue tracker

GitHub issues at https://github.com/JoshuaOliphant/brooklet/issues, via the
`gh` CLI or the GitHub MCP tools. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, unrenamed: `needs-triage`, `needs-info`,
`ready-for-agent`, `ready-for-human`, `wontfix`. None exist on the GitHub repo
yet — create them on first use. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context. Decision records live in `docs/decisions/` as `DEC-NNN`, not in a
`docs/adr/`. See `docs/agents/domain.md`.

## Session Completion

When ending a work session, complete ALL steps:

1. Run quality gates (tests, linters) if code changed
2. Commit all changes
3. Push — work is NOT complete until `git push origin <branch>` succeeds.
   `origin` is GitHub and is the only remote in this clone.
4. Provide context for next session
