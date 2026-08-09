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

## Task Tracking — Forge Issues

This project tracks work as **Forge issues** on
https://forge.smol.ai/joshua-oliphant/brooklet.

Beads is retired here. Ignore any session guidance that says to use beads, `bd`,
TodoWrite, or markdown checklists to track work in this repository — for this
project that guidance is superseded. `.beads/` is kept only as a read-only
archive of historical issues; do not create or update issues in it.

```bash
python3 scripts/forge_issue.py list                      # open work
python3 scripts/forge_issue.py list --state all
python3 scripts/forge_issue.py show 12
python3 scripts/forge_issue.py create --title "..." --body "..." \
    --label type:bug --label P2
python3 scripts/forge_issue.py comment 12 --body "..."
python3 scripts/forge_issue.py close 12 --reason "fixed in abc1234"
python3 scripts/forge_issue.py labels
```

- Open an issue before starting non-trivial work, and cite its number in the
  commit message.
- Give every issue one `type:` label (`type:bug`, `type:feature`, `type:task`)
  and one priority label (`P1`–`P4`). Forge has no separate type or priority
  field, so labels carry that meaning.
- Forge issues have no dependency model. When ordering matters, write it into
  the body ("blocked by #7") rather than expecting the tracker to enforce it.
- Issue bodies must stand alone. Cite concrete `file.py:line` references so a
  reader with no session history can act on one.

New machine setup:

```bash
sf auth git-credential joshua-oliphant/brooklet   # installs the token scripts read
git config forge.repo joshua-oliphant/brooklet    # tells them which repo to use
```

## Forge Platform Notes

**Forge is the primary remote.** `origin` points at
https://forge.smol.ai/joshua-oliphant/brooklet.git; `github` is the backup
mirror. Because `sf` infers the repository from `origin`, this arrangement also
makes bare `sf` commands work without an explicit repository argument.

Keep the two in sync — push both:

```bash
git push origin main     # Forge (primary)
git push github main     # GitHub (backup mirror)
```

Mirroring is manual for now. Automating it as a Forge action has to wait until
Forge Actions actually execute steps rather than simulating them (see below);
until then a Forge-side sync job would report success without pushing anything.

Two sharp edges worth knowing:

- Forge creates **server-side commits on `main`** without being asked. It added
  an "Add MIT license" commit that rewrote the existing `LICENSE`, replacing the
  copyright holder with the Forge username. Check `git log origin/main` after
  the first push and reconcile before assuming the hosts match.
- Forge executes the GitHub workflow files in `.github/workflows/` on its own
  `worker` runner, which **simulates** steps rather than running them: every
  step reports success in under a second. It also ignores `on:` trigger filters
  and `strategy.matrix`. Treat a green Forge Actions run as no evidence that
  anything ran; GitHub Actions remains the real CI.

Forge is alpha and changes often. Check for platform drift with:

```bash
python3 scripts/forge_check_updates.py          # diff live contract vs docs/forge/ snapshots
python3 scripts/forge_check_updates.py --update # accept a new baseline, then commit it
```

Claude Code session transcripts can be attached to Forge commits via
`.claude/hooks/smolforge-transcript.py`. It is **off by default**, because
transcripts on a public Forge repository are readable without authentication.

```bash
python3 .claude/hooks/smolforge-transcript.py --dry-run   # inspect what would be sent
git config --bool forge.transcripts.enabled true          # opt in
```

Only human-readable `text` blocks are published; internal reasoning and tool
output are excluded, because tool output routinely contains file contents.

## Harness Engineering

This project uses Claude Code harness engineering.

- **Hooks** enforce quality gates automatically (lint on edit, tests on stop)
- **Rules** in `.claude/rules/` load context only when touching relevant files
- **Convention tests** in `tests/test_conventions.py` enforce ABOUTME mechanically
- **Decision records** live in `docs/decisions/` (DEC-NNN format)

## Session Completion

When ending a work session, complete ALL steps:

1. Run quality gates (tests, linters) if code changed
2. Commit all changes
3. Push to both remotes — work is NOT complete until `git push origin <branch>`
   (Forge, primary) succeeds. Mirror to the backup with `git push github <branch>`.
4. Provide context for next session
