# Brooklet — The SQLite of Event Streaming

Lightweight JSONL-based event streaming library. Adds consumer coordination (offsets, tailing, topic discovery) on top of append-only JSONL files that other tools already produce.

## Architecture

Brooklet is a **consumer coordination layer**, not a message broker. External tools write JSONL; brooklet reads it with offset tracking.

Source layout uses three subpackages so directory names communicate intent (the "namespaces are one honking great idea" principle applied to layout — directory paths are an interface for both humans and LLM tools).

- `__init__.py` — Public API: `brooklet.open(path)`

#### `core/` — primitives + main code paths
- `core/envelope.py` — Metadata injection (_ts, _seq, _src): `wrap()` on read, `serialize()` on write, `SeqTracker` for high-water-mark fallback _seq across a topic's read
- `core/types.py` — Shared type definitions (Mode, Event, offset dataclasses, SourceDef)
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
- `cli/app.py` — Unified CLI entry point; Typer app with core commands and plugin loading. Re-exported as `brooklet.cli:main` (the package's `__init__.py` re-exports `app`, `main`, `_watch_impl`).
- `cli/plugins.py` — Plugin system using pluggy for CLI extensibility (`hookimpl` lives here)
- `cli/watch_format.py` — One-line-per-event formatter for `brooklet watch`

### Contrib Adapters (3-layer pattern: parsing → consumer integration → CLI)
- `contrib/claude_analytics.py` — Claude Code session analytics (`brooklet scout scan`)
- `contrib/pytest_analytics.py` — pytest-reportlog test run analytics (`brooklet pytest scan`)
- `contrib/otel.py` — Optional OpenTelemetry instrumentation (tracing + metrics); no-op without SDK
- `contrib/topic_tee.py` — `tee_to_topic()`: shared passthrough sink for scan commands' `--output` mode (produce each stat to a topic, warn-not-raise on failure)
- `contrib/cli_options.py` — `StreamDirOption`: shared `--stream-dir` Typer option definition, used by every adapter's CLI command instead of each retyping the same `Annotated[Path | None, ...]` shape

### Key Decisions
- `produce()` is in core — consumers that transform and re-emit need a clean write path (DEC-011)
- Unified topic namespace with auto-registration — `produce()` auto-registers local topics (DEC-012)
- Source registration maps arbitrary external JSONL paths to topic names (DEC-007)
- Size-based segment rotation with flock single-writer enforcement (DEC-014)
- Thin envelope with `_ts`, `_seq`, `_src` auto-injected on both read and write (DEC-004)
- watchdog for filesystem watching in follow mode (DEC-008)
- Python 3.12+ minimum (DEC-009)
- Path-style topic names (`scout/stats`) create nested directories
- Config precedence: CLI flag > .brooklet.toml > BROOKLET_DIR env > user config > git root (DEC-013)
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
        └── <group>-<topic>.json  # Byte offset per consumer group
```

## Dev Commands

```bash
uv run pytest -v              # Run tests
uv run pytest -v --tb=short   # Run tests with short traceback
uv run ruff check .           # Lint
uv run ruff format .          # Format
```

## Conventions
- All `.py` files start with 2-line `ABOUTME:` comment
- TDD: tests first, then minimal implementation
- Simple over clever — readability is the priority
- Before adding new tests or fixtures, check for existing ones in `tests/conftest.py`, `tests/pytest_fixtures.py`, and `tests/scout_helpers.py` to avoid duplication

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

```bash
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file
rm -rf directory            # NOT: rm -r directory
```

Other commands: `apt-get -y`, `HOMEBREW_NO_AUTO_UPDATE=1 brew`, `scp -o BatchMode=yes`.

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
3. Push to remote — work is NOT complete until `git push` succeeds
4. Provide context for next session
