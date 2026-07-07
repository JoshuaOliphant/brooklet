---
paths:
  - "src/brooklet/**/*.py"
---

## Architecture Decisions

Reference these when modifying core modules:

- **DEC-004:** Thin envelope (`_ts`, `_seq`, `_src`) auto-injected on both read AND write
- **DEC-007:** Source registration maps external JSONL paths to topic names
- **DEC-008:** watchdog for filesystem watching in follow mode
- **DEC-009:** Python 3.12+ minimum
- **DEC-011:** `produce()` is in core — consumers that transform and re-emit need a clean write path
- **DEC-012:** Unified topic namespace with auto-registration — `produce()` auto-registers local topics

See `docs/decisions/` for full rationale on each decision.

## Package Layout

Three subpackages, each a clear namespace boundary:

- `core/` — primitives and the main read/write code paths
- `storage/` — everything that persists state under `.brooklet/`
- `cli/` — Typer app, plugin discovery, watch output formatting
- `contrib/` — optional adapters (claude_analytics, pytest_analytics, otel)

## Module Responsibilities

- `core/envelope.py` — `wrap()` on read, `serialize()` on write. Preserves existing `_ts` and `_src`; always sets `_seq`. `SeqTracker` owns the high-water-mark fallback-`_seq` invariant for a topic's whole read (used by both `Consumer` and `Stream.read`).
- `core/types.py` — Shared type definitions (Mode, Event, offset dataclasses, SourceDef).
- `core/stream.py` — Orchestrator. The only module that coordinates the others. `read()` is the offset-less full-scan counterpart to `consume()`.
- `core/consumer.py` — Batch and follow-mode iterators. Uses watchdog for tailing via the `_observe` context manager (single home for observer lifecycle). `Consumer` itself is a thin mode-dispatcher: single-file iteration delegates to `_SingleFileReader` (owns the file handle + running `SingleFileOffset`, including the follow/tailing loop), glob iteration delegates to `_GlobCatchUp` (owns the active-file tracking + running `GlobOffset`, including segment-number binary-search lookup and positional fallback). Both units capture their own mid-file position on interruption (`KeyboardInterrupt`/`GeneratorExit`) internally rather than the parent reaching into generator-in-flight state. `Consumer._persist_offset()` is the one shared "save, report-don't-raise on `OSError`, save-before-assign" implementation both single-file and glob-batch teardown use; glob-follow's per-batch saves keep a different (raise-on-`OSError`) contract and are not routed through it.
- `storage/offsets.py` — Byte offset persistence. One file per consumer group per topic.
- `storage/sidecar.py` — Sequence number sidecar cache for O(1) next-seq lookups.
- `storage/locking.py` — Topic-level file locking (flock) for single-writer enforcement.
- `storage/registry.py` — Maps topic names to file paths. External (registered) and local (produced).
- `storage/segments.py` — Owns the `data-NNNN.jsonl` segment-file naming convention (filename/parse/glob). Producer and consumer both route through it so the format can't drift.
- `storage/atomic.py` — `atomic_write_text()`: crash-safe temp-file-then-`os.replace` write. Single home for the durability primitive used by offsets, sidecar, and registry.
- `storage/names.py` — `validate_safe_name()`: rejects path traversal / unsafe characters in names that become filesystem paths. Shared by registry and offsets.
- `cli/app.py` — Unified CLI entry point. Typer app with core commands and plugin loading. Exposed as `brooklet.cli:main` via the package `__init__` re-export.
- `cli/plugins.py` — Plugin system using pluggy for CLI extensibility. `hookimpl` is imported from here.
- `cli/watch_format.py` — One-line-per-event formatter for `brooklet watch`.
- `__init__.py` — Public API surface. Exports `brooklet.open(path)`.
