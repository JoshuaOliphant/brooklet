---
paths:
  - "src/brooklet/*.py"
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

## Module Responsibilities

- `envelope.py` — `wrap()` on read, `serialize()` on write. Preserves existing `_ts` and `_src`; always sets `_seq`.
- `offsets.py` — Byte offset persistence. One file per consumer group per topic.
- `registry.py` — Maps topic names to file paths. Two kinds: external (registered) and local (produced).
- `consumer.py` — Batch and follow-mode iterators. Uses watchdog for tailing.
- `stream.py` — Orchestrator. The only module that coordinates the others.
- `cli.py` — Unified CLI entry point. Typer app with core commands and plugin loading.
- `types.py` — Shared type definitions (Mode, Event, offset dataclasses, SourceDef).
- `plugins.py` — Plugin system using pluggy for CLI extensibility.
- `__init__.py` — Public API surface. Exports `brooklet.open(path)`.
