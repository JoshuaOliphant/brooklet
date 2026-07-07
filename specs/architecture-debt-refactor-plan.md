# Plan: Architecture Debt Refactor

Spec: `specs/architecture-debt-refactor-spec.md` (AC-1..AC-6)

## Approach

Behavior-preserving refactor executed under TDD, in dependency order so no two
in-flight tasks fight over the same file. Two independent starting points
(`core/consumer.py` extraction, `contrib/` scaffolding extraction) fan in to a
final coverage/lint sweep and a docs update.

```
T1 (glob catch-up state machine) ─┐
                                   ├─▶ T2 ─▶ T3 ─┐
                                   ┘             ├─▶ T5 ─┐
T4 (contrib shared scaffolding) ───────┬─────────┘       ├─▶ T7 ─▶ T8
                                        └─▶ T6 ───────────┘
```

## Tasks

### T1 — Extract the glob catch-up state machine (AC-2)
`Consumer._catch_up_glob` is D-rated (complexity 23) and spreads mutable
coordination state (`_glob_active_file`, `_glob_active_index`, `_file_positions`)
across the parent object so an outer `finally` can capture mid-file progress on
interruption. Extract this into its own unit that owns that state internally and
exposes what `Consumer` needs (iteration + the offset reached so far), so the
parent object no longer needs to reach into generator-in-flight internals.

### T2 — Extract single-file strategy units + shared offset-persistence (AC-1)
Depends on: T1 (serializes edits to `core/consumer.py`).
Split single-file batch and single-file follow iteration into their own units.
Factor the save-before-assign / `_report_save_failure` offset-persistence
contract (currently duplicated across single-file and glob code paths) into one
shared helper both the single-file and glob (from T1) units call.

### T3 — Recompose `Consumer` as a thin dispatcher (AC-1, AC-2)
Depends on: T1, T2.
Wire the T1/T2 units back through `Consumer.__iter__`/`close()` so the public
constructor signature, `__enter__`/`__exit__`, and iteration behavior are
unchanged. Run the full existing consumer test suite plus
`pytest --cov=src/brooklet/core/consumer.py --cov-report=term-missing` to
confirm 100% coverage before moving on.

### T4 — Extract shared contrib adapter scaffolding (AC-3)
Independent of T1-T3 (different files). Pull the CLI-plugin-registration
(`hookimpl`) and `tee_to_topic` passthrough wiring that `claude_analytics.py`,
`pytest_analytics.py`, and `otel_consumer.py` each currently implement
independently into one shared module. Migrate all three adapters to use it.
Parsing/aggregation logic stays adapter-specific per the spec's V1 scope note.

### T5 — `scan_sessions` reuses `Consumer` glob+follow (AC-4)
Depends on: T3 (stable Consumer API to build on), T4 (serializes edits to
`claude_analytics.py`).
Replace `scan_sessions`'s hand-rolled mtime-polling follow loop with brooklet's
existing glob+follow consumer. Preserve: new-session detection, re-aggregation
on file change, and the `removed=True` signal for sessions leaving the
`--current` window.

### T6 — Reduce `otel_consumer.py` parsing complexity (AC-5)
Depends on: T4 (serializes edits to `otel_consumer.py`).
Decompose the span/metric/log parsing functions so no function exceeds
cyclomatic complexity 10 (radon grade B), with no change in accepted/rejected
events or extracted fields.

### T7 — Final coverage/lint sweep (AC-6)
Depends on: T1, T2, T3, T4, T5, T6.
`uv run pytest --cov=src/brooklet --cov-report=term-missing` at 100% with an
empty Missing column; `uv run ruff check .` clean. Patch any gaps left by
individual tasks' local test runs.

### T8 — Update architecture docs
Depends on: T1, T2, T3, T4, T5, T6.
Update `.claude/rules/architecture.md` and `CLAUDE.md`'s module-responsibility
list to describe the new `core/consumer.py` internal structure and the new
shared `contrib/` scaffolding module.

## AC → Task mapping

| AC | Tasks |
|----|-------|
| AC-1 | T2, T3 |
| AC-2 | T1, T3 |
| AC-3 | T4 |
| AC-4 | T5 |
| AC-5 | T6 |
| AC-6 | T7 |
