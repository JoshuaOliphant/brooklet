# Progress: architecture-debt-refactor

Request: Refactor brooklet's core/consumer.py Consumer god-object into per-mode strategy classes (single-file batch, single-file follow, glob batch, glob+follow) sharing a common offset-persistence mixin, so interrupt-safe offset saving isn't reimplemented per mode. Extract a shared contrib adapter framework that claude_analytics.py, pytest_analytics.py, and otel_consumer.py build on instead of duplicating the 3-layer (parsing -> consumer integration -> CLI plugin) pattern by convention only. Have claude_analytics.scan_sessions reuse Consumer's existing glob+follow mode instead of hand-rolling its own mtime-polling follow loop. Reduce complexity in contrib/otel_consumer.py's parsing functions.

- 2026-07-07T14:23:04+00:00 loop initialized (driver=auto)
- 2026-07-07T14:23:51+00:00 → SPEC: feature branch feature/architecture-debt-refactor created; gh/bd/uv tooling confirmed; no observability tasks needed (library/CLI refactor, no new long-running surface)
- 2026-07-07T14:28:53+00:00 → PLAN: spec written at specs/architecture-debt-refactor-spec.md with 6 ACs covering Consumer split, contrib scaffolding, scan_sessions reuse, otel_consumer complexity, and coverage/lint bar
- 2026-07-07T14:30:52+00:00 → BUILD: plan written, 8 tasks in beads with dependencies; brooklet-10r and brooklet-idh ready
- 2026-07-07T14:47:08+00:00 T1, T4, T6 closed (glob catch-up extracted, shared --stream-dir option, otel_consumer scan_* deduped). Full suite 469 passed, target files 100% coverage. Pre-existing unrelated coverage gap noted in cli/__init__.py (20%, untouched by this feature) — flagging for PR body, not fixing (out of scope). T2 now ready.
- 2026-07-07T15:02:13+00:00 T2, T3 closed. Consumer refactor complete: _GlobCatchUp + _SingleFileReader + unified _persist_offset, thin dispatcher confirmed, 471 tests pass, 100% coverage, no function worse than grade B. T5 (scan_sessions glob+follow reuse) now ready.
