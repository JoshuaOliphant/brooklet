# Progress: architecture-debt-refactor

Request: Refactor brooklet's core/consumer.py Consumer god-object into per-mode strategy classes (single-file batch, single-file follow, glob batch, glob+follow) sharing a common offset-persistence mixin, so interrupt-safe offset saving isn't reimplemented per mode. Extract a shared contrib adapter framework that claude_analytics.py, pytest_analytics.py, and otel_consumer.py build on instead of duplicating the 3-layer (parsing -> consumer integration -> CLI plugin) pattern by convention only. Have claude_analytics.scan_sessions reuse Consumer's existing glob+follow mode instead of hand-rolling its own mtime-polling follow loop. Reduce complexity in contrib/otel_consumer.py's parsing functions.

- 2026-07-07T14:23:04+00:00 loop initialized (driver=auto)
- 2026-07-07T14:23:51+00:00 → SPEC: feature branch feature/architecture-debt-refactor created; gh/bd/uv tooling confirmed; no observability tasks needed (library/CLI refactor, no new long-running surface)
- 2026-07-07T14:28:53+00:00 → PLAN: spec written at specs/architecture-debt-refactor-spec.md with 6 ACs covering Consumer split, contrib scaffolding, scan_sessions reuse, otel_consumer complexity, and coverage/lint bar
