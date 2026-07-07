# Acceptance Criteria: Architecture Debt Refactor

Source request (`.sdlc/state.json`): refactor `core/consumer.py`'s `Consumer` god-object
into per-mode strategy classes sharing a common offset-persistence mixin; extract a
shared `contrib/` adapter framework so `claude_analytics.py`, `pytest_analytics.py`,
and `otel_consumer.py` stop duplicating their "3-layer pattern" by convention only;
have `claude_analytics.scan_sessions` reuse `Consumer`'s existing glob+follow instead
of hand-rolling its own polling loop; reduce complexity in
`contrib/otel_consumer.py`'s parsing functions.

This is a **behavior-preserving refactor**, not new user-facing functionality. The
actor is the brooklet maintainer; "done" means external behavior and the public API
are unchanged while internal structure and complexity improve, verified by the
existing test suite plus new tests for any extracted units.

## AC-1: Single-file Consumer behavior is unchanged after the strategy split

**Given** the existing `Consumer` test suite covering single-file batch and
single-file follow modes
**When** `core/consumer.py`'s mode-dispatch logic is restructured into per-mode
strategy units (exact class/function boundaries are a PLAN/BUILD decision — see
Notes) sharing common offset-persistence and crash-safety behavior
**Then** all existing single-file consumer tests pass unchanged, the public
`Consumer` constructor signature and `__iter__`/`close()`/context-manager behavior
are unchanged, and `uv run pytest --cov=src/brooklet/core/consumer.py
--cov-report=term-missing` shows 100% coverage with zero missing lines

**Edge cases:**
- A `KeyboardInterrupt`/`GeneratorExit` raised mid-read must still persist the
  offset reached so far (save-before-assign contract preserved).
- Offset-save failures (`OSError`) must still be reported via both `logger.warning`
  and stderr (existing `_report_save_failure` contract).

## AC-2: Glob Consumer behavior is unchanged after extracting the catch-up state machine

**Given** the existing `Consumer` test suite covering glob batch and glob+follow
modes, including segment-number-based and positional-index fallback offset tracking
**When** the glob catch-up logic (currently `_catch_up_glob`, D-rated at cyclomatic
complexity 23) is extracted into its own unit with mutable coordination state
(`_glob_active_file`, `_glob_active_index`, `_file_positions`) no longer spread
across the parent `Consumer` object
**Then** all existing glob consumer tests pass unchanged, offset semantics for
segment-numbered files (`data-NNNN.jsonl`) and non-conforming external glob sources
are both preserved, and coverage stays at 100% with zero missing lines

**Edge cases:**
- Glob matches zero files after previously having a non-zero offset → offset resets
  to `(0, 0)` with a logged error (existing behavior).
- A file becomes unreadable mid-catch-up (`OSError` on open) → offset advances past
  it with a logged warning (existing behavior).
- Interruption mid-file during glob catch-up still captures the mid-file byte
  position into the offset via the existing `finally` contract.

## AC-3: A shared contrib adapter framework replaces convention-only duplication

**Given** `claude_analytics.py`, `pytest_analytics.py`, and `otel_consumer.py` each
independently implement the same documented "3-layer pattern" (parsing → consumer
integration → CLI plugin) with duplicated scaffolding
**When** the scaffolding that is genuinely identical across all three adapters
(CLI plugin registration via `hookimpl`, `tee_to_topic` passthrough wiring) is
extracted into a shared module
**Then** all three adapters build on the shared module for that scaffolding, each
adapter's existing test suite passes unchanged, and no adapter-specific parsing
logic is forced into a common abstraction it doesn't naturally share

**Notes:** Full unification of scan/aggregate logic across adapters is out of scope
for V1 — their domains (Claude Code sessions, pytest reports, OTLP spans) differ
enough that forcing shared aggregation logic would be a worse abstraction than three
independent implementations. V1 scope is the scaffolding that is *actually*
identical today, not everything the ABOUTME comments describe as parallel.

## AC-4: `scan_sessions` reduces hand-rolled follow-loop complexity where safe

**Correction (logged as a decision during BUILD):** `scan_sessions` actually
contains *two* independent follow-mode code paths, not one. The `current=False`
(default) glob-mode follow path **already** consumes via `Consumer`'s glob+follow
(`stream.consume("sessions", group="scout-follow", follow=True)`) — that part of
the original AC-4 framing was already true before this feature started. The
`current=True` path's follow loop is the one that hand-rolls mtime-based polling,
and it does so because its behavior contract is fundamentally time-driven, not
event-driven: `tests/test_scout.py::test_current_follow_yields_removal_on_file_age_out`
requires a `removed=True` signal to fire purely from elapsed wall-clock time, with
**no new file write required**. `Consumer`'s glob+follow only wakes callers on new
data (watchdog events / internal poll-and-yield); it has no mechanism to invoke a
caller on a pure timeout with zero new bytes. Forcing the `--current` removal
signal onto `Consumer`'s primitive would either break that test's timeliness
guarantee or require building a new timer-driven abstraction, which is out of
scope. AC-4 is revised accordingly:

**Given** `scan_sessions`'s `current=True, follow=True` branch is a single
104-line inline block (part of why the function is E-rated at complexity 32),
with its active-file resolution logic (mtime filtering by `window_minutes`)
duplicated between the initial scan (before the loop) and the loop's rescans
**When** the `--current` mode (both its one-shot and follow variants) is
decomposed into its own well-named helper(s), and the duplicated active-file
resolution is extracted into one shared helper
**Then** `scan_sessions`'s complexity drops materially (target: no single
function in the file above complexity 15), the polling *mechanism* for
`--current` follow is unchanged (still time-driven, still meets the age-out
test's timeliness guarantee), and all existing `claude_analytics`/`scout` tests
pass unchanged

**Edge cases:**
- `window_minutes=0` (single most-recent-file backward-compat mode) continues to
  work.
- A session file that is deleted while being watched does not crash the follow
  loop.
- The already-correct `current=False` glob+follow path (verified by
  `test_scan_sessions_glob_follow_groups_events_by_session`) is not regressed.

## AC-5: `otel_consumer.py` parsing complexity is reduced

**Given** `otel_consumer.py`'s span/metric/log parsing functions carry a combined
cyclomatic complexity of 65 despite the file having only 3 commits (written dense,
not grown dense)
**When** the parsing functions are decomposed into smaller, single-responsibility
helpers
**Then** no function in the file has cyclomatic complexity above 10 (radon grade B
or better), existing `otel_consumer` tests pass unchanged, and no parsing behavior
(accepted/rejected malformed events, extracted fields) changes

## AC-6: No regression in project-wide coverage or lint

**Given** brooklet's convention of 100% line coverage and a clean `ruff check`
**When** all refactors above are complete
**Then** `uv run pytest --cov=src/brooklet --cov-report=term-missing` reports 100%
coverage with an empty Missing column across the whole package, and
`uv run ruff check .` passes with no findings

---

## Decisions logged

See `.sdlc/decisions.jsonl` (via `sdlc_state.py decide`) for the autonomous scope
calls made while writing this spec — notably: exact class/function boundaries for
AC-1/AC-2 deferred to PLAN/BUILD rather than fixed here, and AC-3 scoped to
genuinely-shared scaffolding rather than full adapter unification.
