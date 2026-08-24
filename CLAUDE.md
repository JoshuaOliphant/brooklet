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

All 43 active beads issues were triaged against the code before the move: 14 were
carried forward, 3 were already done, and 25 were discarded. The full mapping and
the reasoning for each discard is in `docs/forge/beads-migration.md` — read that
before wondering where an old `brooklet-xxx` id went.

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

**Issues live on Forge; the git remote points at GitHub.** These are two
separate things, and conflating them has caused a wrong "the tracker is empty"
conclusion before. Verified 2026-08-23:

```
$ git remote -v
origin  https://github.com/JoshuaOliphant/brooklet.git (fetch)
origin  https://github.com/JoshuaOliphant/brooklet.git (push)
```

There is **one** remote, and it is GitHub. There is no `forge` or `github`
remote. So `git push origin main` pushes code to GitHub, and the two-remote
mirroring described in earlier revisions of this file does not apply to this
clone — `git push github main` fails with "does not appear to be a git
repository".

Code and issues are therefore split across hosts:

| what | where | how |
| --- | --- | --- |
| code | GitHub, via `origin` | `git push origin main` |
| issues | Forge, `joshua-oliphant/brooklet` | `python3 scripts/forge_issue.py` |

`scripts/forge_issue.py` does not read `origin`. It reads `git config
forge.repo`, so it keeps working regardless of where the remote points. If it
prints "No Forge repo configured", that is the missing config, **not** an empty
tracker:

```bash
git config forge.repo joshua-oliphant/brooklet
```

Note that `sf` does infer its repository from `origin`. With `origin` on GitHub,
bare `sf` commands no longer resolve to the Forge repo and need an explicit
repository argument.

To restore the documented two-remote arrangement, add Forge as the primary and
demote GitHub to a named mirror:

```bash
git remote rename origin github
git remote add origin https://forge.smol.ai/joshua-oliphant/brooklet.git
```

Then push both; mirroring is manual. Automating it as a Forge action has to wait
until Forge Actions actually execute steps rather than simulating them (see
below); until then a Forge-side sync job would report success without pushing
anything.

Two sharp edges worth knowing:

- Forge creates **server-side commits on `main`** without being asked. It added
  an "Add MIT license" commit that rewrote the existing `LICENSE`, replacing the
  copyright holder with the Forge username. Check `git log origin/main` after
  the first push and reconcile before assuming the hosts match.
- Forge reads workflows from `.github/workflows/` — the same directory GitHub
  uses — and honours its own extra keys there. Definitions are only re-read on a
  push to the **default branch**, though existing definitions fire on a push to
  any branch.

### Forge Actions: what actually runs

`runner` accepts exactly `worker` and `container`, and **`worker` is the
default**. The `worker` runner *simulates* steps: it never executes your
commands, string-matches them to emit canned output, and fails randomly about
10% of the time. That is why an `uv sync` and a 478-test suite each "passed" in
under a second, and why a docs-only push once reported `publish` as failed.
**A green Forge Actions run on the default runner is not evidence that anything
ran.** GitHub Actions remains the real CI.

Forge silently drops `strategy`/`matrix`, `env`, `if`, `timeout-minutes` and
`runs-on`, which is why a two-version matrix collapses to one job. Of the
trigger events it only recognises `push`, `pull_request`, `workflow_dispatch`
and `schedule` — and `schedule` is limited to an operator allowlist, so cron
never fires here. `push.branches` works; `push.paths` is parsed and then
ignored; `branches-ignore` is not parsed at all, which makes it *worse* than
omitting it.

**`release` is not a recognised event.** A workflow whose events Forge
recognises none of falls back to push-on-default-branch — which is exactly why
`publish.yml` ran on every push to main. The fix is to declare an event Forge
does know: `publish.yml` now also lists `workflow_dispatch`, which GitHub treats
as a harmless added manual trigger. Keep that in mind before adding any
release-triggered workflow.

The `container` runner is genuinely real — Forge's own repo (`swyx/forge`) runs
its CI on it, with jobs taking one to three minutes and `npm ci` alone taking
~28s. Our workflows get the fake runner purely because they never declare
`runner: container`.

But opting in would not help brooklet yet, because the container is Node-shaped:

- `packages/runner/Dockerfile` pins `docker.io/cloudflare/sandbox:0.12.3`, the
  **default** image variant, which ships no Python interpreter. Upstream also
  builds a `python` variant (CPython 3.11.14), which Forge does not use — and
  3.11 is below this project's 3.12 floor anyway.
- `RUNNER_ALLOWED_HOSTS` contains no `pypi.org`, `files.pythonhosted.org` or
  `astral.sh`, and there is no per-workflow field to add a host. `github.com` *is*
  allowed, so bootstrapping an interpreter might work, but `uv sync` still cannot
  reach the package index.

Container jobs also reject `uses:` steps and never receive Action secrets. So
Python CI on Forge waits on two small upstream changes (a different image tag and
a wider egress allowlist), not on anything in this repository.

Re-checked 2026-08-20 against `@smolai/forge` 0.4.0 GA: still blocked. The
container CI docs describe the standard image as carrying "pinned pnpm and Bun
toolchains" and nothing else, and list `uses:` steps among the cases that "fail
closed". One restriction was added rather than lifted — container workflows
"currently accept public repositories only". GitHub Actions remains the only CI
here that proves anything ran.

Be aware Forge's own workflows are written against GitHub semantics Forge does
not fully implement: `ci.yml` uses `branches-ignore`, which Forge does not parse,
so it fires on every branch, and `git-ingest-contracts.yml` uses `push.paths`,
which Forge parses and ignores.

The Forge **Wiki** is still unavailable to this account. Re-probed 2026-08-20
with the git-credential token, the write endpoints remain allowlisted:

    POST /api/repos/joshua-oliphant/brooklet/wiki/builds -> 403 "Forge Wiki is not enabled for this account"
    POST /api/repos/joshua-oliphant/brooklet/wiki/ask    -> 403 (same)
    GET  /api/repos/joshua-oliphant/brooklet/wiki/search-index -> 404 "Wiki has not been generated"

`POST .../wiki/builds` does exist and is the enable-and-build call, even though it
is absent from `llms.txt`. Read endpoints are reachable; the gate is on writes.

Do not read `GET /api/repos/:owner/:repo/wiki` as an access signal. It returns
500 for this repository *and* for `swyx/forge`, which has a working wiki — the
500 is a server-side bug on that route, not an account verdict. Probing it first
led to a wrong "the allowlist lifted" conclusion on 2026-08-20. **Test access with
`POST .../wiki/builds` and read the error body**, which is specific and honest.

Wiki pages are model-derived from the source, never hand-authored; the only way to
steer them is a committed `.forge/wiki.json`. Docs add that only repository writers
and admins may start builds, capped at 100 manual builds per user per UTC day.

The contract now documents a stateless Streamable HTTP MCP server at
`POST /mcp/wiki`, with tools `read_wiki_structure`, `read_wiki_contents` and
`ask_question`. Public read tools need no token; `ask_question` needs a PAT
carrying the `wiki:ask` scope. It would be the most directly useful thing Forge
has added for agent work here, but it is unreachable until the account is taken
off the Wiki allowlist — `/wiki/ask` returns the same 403 as `/wiki/builds`.

`sf content` is the hand-authored counterpart: immutable Markdown drafts,
anchored review threads, atomic publish, and pointer rollback to a prior
release. It has not been exercised here. Note that `sf` keeps its own keychain
credential separate from the git-credential token: `sf auth status` can report
`authenticated: false` while `scripts/forge_issue.py` and direct API calls work
fine. Run `sf auth login` before assuming the CLI is broken.

### CLI surface

`sf` 0.4.0 GA carries 74 commands, up from 39 in 0.4.0-preview.0. Nothing was
removed, and none of these groups existed when this project moved to Forge:

- `sf pr create|list|view` — pull requests without leaving the terminal.
- `sf agent create|message|events|approve|cancel|profiles` — durable
  repository-agent threads at an explicit execution tier, with an approval gate.
- `sf release enqueue|status|watch` — submit an exact feature SHA to the
  protected production merge queue.
- `sf content …` — the Markdown publishing pipeline described above.
- `sf benchmark` — plan or run an exact-tree Forge/GitHub comparison.

`sf gist import` also still ships, but the Gists API was deleted from the
platform and `GET /api/snippets/:id` replaced it. The command is dead; do not
build on it.

Nothing in this list is used by this repository yet.

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

## Agent skills

### Issue tracker

Forge issues at https://forge.smol.ai/joshua-oliphant/brooklet, driven by
`python3 scripts/forge_issue.py`. Not GitHub Issues. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, unrenamed: `needs-triage`, `needs-info`,
`ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context. Decision records live in `docs/decisions/` as `DEC-NNN`, not in a
`docs/adr/`. See `docs/agents/domain.md`.

## Session Completion

When ending a work session, complete ALL steps:

1. Run quality gates (tests, linters) if code changed
2. Commit all changes
3. Push — work is NOT complete until `git push origin <branch>` succeeds.
   `origin` is GitHub; there is no second remote in this clone. See Forge
   Platform Notes if you restore the two-remote arrangement.
4. Provide context for next session
