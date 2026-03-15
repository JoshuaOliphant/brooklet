# Brooklet — The SQLite of Event Streaming

Lightweight JSONL-based event streaming library. Adds consumer coordination (offsets, tailing, topic discovery) on top of append-only JSONL files that other tools already produce.

## Architecture

Brooklet is a **consumer coordination layer**, not a message broker. External tools write JSONL; brooklet reads it with offset tracking.

### Core Modules
- `envelope.py` — Metadata injection (_ts, _seq, _src): `wrap()` on read, `serialize()` on write
- `offsets.py` — Byte offset persistence per consumer group
- `registry.py` — Maps topic names to sources; supports external (registered) and local (produced)
- `consumer.py` — Batch and follow-mode iterators over JSONL files
- `stream.py` — Orchestrator: `register()`, `produce()`, `consume()`, `topics()`
- `__init__.py` — Public API: `brooklet.open(path)`

### Key Decisions
- `produce()` is in core — consumers that transform and re-emit need a clean write path (DEC-011)
- Unified topic namespace with auto-registration — `produce()` auto-registers local topics (DEC-012)
- Source registration maps arbitrary external JSONL paths to topic names (DEC-007)
- Thin envelope with `_ts`, `_seq`, `_src` auto-injected on both read and write (DEC-004)
- watchdog for filesystem watching in follow mode (DEC-008)
- Python 3.12+ minimum (DEC-009)
- Path-style topic names (`scout/stats`) create nested directories
- Full design docs at `second_brain/projects/_active/brooklet/`

### Data Layout
```
<stream_dir>/
├── <topic>/
│   └── data.jsonl            # Produced events (local topics)
├── <parent>/<child>/
│   └── data.jsonl            # Path-style nested topics
└── .brooklet/
    ├── sources.json          # Registry (external + local sources)
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

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

```bash
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file
rm -rf directory            # NOT: rm -r directory
```

Other commands: `apt-get -y`, `HOMEBREW_NO_AUTO_UPDATE=1 brew`, `scp -o BatchMode=yes`.

## Session Completion

When ending a work session, complete ALL steps:

1. Run quality gates (tests, linters) if code changed
2. Commit all changes
3. Push to remote — work is NOT complete until `git push` succeeds
4. Provide context for next session
