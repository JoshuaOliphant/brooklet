# Brooklet — The SQLite of Event Streaming

Lightweight JSONL-based event streaming library. Adds consumer coordination (offsets, tailing, topic discovery) on top of append-only JSONL files that other tools already produce.

## Architecture

Brooklet is a **consumer coordination layer**, not a message broker. External tools write JSONL; brooklet reads it with offset tracking.

### Core Modules
- `envelope.py` — Thin metadata injection (_ts, _seq, _src) on read
- `offsets.py` — Byte offset persistence per consumer group
- `registry.py` — Maps external JSONL paths to topic names (.brooklet/sources.json)
- `consumer.py` — Batch and follow-mode iterators over JSONL files
- `stream.py` — Orchestrator tying registry + consumer together
- `__init__.py` — Public API: `brooklet.open(path)`

### Key Decisions
- No `produce()` — external tools write JSONL, brooklet reads (DEC-006)
- Source registration is core — maps arbitrary paths to topic names (DEC-007)
- Thin envelope with `_ts`, `_seq`, `_src` auto-injected on read (DEC-004)
- watchdog for filesystem watching in follow mode (DEC-008)
- Python 3.12+ minimum (DEC-009)
- Full design docs at `second_brain/projects/_active/brooklet/`

### Data Layout
```
<stream_dir>/
└── .brooklet/
    ├── sources.json          # Registered external sources
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
