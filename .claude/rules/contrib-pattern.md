---
paths:
  - "src/brooklet/contrib/**/*.py"
---

## Contrib Adapter Conventions

Contrib adapters follow the **3-layer pattern**:
1. **Parsing** — Extract structured data from raw JSONL
2. **Consumer integration** — Wire into brooklet's consume/produce pipeline
3. **CLI** — Provide a `scan` subcommand via typer

See existing adapters for reference:
- `contrib/claude_analytics.py` — Claude Code session analytics (`brooklet scout scan`)
- `contrib/pytest_analytics.py` — pytest-reportlog analytics (`brooklet pytest scan`)

Use `/autonomous-sdlc:bdd-spec` to write acceptance criteria before starting new adapters.
