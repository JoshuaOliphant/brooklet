---
paths:
  - "src/brooklet/contrib/**/*.py"
---

## Contrib Adapter Conventions

Contrib adapters follow the **3-layer pattern**:
1. **Parsing** — Extract structured data from raw JSONL
2. **Consumer integration** — Wire into brooklet's consume/produce pipeline.
   For `--output` mode (mirror parsed stats into a topic), use the shared
   `contrib/topic_tee.py:tee_to_topic()` passthrough sink rather than hand-rolling
   a produce-and-warn generator.
3. **CLI** — Provide a `scan` subcommand via typer. Use the shared
   `contrib/cli_options.py:StreamDirOption` for the `--stream-dir` option rather
   than retyping the `Annotated[Path | None, typer.Option(...)]` shape — every
   adapter needs the same option, so there is exactly one definition of it.

See existing adapters for reference:
- `contrib/claude_analytics.py` — Claude Code session analytics (`brooklet scout scan`)
- `contrib/pytest_analytics.py` — pytest-reportlog analytics (`brooklet pytest scan`)

Use `/autonomous-sdlc:bdd-spec` to write acceptance criteria before starting new adapters.
