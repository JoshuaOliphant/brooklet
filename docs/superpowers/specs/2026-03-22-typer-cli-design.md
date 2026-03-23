# Brooklet CLI — Typer + Pluggy Design Spec

**Date**: 2026-03-22
**Status**: Proposed
**Relates to**: DEC-005 (Library-first, CLI wraps it)

## Overview

A unified `brooklet` CLI built with Typer, using pluggy for plugin discovery. Core commands (`produce`, `consume`, `topics`) wrap the library API. Contrib modules (`scout`, `pytest`) register as built-in plugins via the same hookspec interface that third-party packages use.

## Goals

- Single entry point: `brooklet` replaces `brooklet-scout` and `brooklet-pytest`
- Pipe-friendly: `produce` reads stdin, `consume` writes stdout
- Extensible: third-party packages add subcommands via entry points
- Built-in plugins use the same interface as third-party plugins

## Non-Goals

- Commands without library backing (`lag`, `peek`, `seek`, `replay`) are deferred — tracked in beads
- No `brooklet install` command (use `uv add` / `pip install` for plugins)
- No plugin configuration system

## Architecture

### Plugin System (`src/brooklet/plugins.py`)

Hookspec definition and plugin manager setup:

```python
import pluggy

hookspec = pluggy.HookspecMarker("brooklet")
hookimpl = pluggy.HookimplMarker("brooklet")

class BrookletSpec:
    @hookspec
    def brooklet_commands(self, cli) -> None:
        """Register subcommands on the brooklet Typer app."""

def get_plugin_manager():
    pm = pluggy.PluginManager("brooklet")
    pm.add_hookspecs(BrookletSpec)
    # Built-in plugins
    from brooklet.contrib.claude_analytics import ScoutPlugin
    from brooklet.contrib.pytest_analytics import PytestPlugin
    pm.register(ScoutPlugin())
    pm.register(PytestPlugin())
    # Third-party plugins via entry points
    pm.load_setuptools_entrypoints("brooklet")
    return pm
```

### CLI App (`src/brooklet/cli.py`)

Typer application with core commands. Plugin commands are registered at startup.

**Core commands:**

- `brooklet produce <topic>` — Read JSON lines from stdin, write to topic
  - Options: `--source NAME`, `--stream-dir PATH` (default: `.`, env: `BROOKLET_DIR`)
- `brooklet consume <topic> --group <name>` — Read events, write JSON lines to stdout
  - Options: `--follow`, `--stream-dir PATH`
- `brooklet topics` — List registered topics
  - Options: `--stream-dir PATH`, `--json`

**Startup flow:**

1. Create Typer app with core commands
2. Call `get_plugin_manager()`
3. Call `pm.hook.brooklet_commands(cli=app)`
4. Typer runs

### Built-in Plugins

Each contrib module adds a plugin class alongside its existing code. The existing 3-layer architecture (parsing → consumer integration → rendering) is untouched. The plugin class creates Typer commands that call into the existing layers.

**`contrib/claude_analytics.py`** — adds `ScoutPlugin`:

```python
class ScoutPlugin:
    @hookimpl
    def brooklet_commands(self, cli):
        scout_app = typer.Typer(help="Claude Code session analytics")

        @scout_app.command()
        def scan(
            path: str,
            current: bool = False,
            follow: bool = False,
            rich: bool = False,
            window: int = 30,
            output: str | None = None,
        ):
            # Delegates to existing scan_sessions() + render functions
            ...

        cli.add_typer(scout_app, name="scout")
```

**`contrib/pytest_analytics.py`** — adds `PytestPlugin`:

```python
class PytestPlugin:
    @hookimpl
    def brooklet_commands(self, cli):
        pytest_app = typer.Typer(help="pytest-reportlog analytics")

        @pytest_app.command()
        def scan(
            path: str,
            glob: bool = False,
            follow: bool = False,
            output: str | None = None,
        ):
            # Delegates to existing scan_runs() + render functions
            ...

        cli.add_typer(pytest_app, name="pytest")
```

### Third-Party Plugin Interface

A package like `brooklet-duckdb` would:

1. Implement a class with `@hookimpl`:
   ```python
   from brooklet.plugins import hookimpl

   class DuckDBPlugin:
       @hookimpl
       def brooklet_commands(self, cli):
           # Add commands to cli
           ...
   ```

2. Declare the entry point in `pyproject.toml`:
   ```toml
   [project.entry-points.brooklet]
   duckdb = "brooklet_duckdb:DuckDBPlugin"
   ```

### Help Output Grouping

Typer's `rich_help_panel` groups commands in the help output:

```
Core Commands:
  produce   Produce events to a topic from stdin
  consume   Consume events from a topic to stdout
  topics    List registered topics

Plugins:
  scout     Claude Code session analytics
  pytest    pytest-reportlog analytics
```

Core commands use `rich_help_panel="Core Commands"`. Plugin commands register under `rich_help_panel="Plugins"` (or Typer sub-apps appear as their own groups automatically).

## File Changes

### New Files

| File | Purpose |
|------|---------|
| `src/brooklet/cli.py` | Typer app, core commands, plugin loading |
| `src/brooklet/plugins.py` | Hookspec definitions, plugin manager factory |
| `tests/test_cli.py` | Core CLI command tests |
| `tests/test_plugins.py` | Plugin discovery and registration tests |

### Modified Files

| File | Change |
|------|--------|
| `pyproject.toml` | Add `typer`, `pluggy` deps; replace two script entry points with one |
| `src/brooklet/contrib/claude_analytics.py` | Add `ScoutPlugin` class with `@hookimpl` |
| `src/brooklet/contrib/pytest_analytics.py` | Add `PytestPlugin` class with `@hookimpl` |

### Unchanged

- All library modules (`stream.py`, `consumer.py`, `envelope.py`, `offsets.py`, `registry.py`, `types.py`)
- `__init__.py` (library API unaffected)
- All existing tests (business logic tests still pass)

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `typer` | `>=0.9` | CLI framework |
| `pluggy` | `>=1.0` | Plugin discovery |

Both are added to `[project.dependencies]`.

## Testing

### Fixtures to Reuse

From `tests/conftest.py`:
- `tmp_stream_dir` — temporary stream directory
- `sample_jsonl` — sample JSONL file with 3 events
- `brooklet_dir` — temporary `.brooklet` metadata directory

From `tests/pytest_fixtures.py`:
- `SINGLE_RUN_EVENTS`, `ALL_PASS_EVENTS` — pytest report data
- `write_run_file()` — helper to write test JSONL files

### New Test Coverage

**`tests/test_cli.py`** — Core command tests using Typer's `CliRunner`:
- `produce` reads JSON lines from stdin, writes to topic
- `consume` outputs JSON lines to stdout
- `consume --follow` tails for new events
- `topics` lists registered topics
- `topics --json` outputs machine-readable JSON
- `--stream-dir` option overrides default directory
- `BROOKLET_DIR` env var sets stream directory
- Error cases: missing topic, invalid JSON input, nonexistent stream dir
- Pipe roundtrip: produce then consume yields same events

**`tests/test_plugins.py`** — Plugin system tests:
- Built-in plugins (scout, pytest) appear in help output
- Third-party plugin via mock adds a command
- Plugin commands are grouped separately from core commands
- `brooklet scout scan <path>` delegates to existing scan_sessions()
- `brooklet pytest scan <path>` delegates to existing scan_runs()

### Existing Tests — No Changes

- `tests/test_scout.py` — tests `scan_sessions()`, `aggregate_session()` directly
- `tests/test_pytest_analytics.py` — tests `scan_runs()`, `aggregate_run()` directly
- BDD tests in `tests/bdd/` — test acceptance criteria at the library level

## Deferred Work (Beads Tasks)

Commands that need library methods built first:

| Command | Description | Library Method Needed |
|---------|-------------|----------------------|
| `brooklet lag <topic> --group <name>` | Show consumer lag (events behind) | `stream.lag(topic, group)` |
| `brooklet peek <topic> --group <name> --count N` | Preview events without advancing offset | `stream.peek(topic, group, count)` |
| `brooklet seek <topic> --group <name> --to N` | Set consumer offset to position | `stream.seek(topic, group, position)` |
| `brooklet replay <topic> --to-seq N` | Replay events up to a sequence number | `stream.replay(topic, to_seq)` |

Each deferred command needs two beads tasks:
1. Implement the library method on `Stream`
2. Add the CLI command wiring

## Migration

The old entry points (`brooklet-scout`, `brooklet-pytest`) are replaced by:

```toml
[project.scripts]
brooklet = "brooklet.cli:main"
```

Users update from:
- `brooklet-scout <path>` → `brooklet scout scan <path>`
- `brooklet-pytest <path>` → `brooklet pytest scan <path>`

The old `main()` functions remain importable for backward compatibility but are no longer the primary entry points.
