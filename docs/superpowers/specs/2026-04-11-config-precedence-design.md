# Config Precedence for Worktree Isolation

**Date:** 2026-04-11
**Status:** Approved (conversation-level design)

## Problem

When multiple Claude Code instances run in separate git worktrees on the same
repo, they share brooklet state (offsets, registry) if `BROOKLET_DIR` points to
a single location. This causes offset conflicts and registry clobbers.

## Design

Add a `resolve_stream_dir()` function with a 5-layer config precedence chain:

```
1. --stream-dir CLI flag          (highest priority)
2. .brooklet.toml (walk up to repo root)
3. BROOKLET_DIR env var
4. ~/.config/brooklet/config.toml
5. git repo root, else "."        (lowest priority)
```

### New module: `config.py`

Single responsibility: resolve the stream directory path from layered config.

- `resolve_stream_dir(cli_flag=None)` — applies the precedence chain
- `find_config_file(filename, start_dir, stop_at_git_root=True)` — walks up
  parent directories looking for a config file, stops at `.git` boundary
- `read_toml_stream_dir(config_path)` — reads `stream_dir` key from a TOML file

### Config file format: `.brooklet.toml`

```toml
stream_dir = "."
```

Minimal. Relative paths resolve relative to the config file's parent directory.

### User-wide config: `~/.config/brooklet/config.toml`

Same format. Provides a default for all projects.

### Smart default (layer 5)

Use `git rev-parse --show-toplevel` to anchor to the repo root. Falls back to
`"."` if not in a git repo. This makes worktree isolation automatic.

### Integration points

- `cli.py`: Replace hardcoded `Path(".")` default with `resolve_stream_dir()`
- `Stream.__init__`: No changes needed (already accepts a path)
- `brooklet.open()`: No changes needed (already accepts a path)

### TOML parsing

Use `tomllib` (stdlib since Python 3.11, brooklet requires 3.12+). No new
dependencies.

## Decision Record

This will be captured as DEC-013.

## Testing Strategy

- Unit tests for `resolve_stream_dir()` covering each precedence layer
- Unit tests for upward directory walk stopping at `.git`
- Integration test: CLI picks up `.brooklet.toml` without `--stream-dir`
- Test that relative paths in config resolve relative to config file location
