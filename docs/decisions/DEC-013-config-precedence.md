# DEC-013: Config Precedence Chain for Worktree Isolation

**Status:** Accepted

## Context

When multiple Claude Code instances run in separate git worktrees on the same
repo, they share brooklet state (offsets, registry) if `BROOKLET_DIR` points to
a single location. Environment variables are global — they can't distinguish
between worktrees. A layered config resolution is needed so local settings
override global ones.

## Decision

Add a `resolve_stream_dir()` function in `config.py` with a 5-layer precedence
chain (highest to lowest):

1. `--stream-dir` CLI flag
2. `.brooklet.toml` (walk up parent directories, stop at `.git` boundary)
3. `BROOKLET_DIR` environment variable
4. `~/.config/brooklet/config.toml` (user-wide defaults)
5. Git repo root (`git rev-parse --show-toplevel` equivalent), else cwd

Config files use TOML format (stdlib `tomllib`, no new dependencies). Relative
paths resolve relative to the config file's parent directory. The upward walk
stops at the `.git` boundary (both file and directory forms, covering worktrees).

The CLI no longer uses Typer's `envvar` parameter — all precedence logic runs
through `resolve_stream_dir()`.

## Consequences

**Positive:**
- Parallel brooklet instances in git worktrees are isolated by default
- `.brooklet.toml` gives per-project control without env var pollution
- Smart git-root default means zero-config works for most cases
- Follows the git/ruff/npm convention that users already expect

**Negative:**
- Removing `envvar` from the Typer option means `--help` no longer shows the
  env var association (documented in help text instead)
- One more file to potentially create (`.brooklet.toml`), though it's optional
