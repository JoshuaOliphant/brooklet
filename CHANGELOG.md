# Changelog

All notable changes to brooklet are documented here.

## [Unreleased]

### Features

- Add Claude Code harness engineering setup (#7)

## [0.3.0] - 2026-03-24

### Features

- Add --version flag and cat command

## [0.2.1] - 2026-03-24

### Documentation

- Add uv tool install option to README
- Add test/fixture reuse convention to CLAUDE.md
- Add Try It section with fun CLI examples

### Features

- Add --stream-dir to plugin subcommands (#5)

### Miscellaneous

- Update lockfile for v0.2.0
- Bump version to 0.2.1

## [0.2.0] - 2026-03-24

### Features

- Unified brooklet CLI with Typer + pluggy plugin system (#4)

### Miscellaneous

- Bump version to 0.2.0 for unified CLI release

## [0.1.1] - 2026-03-23

### Bug Fixes

- Lower minimum Python to 3.11, bump to v0.1.1

## [0.1.0] - 2026-03-23

### Bug Fixes

- Harden error handling, fix critical bugs from PR review
- Add OSError handling and offset durability to glob+follow
- Add 5s timeout to observer.join() calls to prevent hang on shutdown (#1)
- Stabilize GlobOffset file_index across sessions (#2)
- Ensure stream_dir fixture creates directory, fix AC-PT-5 marker
- Address PR review findings — error handling, validation, test coverage
- Address Qodo PR review — follow mode, offset collision, null duration
- Batch mode reads files directly, no offset tracking

### Documentation

- Add pytest adapter design spec (AC-1 through AC-6)
- Add pytest adapter implementation plan (8 tasks, TDD)
- Add pytest adapter to README and CLAUDE.md
- Add ci_health_check.py example showing brooklet-pytest pipeline

### Features

- Scaffold brooklet project with TDD infrastructure
- Implement brooklet v0.1 API with 43 passing tests
- Add glob+follow mode to Consumer (AC-13 through AC-16)
- Add produce() support — serialize, register_local, path-style topics
- Add scout analytics module and update README
- Rename scout to claude_analytics, add multi-session window and active duration
- Add RunStats dataclass and aggregate_run for pytest analytics
- Add scan_runs consumer integration for pytest analytics
- Add render_run_block, CLI entry point, and brooklet-pytest script
- Add CI workflow, PyPI publish workflow, and pipeline example

### Miscellaneous

- Merge AGENTS.md into CLAUDE.md and remove

### Refactor

- Introduce typed offset dataclasses, Mode literal, and SourceDef

### Testing

- Add BDD acceptance tests for produce (AC-1 through AC-12)
- Add BDD acceptance tests for pytest adapter (AC-PT-1 through AC-PT-6)
- Add integration tests for pytest adapter roundtrip

