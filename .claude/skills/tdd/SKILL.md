---
name: tdd
description: Implement a feature using strict TDD workflow
argument-hint: "[feature description]"
allowed-tools: Read Grep Glob Edit Write Bash Agent
---

## TDD Protocol for: $ARGUMENTS

Follow this sequence exactly:

1. **Orient:** Read CLAUDE.md. Run `git log --oneline -5`. Understand current state.
2. **Baseline:** Run `uv run pytest -v --tb=short` — all tests must pass before you start.
3. **Write failing test:** Create a test that specifies the desired behavior. Run it — confirm it fails.
4. **Implement:** Write the minimal code to make the test pass. Nothing more.
5. **Refactor:** Clean up while keeping tests green.
6. **Verify:** Run `uv run pytest -v && uv run ruff check .`
7. **ABOUTME:** Ensure any new .py files start with a 2-line `# ABOUTME:` comment.
8. **Commit:** Descriptive message explaining "why", not "what".
