---
name: review
description: Review recent changes against project conventions
allowed-tools: Read Grep Glob Bash
context: fork
---

## Code Review Checklist

Review the most recent changes (`git diff HEAD~1`) against these criteria:

1. **ABOUTME:** Every .py file in src/ starts with 2-line `# ABOUTME:` comment
2. **Tests exist:** New functionality has corresponding tests
3. **No fixture duplication:** Check conftest.py, pytest_fixtures.py, scout_helpers.py
4. **Lint clean:** `uv run ruff check .` passes
5. **Tests pass:** `uv run pytest -v --tb=short` passes
6. **Contrib pattern:** Any contrib adapter follows 3-layer pattern (parsing, consumer integration, CLI)

Grade: **PASS** or **FAIL** with specific issues listed.
Do NOT fix anything — only report.
