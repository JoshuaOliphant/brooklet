---
paths:
  - "tests/**/*.py"
---

## Testing Conventions

- **Check for existing fixtures** in `tests/conftest.py`, `tests/pytest_fixtures.py`,
  and `tests/scout_helpers.py` before adding new ones — avoid duplication.
- BDD feature tags `@ac-1` through `@ac-29` map to acceptance criteria in `tests/bdd/features/`.
- Tags `@ac-pt-1` through `@ac-pt-6` are pytest adapter acceptance criteria.
- BDD features live in `tests/bdd/features/`.
- Use `uv run pytest -v --tb=short` to run tests.
- TDD: write the failing test first, then implement.
