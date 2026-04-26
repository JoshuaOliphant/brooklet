---
paths:
  - "src/brooklet/**/*.py"
  - "tests/**/*.py"
---

## 100% Line Coverage Is the Bar

Brooklet ships with 100% line coverage. **Don't lower it.** Every PR that
touches `src/brooklet/` must keep `uv run pytest --cov=src/brooklet` at 100%.
The `concurrency = ["thread"]` setting in `pyproject.toml` is required for
follow-mode code paths that run on background threads — don't remove it.

### Workflow when adding code

1. Write the test first (TDD — see `testing.md`).
2. Run `uv run pytest --cov=src/brooklet --cov-report=term-missing` and
   confirm zero `Missing` lines on the file you touched.
3. If a branch is genuinely unreachable, **delete it** rather than papering
   over with `# pragma: no cover`. Unreachable code is a bug, not a feature.

### When `# pragma: no cover` is acceptable

Only for code that is reachable in production but cannot be exercised in a
test environment. Today the only one in the repo is the `ImportError`
fallback in `contrib/otel.py`, which fires solely when the OTel SDK is
absent — but the test suite needs the SDK installed to cover the SDK path.
Document the reason inline:

```python
except ImportError:  # pragma: no cover — only hit when OTel SDK is absent
    pass
```

Avoid `# pragma: no cover` for:
- defensive `except` blocks against impossible races (delete them)
- "future-proofing" branches that currently can't fire (delete them)
- code that's hard to test because of poor structure — refactor instead
  (extract a helper, inject a dependency, narrow the scope)

### When to delete vs. test

If covering a branch requires more than ~20 lines of mock/monkeypatch
scaffolding, **stop and ask whether the branch should exist at all.**
A branch that needs that much ceremony to reach is usually dead code,
overly defensive error handling, or a sign that the function does too much.

### Verifying

```bash
uv run pytest --cov=src/brooklet --cov-report=term-missing
```

Look for `100%` in the `Cover` column and an empty `Missing` column for
every file under `src/brooklet/`.
