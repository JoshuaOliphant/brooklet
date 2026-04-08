# Harness Engineering with Claude Code

A replicable design for making AI coding agents reliable, based on OpenAI's
"harness engineering" principles adapted for Claude Code.

**Core idea:** Your job shifts from writing code to designing the environment,
constraints, and feedback loops that let Claude Code do reliable work.

---

## The Stack

Every principle maps to a specific Claude Code feature:

| Principle | Claude Code Feature | Why |
|-----------|-------------------|-----|
| Brief instruction file | `CLAUDE.md` (~100 lines) | Table of contents, not encyclopedia |
| Progressive context | `.claude/rules/*.md` with `paths:` frontmatter | Load rules only when touching relevant files |
| Automated lint feedback | `PostToolUse` hook on Edit/Write | Claude self-corrects on every edit |
| Baseline verification | `SessionStart` hook | Every session starts from known-good state |
| "Done" verification | `Stop` hook | Tests must pass before Claude finishes |
| Guardrails | `PreToolUse` hook + `permissions` | Block destructive operations mechanically |
| Evaluator separation | Skill with `context: fork` | Read-only reviewer can't modify code |
| Sprint contracts | `.claude/skills/` | Reusable workflows invoked via `/skill-name` |
| Convention enforcement | Structural tests in CI | Conventions become test failures, not prose |
| Decision records | `docs/decisions/` in-repo | Agent-discoverable architectural context |
| Session persistence | Auto memory | Claude learns project patterns across sessions |

---

## Directory Layout

```
project/
├── CLAUDE.md                        # ~100 lines, the map
├── CLAUDE.local.md                  # Personal overrides (gitignored)
├── .claude/
│   ├── settings.json                # Hooks + permissions (team-shared)
│   ├── settings.local.json          # Personal settings (gitignored)
│   ├── hooks/
│   │   └── protect-critical.sh      # PreToolUse guardrail
│   ├── skills/
│   │   ├── tdd/SKILL.md             # /tdd [feature] — TDD workflow
│   │   ├── review/SKILL.md          # /review — forked evaluator
│   │   └── spec/SKILL.md            # /spec [name] — design spec first
│   └── rules/
│       ├── architecture.md          # Loads for src/**/*.py
│       ├── testing.md               # Loads for tests/**/*.py
│       └── contrib-pattern.md       # Loads for contrib/**/*.py
├── docs/
│   ├── harness-engineering.md       # This document
│   └── decisions/
│       ├── README.md                # Decision record index
│       └── DEC-NNN-*.md             # Individual decisions
└── tests/
    └── test_conventions.py          # Mechanical enforcement
```

---

## 1. CLAUDE.md — The Map

Keep it under 100 lines. It should contain:
- Project description (1-2 sentences)
- Architecture overview (module list with one-line descriptions)
- Dev commands (test, lint, format)
- Conventions (brief, link to rules/ for details)
- Pointer to this document for harness details

**Don't put here:** Detailed architecture, full convention docs, decision rationale.
Those go in `.claude/rules/` and `docs/decisions/`.

### Path-Scoped Rules

`.claude/rules/*.md` files with `paths:` frontmatter load only when Claude
reads files matching those paths:

```yaml
# .claude/rules/api-layer.md
---
paths:
  - "src/api/**/*.py"
---
API endpoints must validate input with pydantic models.
Return standard error format: {"error": str, "code": int}.
```

This is **progressive disclosure** — Claude gets exactly the context it needs
for the files it's touching, without burning context on irrelevant rules.

---

## 2. Hooks — Mechanical Feedback Loops

Configure in `.claude/settings.json`. These are **hard enforcement** — not
advisory like CLAUDE.md, but deterministic checks that block or inject context.

### SessionStart — Baseline Verification

```json
{
  "hooks": {
    "SessionStart": [{
      "matcher": "",
      "hooks": [{
        "type": "command",
        "command": "cd \"$CLAUDE_PROJECT_DIR\" && <your-deps-sync> && bash -c 'set -o pipefail; <your-test-command> | tail -8'",
        "timeout": 120,
        "statusMessage": "Verifying baseline..."
      }]
    }]
  }
}
```

Output is injected into context. If tests are already failing, Claude knows
immediately — no wasted work building on a broken foundation.

### PostToolUse — Lint on Every Edit

```json
{
  "PostToolUse": [{
    "matcher": "Edit|Write",
    "hooks": [{
      "type": "command",
      "command": "INPUT=$(cat); FILE=$(echo \"$INPUT\" | jq -r '.tool_input.file_path // empty'); if [ -n \"$FILE\" ] && echo \"$FILE\" | grep -q '\\.py$'; then cd \"$CLAUDE_PROJECT_DIR\" && <your-linter> \"$FILE\" 2>&1; fi; exit 0"
    }]
  }]
}
```

Claude sees lint errors immediately after each edit and self-corrects.

### Stop — "Done" Verification

```json
{
  "Stop": [{
    "matcher": "",
    "hooks": [{
      "type": "command",
      "command": "cd \"$CLAUDE_PROJECT_DIR\" && echo '=== Quality Gate ===' && <your-linter> && bash -c 'set -o pipefail; <your-test-command> | tail -15'"
    }]
  }]
}
```

If tests fail, output is injected and Claude **keeps working** instead of
stopping. This is evaluator separation — a mechanical check, not Claude
rating its own work.

### PreToolUse — Guardrails

Script that reads JSON from stdin, exits 2 to block:

```bash
#!/bin/bash
INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty')
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

if [ "$TOOL" = "Edit" ] || [ "$TOOL" = "Write" ]; then
  if echo "$FILE" | grep -qE '\.env|secrets'; then
    echo "Blocked: cannot modify sensitive file $FILE" >&2
    exit 2
  fi
fi

if [ "$TOOL" = "Bash" ]; then
  if echo "$CMD" | grep -qE 'git (reset --hard|push --force|clean -f)'; then
    echo "Blocked: destructive git command" >&2
    exit 2
  fi
fi

exit 0
```

Hooks run **before** permission checks and **cannot be bypassed**.

---

## 3. Permissions — Reduce Friction for Safe Ops

```json
{
  "permissions": {
    "allow": [
      "Skill",
      "Bash(<your-test-command>)",
      "Bash(<your-lint-command>)",
      "Bash(git status *)",
      "Bash(git log *)",
      "Bash(git diff *)",
      "Bash(git add *)",
      "Bash(git commit *)",
      "Bash(git push *)"
    ],
    "deny": [
      "Bash(rm -rf /*)"
    ]
  }
}
```

Less friction = more iterations per session = better output.

---

## 4. Skills — Reusable Workflows

### /tdd — Test-Driven Development

Enforces the exact TDD sequence: orient, baseline, failing test, implement,
refactor, verify, commit. Prevents Claude from skipping straight to
implementation.

### /review — Evaluator Separation

Key feature: `context: fork` runs in an **isolated subagent** that can read
code but not modify it. This is literal evaluator separation — the reviewer
can't be biased by having written the code.

```yaml
---
name: review
context: fork
allowed-tools: Read Grep Glob Bash
---
```

### /spec — Plan Before Build

Creates a design spec that a human reviews before any code is written.
This is the "sprint contract" pattern — agent proposes, human approves,
then implementation begins.

---

## 5. Structural Convention Tests

Turn conventions into tests. When the Stop hook runs the test suite,
convention violations become failures that Claude must fix:

```python
def test_all_py_files_have_header():
    """Every .py file must start with the expected header."""
    for path in Path("src").rglob("*.py"):
        first_line = path.read_text().splitlines()[0]
        assert first_line.startswith("# EXPECTED_PREFIX"), f"{path} missing header"
```

This is the most important shift: **conventions enforced by tests, not prose.**

---

## 6. Decision Records

Put architectural decisions in `docs/decisions/` as markdown files. Each one
has Context, Decision, and Consequences sections. Reference them from
`.claude/rules/` so Claude finds them when working on relevant files.

This replaces "tribal knowledge" with agent-discoverable documentation.

---

## Adapting to a New Project

1. **Copy the `.claude/` skeleton** (settings.json, hooks/, skills/, rules/)
2. **Customize hooks** — replace test/lint commands with your project's
3. **Write 3-5 rules** for your project's key areas
4. **Create convention tests** for your project's invariants
5. **Add decision records** for existing architectural decisions
6. **Keep CLAUDE.md under 100 lines** — use it as the map

### Customization Checklist

- [ ] Replace test command in SessionStart/Stop hooks
- [ ] Replace lint command in PostToolUse hook
- [ ] Update permissions.allow with your safe commands
- [ ] Write path-scoped rules for your directory structure
- [ ] Define your project's structural convention tests
- [ ] Add your architectural decisions to docs/decisions/
- [ ] Create project-specific skills for your workflows

---

## Principles

1. **Mechanical over advisory.** Hooks and tests beat prose instructions.
2. **Progressive disclosure.** Load context only when relevant.
3. **Evaluator separation.** Don't let the writer grade its own work.
4. **Fast feedback.** Lint on every edit, test on every stop.
5. **Context is scarce.** Keep CLAUDE.md small, delegate to rules.
6. **Conventions as tests.** If it matters, make it a test.
7. **Guardrails not guidelines.** Block dangerous operations, don't just warn.
