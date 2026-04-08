# Harness Engineering with Claude Code

A replicable blueprint for making AI coding agents reliable, based on OpenAI's
"harness engineering" principles adapted for Claude Code's feature set.

**Core idea:** Your job shifts from writing code to designing the environment,
constraints, and feedback loops that let Claude Code do reliable work.

This document is a **portable blueprint** — generic enough for any project,
with references to specific plugin skills so future sessions know what to reuse.

---

## The Stack

Every harness principle maps to a Claude Code feature:

| Principle | Claude Code Feature | Why |
|-----------|-------------------|-----|
| Brief instruction file | `CLAUDE.md` (~100 lines) | Table of contents, not encyclopedia |
| Progressive context | `.claude/rules/*.md` with `paths:` frontmatter | Load context only for relevant files |
| Automated lint feedback | `PostToolUse` hook on Edit/Write | Self-corrects on every edit |
| Baseline verification | `SessionStart` hook | Start from known-good state |
| "Done" verification | `Stop` hook (command or prompt-based) | Tests must pass before stopping |
| Guardrails | `PreToolUse` hook + `permissions` | Block destructive ops mechanically |
| Evaluator separation | Forked skills, plugin review agents | Read-only reviewer can't modify code |
| Reusable workflows | Plugin skills (TDD, BDD, review) | Installed once, available across projects |
| Specialized workers | `.claude/agents/*.md` | Domain-specific subagents with scoped tools |
| Convention enforcement | Structural tests in test suite | Conventions become test failures, not prose |
| Decision records | `docs/decisions/` in-repo | Agent-discoverable architectural context |
| Institutional memory | Knowledge capture/retrieve patterns | Cross-session learning |
| Tool integration | `.mcp.json` MCP server configs | Project-level external tools |
| Safe parallelism | Worktree isolation (`isolation: "worktree"`) | Agents work on isolated copies |

---

## Directory Layout

```
project/
├── CLAUDE.md                        # ~100 lines, the map
├── CLAUDE.local.md                  # Personal overrides (gitignored)
├── .mcp.json                        # Project-level MCP servers (optional)
├── .claude/
│   ├── settings.json                # Hooks + permissions (team-shared)
│   ├── settings.local.json          # Personal settings (gitignored)
│   ├── hooks/
│   │   └── protect-critical.sh      # PreToolUse guardrail script
│   ├── agents/                      # Custom subagent definitions (optional)
│   │   └── domain-expert.md         # Specialized agent with scoped tools
│   ├── skills/                      # Project-local skills (only if needed)
│   │   └── my-workflow/SKILL.md     # Use only when no plugin covers it
│   └── rules/
│       ├── architecture.md          # Loads for src/**/*.py
│       ├── testing.md               # Loads for tests/**/*.py
│       └── contrib-pattern.md       # Loads for specific subdirectories
├── docs/
│   ├── harness-engineering.md       # This document
│   └── decisions/
│       ├── README.md                # Decision record index
│       └── DEC-NNN-*.md             # Individual decisions
└── tests/
    └── test_conventions.py          # Mechanical convention enforcement
```

---

## 1. CLAUDE.md — The Map

Keep it under ~100 lines. It should contain:
- Project description (1-2 sentences)
- Architecture overview (module list with one-line descriptions)
- Dev commands (test, lint, format)
- Conventions (brief, link to rules/ for details)
- Pointer to this document for harness details

**Don't put here:** Detailed architecture, full convention docs, decision
rationale. Those go in `.claude/rules/` and `docs/decisions/`.

---

## 2. Rules — Progressive Context

`.claude/rules/*.md` files with `paths:` frontmatter load only when Claude
touches matching files:

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
without burning tokens on irrelevant rules.

**Good rules to start with:**
- Architecture overview for `src/` (module responsibilities, key decisions)
- Testing conventions for `tests/` (fixtures, markers, patterns)
- Domain patterns for specialized directories (contrib, plugins, etc.)

**Reference decisions from rules.** When a rule references a decision record
(`DEC-007`), Claude can follow the link to understand the rationale.

---

## 3. Hooks — Mechanical Feedback Loops

Hooks are **hard enforcement** — deterministic checks that block, inject
context, or force continuation. Configure in `.claude/settings.json`.

### 3.1 Essential Hooks (implement these first)

#### SessionStart — Baseline Verification

```json
{
  "SessionStart": [{
    "matcher": "",
    "hooks": [{
      "type": "command",
      "command": "cd \"$CLAUDE_PROJECT_DIR\" && { <deps-sync> || echo 'WARNING: sync failed'; } && echo '--- Baseline ---' && bash -c 'set -o pipefail; <test-command> | tail -8'",
      "timeout": 120,
      "statusMessage": "Verifying baseline..."
    }]
  }]
}
```

**Key detail:** Use `{ cmd || echo WARNING; }` not `&&` chaining — a sync
failure must not hide the test baseline.

#### PostToolUse — Lint on Every Edit

```json
{
  "PostToolUse": [{
    "matcher": "Edit|Write",
    "hooks": [{
      "type": "command",
      "command": "INPUT=$(cat); FILE=$(echo \"$INPUT\" | jq -r '.tool_input.file_path // empty'); if [ -n \"$FILE\" ] && echo \"$FILE\" | grep -q '\\.py$'; then cd \"$CLAUDE_PROJECT_DIR\" && <linter> \"$FILE\" 2>&1; fi"
    }]
  }]
}
```

**Key detail:** Do NOT add `; exit 0` — let the linter's exit code propagate.

#### Stop — Quality Gate

```json
{
  "Stop": [{
    "matcher": "",
    "hooks": [{
      "type": "command",
      "command": "cd \"$CLAUDE_PROJECT_DIR\" && echo '=== Quality Gate ===' && { <linter> 2>&1 || true; } && echo '---' && bash -c 'set -o pipefail; <test-command> | tail -15'"
    }]
  }]
}
```

**Key detail:** Wrap the linter in `{ cmd || true; }` so lint failures don't
prevent the test suite from running. You want to see both problems at once.

#### PreToolUse — Guardrails

Script that reads JSON from stdin, exits 2 to block. **Must fail closed.**

```bash
#!/bin/bash
set -euo pipefail

# Fail closed: if jq is missing, block everything
if ! command -v jq &>/dev/null; then
  echo "GUARDRAIL ERROR: jq not installed" >&2
  exit 2
fi

INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty') || { echo "GUARDRAIL ERROR: parse failed" >&2; exit 2; }
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty') || true
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty') || true

# Block sensitive files (anchored to path boundaries)
if [ "$TOOL" = "Edit" ] || [ "$TOOL" = "Write" ]; then
  if echo "$FILE" | grep -qE '(^|/)\.env($|\.)|secrets\.'; then
    echo "Blocked: cannot modify sensitive file $FILE" >&2
    exit 2
  fi
fi

# Block destructive git commands (both long and short flags)
if [ "$TOOL" = "Bash" ]; then
  if echo "$CMD" | grep -qE 'git\s+(reset\s+--hard|push\s+(--force|-f)|clean\s+-[a-zA-Z]*f)'; then
    echo "Blocked: destructive git command" >&2
    exit 2
  fi
fi

exit 0
```

### 3.2 Hook Types

Claude Code supports three hook types, each with different strengths:

| Type | How It Works | Best For |
|------|-------------|----------|
| `command` | Runs a shell command, reads stdin JSON, exit code controls behavior | Deterministic checks (lint, test, file guards) |
| `prompt` | Sends context to an LLM for evaluation | Nuanced quality checks (code review, convention adherence) |
| `http` | POSTs JSON to a URL, receives JSON response | External integrations (CI, Slack, custom services) |

**Prompt-based Stop hooks** are powerful for evaluator separation with nuance:

```json
{
  "Stop": [{
    "matcher": "",
    "hooks": [{
      "type": "prompt",
      "prompt": "Review the changes in this session. Check: (1) all new .py files have ABOUTME headers, (2) tests exist for new functionality, (3) no mock implementations. Respond with ok:true if all pass, ok:false with a reason if not."
    }]
  }]
}
```

The LLM evaluator can catch things a shell script can't — like missing test
coverage or convention violations that require understanding the code.

### 3.3 Hook Event Reference

Beyond the 4 essential hooks, Claude Code provides events for the full
lifecycle. Use these when you need more control:

| Event | When It Fires | Use Case |
|-------|--------------|----------|
| `SessionStart` | New session begins | Baseline verification, dep sync |
| `SessionEnd` | Session ending | Cleanup, push reminders |
| `UserPromptSubmit` | Before processing user input | Context injection, input validation |
| `PreToolUse` | Before a tool runs | Guardrails, input modification |
| `PostToolUse` | After a tool runs | Lint feedback, format-on-save |
| `Stop` | Claude tries to stop | Quality gate (tests, lint) |
| `SubagentStop` | A subagent tries to stop | Quality gate for subagents |
| `StopFailure` | Turn ends due to API error | Graceful error handling |
| `PreCompact` | Before context compaction | Save state |
| `PostCompact` | After context compaction | Restore state |
| `Setup` | Via `--init` / `--maintenance` | One-time repo setup |
| `InstructionsLoaded` | CLAUDE.md / rules loaded | React to instruction changes |
| `CwdChanged` | Working directory changes | Environment management (direnv) |
| `FileChanged` | A watched file changes | React to external changes |
| `WorktreeCreate` | Worktree created for isolation | Set up worktree environments |
| `WorktreeRemove` | Worktree cleaned up | Clean up worktree resources |
| `PermissionRequest` | Permission prompt about to show | Auto-approve/deny patterns |
| `Notification` | Notification event | Custom notification routing |

### 3.4 Advanced Hook Features

**Conditional hooks** with `if` — only fire when the tool matches a pattern:

```json
{
  "PreToolUse": [{
    "matcher": "Bash",
    "hooks": [{
      "type": "command",
      "if": "Bash(git push *)",
      "command": "echo 'Reminder: push requires PR review' >&2"
    }]
  }]
}
```

**One-time hooks** with `once: true` — fire only on first match per session.

**Model override** for prompt hooks — use a cheaper model for routine checks:

```json
{ "type": "prompt", "prompt": "...", "model": "haiku" }
```

### 3.5 Hook Anti-Patterns

These are lessons learned. Avoid them:

| Anti-Pattern | Why It's Bad | Fix |
|-------------|-------------|-----|
| `; exit 0` at end of hook | Swallows all errors including jq/cd failures | Let the command's exit code propagate |
| `&&` chaining independent checks | First failure hides all subsequent results | Use `{ cmd \|\| true; }` or `;` between independent checks |
| Security hook that `exit 0` on parse failure | Guardrail degrades to allow-all | `exit 2` on any parse error (fail closed) |
| Unanchored file regex (`.env`) | Matches `environment.py`, `my.env.bak` | Anchor to path boundaries: `(^|/)\.env($|\.)` |
| Only matching long git flags | `git push -f` bypasses `--force` check | Match both: `(--force|-f)` |
| Restrictive allowlist when deny hook exists | Creates friction without adding safety | Use `deny` for catastrophic ops only |

---

## 4. Permissions Strategy

```json
{
  "permissions": {
    "deny": [
      "Bash(rm -rf /*)"
    ]
  }
}
```

**Philosophy:** If you already have a root-level deny hook for dangerous
commands, you don't need a restrictive allowlist — it just slows Claude down.
Use `deny` rules for catastrophic operations. Let Claude work freely otherwise.

Less friction = more iterations per session = better output.

**Permission modes:** Claude Code supports `default`, `acceptEdits`, `plan`,
`bypassPermissions`, and `auto`. For harness engineering, `auto` mode with
good hooks and deny rules is the sweet spot — mechanical guardrails provide
safety while minimizing interruptions.

---

## 5. Plugin Skill Ecosystem

Before creating project-local skills, check what's already available in your
plugin ecosystem. These skills are installed once and work across all projects.

### Development Workflows

| Need | Plugin Skill | What It Does |
|------|-------------|--------------|
| TDD | `/autonomous-sdlc:tdd-workflow` | Red-green-refactor with uv+pytest |
| Acceptance criteria | `/autonomous-sdlc:bdd-spec` | Given/When/Then spec writing |
| Test scaffolding | `/autonomous-sdlc:bdd-generate` | Wire specs into pytest-bdd |
| Verification | `/autonomous-sdlc:verification-stack` | ruff + pytest + type checking |
| Issue tracking | `/autonomous-sdlc:beads-workflow` | bd CLI for issue management |

### Review & Quality

| Need | Plugin Skill | What It Does |
|------|-------------|--------------|
| PR review | `/pr-review-toolkit:review-pr` | Multi-agent review (comments, tests, errors, types) |
| Auto-fix | `/imbue-code-guardian:autofix` | Find and fix code issues automatically |
| Code review | `/superpowers:requesting-code-review` | Request structured code review |
| Pre-commit check | `/superpowers:verification-before-completion` | Verify before claiming done |

### Planning & Design

| Need | Plugin Skill | What It Does |
|------|-------------|--------------|
| Brainstorming | `/superpowers:brainstorming` | Explore intent and design before code |
| Implementation plan | `/superpowers:writing-plans` | Multi-step plan with review |
| Plan execution | `/superpowers:executing-plans` | Execute plan with checkpoints |
| Parallel work | `/superpowers:dispatching-parallel-agents` | Fan out independent tasks |
| Debugging | `/superpowers:systematic-debugging` | Root cause analysis before fixes |

### Knowledge Management

| Need | Plugin Skill | What It Does |
|------|-------------|--------------|
| Capture solutions | `/compound-knowledge:compound-capture` | Save solved problems as YAML files |
| Retrieve knowledge | `/compound-knowledge:compound-retrieve` | Search past solutions before debugging |
| CLAUDE.md audit | `/claude-md-management:claude-md-improver` | Check and improve CLAUDE.md quality |
| Automation audit | `/claude-code-setup:claude-automation-recommender` | Recommend harness improvements |

### When to Create Project-Local Skills

Only when:
- The workflow is unique to this project's domain
- No existing plugin covers the use case
- The workflow needs project-specific paths, conventions, or tools

Project-local skills go in `.claude/skills/`. They support `context: fork`
for evaluator separation, `model:` for model override, `effort:` for effort
level, and `hooks:` for skill-scoped hooks.

---

## 6. Custom Agents

Define specialized subagents in `.claude/agents/*.md`:

```yaml
---
name: domain-expert
description: Analyzes domain models and validates business logic
model: sonnet
tools:
  - Read
  - Grep
  - Glob
disallowedTools:
  - Edit
  - Write
  - Bash
---

You are a domain analysis expert for this project.
Review domain models for correctness and consistency.
Flag potential issues but do not modify any code.
```

**Use agents for:**
- **Evaluator separation** — read-only agents that review but can't modify
- **Domain expertise** — agents with specialized knowledge of a subsystem
- **Parallel investigation** — agents that research independently
- **Isolated execution** — agents with `isolation: "worktree"` work on copies

Agents can also declare `background: true` to run as background tasks.

---

## 7. Structural Convention Tests

Turn conventions into tests. When the Stop hook runs the test suite,
convention violations become failures Claude must fix:

```python
def test_all_py_files_have_header():
    """Every .py file must start with the expected header."""
    src_dir = Path("src/your_package")
    py_files = [p for p in src_dir.rglob("*.py") if p.name != "__init__.py"]
    assert py_files, "No .py files found"

    for path in py_files:
        lines = path.read_text().splitlines()
        assert lines, f"{path}: file is empty, must have header"
        assert len(lines) >= 2, f"{path}: file too short for header"
        assert lines[0].startswith("# ABOUTME:"), f"{path}: missing header"
```

**Convention test design:**
- Skip `__init__.py` consistently (they're package markers, not code files)
- Fail on empty files — don't silently skip them
- Include the offending file path in assertion messages
- Guard that files were actually found (`assert py_files`)

This is the most important shift: **conventions enforced by tests, not prose.**

---

## 8. Decision Records

Put architectural decisions in `docs/decisions/` as markdown files:

```markdown
# DEC-NNN: Title

**Status:** Accepted

## Context
Why this decision was needed.

## Decision
What was decided and how it works.

## Consequences
Trade-offs, both positive and negative.
```

Reference decisions from `.claude/rules/` so Claude finds them when working
on relevant files. This replaces "tribal knowledge" with agent-discoverable
documentation.

---

## 9. Knowledge Management

### Institutional Knowledge

Use the compound-knowledge pattern to capture and retrieve solutions across
sessions:

- **After solving a non-trivial problem:** `/compound-knowledge:compound-capture`
  saves structured YAML files describing the problem, solution, and context
- **Before debugging or implementing:** `/compound-knowledge:compound-retrieve`
  searches past solutions to prevent repeated mistakes

This creates a growing knowledge base that makes every session smarter.

### Auto-Memory

Claude Code automatically saves and recalls memories across sessions. These
persist in `~/.claude/projects/<project>/memory/` and include user
preferences, project context, and feedback.

Auto-memory complements harness engineering — it remembers what worked and
what didn't across sessions, while hooks and tests enforce it mechanically.

---

## 10. MCP Servers

Project-level MCP servers provide external tool integration. Configure in
`.mcp.json` at the project root:

```json
{
  "mcpServers": {
    "my-db": {
      "command": "uvx",
      "args": ["mcp-server-sqlite", "--db-path", "./data.db"]
    }
  }
}
```

MCP servers are useful for:
- Database access (read-only query tools for analysis agents)
- External API integration (project management, monitoring)
- Custom tool servers specific to your domain

---

## Adapting to a New Project

### Phase 1: Foundation (do this first)

1. **Create `CLAUDE.md`** — under 100 lines, the map to everything else
2. **Set up `.claude/settings.json`** with the 4 essential hooks
3. **Write the guardrail script** — fail closed, anchored regexes
4. **Create 2-3 rules** for your key directories

### Phase 2: Quality Layer

5. **Write convention tests** for your project's invariants
6. **Add decision records** for existing architectural decisions
7. **Audit installed plugins** — identify skills to reuse

### Phase 3: Advanced (add as needed)

8. **Create custom agents** for specialized review or analysis
9. **Add prompt-based Stop hooks** for nuanced quality checks
10. **Set up MCP servers** for external integrations
11. **Initialize knowledge capture** for institutional learning

### Customization Checklist

- [ ] Replace test command in SessionStart/Stop hooks
- [ ] Replace lint command in PostToolUse hook
- [ ] Update guardrail patterns for your sensitive files and destructive ops
- [ ] Write path-scoped rules for your directory structure
- [ ] Define structural convention tests
- [ ] Add architectural decision records
- [ ] Audit installed plugin skills — reuse before reinventing
- [ ] Consider prompt-based Stop hooks for nuanced quality checks
- [ ] Set up `.mcp.json` if external tools are needed
- [ ] Run `/claude-code-setup:claude-automation-recommender` for suggestions

---

## Principles

1. **Mechanical over advisory.** Hooks and tests beat prose instructions.
2. **Progressive disclosure.** Load context only when relevant.
3. **Evaluator separation.** Don't let the writer grade its own work.
4. **Fast feedback.** Lint on every edit, test on every stop.
5. **Context is scarce.** Keep CLAUDE.md small, delegate to rules and decisions.
6. **Conventions as tests.** If it matters, make it a test.
7. **Guardrails not guidelines.** Block dangerous operations, don't just warn.
8. **Fail closed.** Security hooks must block on parse errors, not allow.
9. **Reuse over reinvent.** Check plugin skills before writing project-local ones.
10. **Layer incrementally.** Start with essential hooks, add complexity as needed.
