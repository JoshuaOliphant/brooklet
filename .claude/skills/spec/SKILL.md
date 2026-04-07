---
name: spec
description: Create a design spec before implementing a feature
argument-hint: "[feature name]"
allowed-tools: Read Grep Glob Write Bash
---

## Create Design Spec for: $ARGUMENTS

Before any code is written, create a spec at `docs/superpowers/specs/<feature-name>.md`:

1. **Research:** Read existing code to understand current architecture
2. **Write spec** with these sections:
   - Problem statement
   - Proposed approach
   - Files to modify/create
   - Acceptance criteria (testable assertions)
   - Edge cases
3. **Do NOT write any code** — only the spec document
4. Commit the spec with message: `spec: $ARGUMENTS`

The human will review and approve before implementation begins.
