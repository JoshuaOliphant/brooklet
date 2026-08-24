# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root.
- **`docs/decisions/`**: read the decision records that touch the area you're about to work in. This repo names them `DEC-NNN-<slug>.md` and indexes them in `docs/decisions/README.md`. Where a skill says "ADR", it means one of these; there is no `docs/adr/` here and nothing should create one.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

Single-context repo:

```
/
├── CONTEXT.md                          ← does not exist yet; created lazily
├── docs/decisions/
│   ├── README.md                       ← the index
│   ├── DEC-004-envelope.md
│   └── DEC-015-topic-monotonic-seq.md
└── src/brooklet/
```

A new decision record takes the next free `DEC-NNN`. The numbering has gaps (there is no DEC-001 through DEC-003); do not renumber to close them.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

Until `CONTEXT.md` exists, the closest thing to a glossary is the Architecture section of `CLAUDE.md`, which names the subpackages and the coordination-layer framing.

## Flag decision-record conflicts

If your output contradicts an existing decision record, surface it explicitly rather than silently overriding:

> _Contradicts DEC-015 (topic-monotonic `_seq`), but worth reopening because…_
