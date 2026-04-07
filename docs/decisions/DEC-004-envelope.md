# DEC-004: Thin Envelope Metadata

**Status:** Accepted

## Context

Events flowing through brooklet need minimal metadata for coordination (timestamps, sequencing, source tracking) without imposing a heavy schema.

## Decision

Auto-inject `_ts`, `_seq`, `_src` fields on both read (`wrap()`) and write (`serialize()`). Never clobber existing values — if a field is already present, preserve it.

## Consequences

- Lightweight: only 3 fields added
- Non-destructive: external tools' existing fields are preserved
- Consistent: every event has coordination metadata regardless of source
