# DEC-004: Thin Envelope Metadata

**Status:** Accepted (the `_seq` semantics below are partly superseded by [DEC-015](DEC-015-topic-monotonic-seq.md))

## Context

Events flowing through brooklet need minimal metadata for coordination (timestamps, sequencing, source tracking) without imposing a heavy schema.

## Decision

Auto-inject `_ts`, `_seq`, `_src` fields on both read (`wrap()`) and write (`serialize()`). Preserve existing `_ts` and `_src` via `setdefault()`. `_seq` is always set by brooklet as the canonical sequence number.

> **Note ([DEC-015](DEC-015-topic-monotonic-seq.md)):** "`_seq` is always set by brooklet" holds only at *produce* time (`serialize()` overwrites). At *read* time, `wrap()` preserves a valid persisted `_seq` (topic-monotonic) and falls back to the supplied counter only for lines lacking a valid int `_seq` — validity is checked explicitly rather than via `setdefault()`, so a non-int `_seq` is treated as absent. The original blanket "always set" rule is retained here as the historical record; see DEC-015 for the current model.

## Consequences

- Lightweight: only 3 fields added
- Non-destructive: external tools' existing fields are preserved
- Consistent: every event has coordination metadata regardless of source
