# DEC-007: Source Registration

**Status:** Accepted

## Context

Brooklet reads JSONL files that external tools produce. We need a way to map arbitrary file paths to topic names so consumers can refer to them by name.

## Decision

`register()` maps external JSONL file paths to topic names. Registrations persist in `.brooklet/sources.json` for cross-session use.

## Consequences

- External tools don't need to know about brooklet's directory layout
- Topic names are decoupled from file locations
- Registry is the single source of truth for "where does this topic's data live?"
