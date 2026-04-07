# DEC-011: Produce in Core

**Status:** Accepted

## Context

Consumers sometimes need to transform events and re-emit them to new topics. Should `produce()` be in core or a separate module?

## Decision

`produce()` is in core (stream.py). Consumers that transform and re-emit need a clean write path without importing additional modules.

## Consequences

- Clean read-transform-write pipeline within core API
- `stream.produce()` handles serialization, envelope injection, and file appending
- Local topics created by produce are auto-registered (see DEC-012)
