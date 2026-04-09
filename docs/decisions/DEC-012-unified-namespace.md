# DEC-012: Unified Topic Namespace with Auto-Registration

**Status:** Accepted

## Context

With both external (registered) and local (produced) topics, consumers need a single namespace to discover and consume from any topic.

## Decision

Unified topic namespace. `produce()` auto-registers local topics in the registry. External topics are registered via `register()`. Both appear in `topics()`.

## Consequences

- Single `topics()` call lists everything
- No distinction needed at consume time between external and local topics
- Path-style topic names (`scout/stats`) create nested directories automatically
