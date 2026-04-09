# DEC-008: Watchdog for Follow Mode

**Status:** Accepted

## Context

Follow mode needs to tail JSONL files and yield new events as they're appended. Polling is wasteful; we need filesystem event notification.

## Decision

Use the `watchdog` library for filesystem watching in follow mode.

## Consequences

- Efficient: OS-level file change notifications instead of polling
- Cross-platform: watchdog handles Linux (inotify), macOS (FSEvents), Windows
- Additional dependency, but well-maintained and widely used
