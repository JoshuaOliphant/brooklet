# Architecture Decision Records

Each decision is referenced in code and CLAUDE.md as `DEC-NNN`.

| ID | Title | Status |
|----|-------|--------|
| DEC-004 | [Thin envelope metadata](DEC-004-envelope.md) | Accepted (`_seq` semantics partly superseded by DEC-015) |
| DEC-007 | [Source registration](DEC-007-source-registration.md) | Accepted |
| DEC-008 | [Watchdog for follow mode](DEC-008-watchdog.md) | Accepted |
| DEC-009 | [Python 3.12+ minimum](DEC-009-python-312.md) | Accepted |
| DEC-011 | [Produce in core](DEC-011-produce-in-core.md) | Accepted |
| DEC-012 | [Unified topic namespace](DEC-012-unified-namespace.md) | Accepted |
| DEC-014 | [Segment rotation and single-writer contract](DEC-014-segment-rotation.md) | Accepted |
| DEC-015 | [`_seq` is topic-monotonic](DEC-015-topic-monotonic-seq.md) | Accepted |

Numbering has gaps (DEC-001–003, 005, 006, 010, 013 have no record). The
surviving IDs are the ones referenced from code and CLAUDE.md.
