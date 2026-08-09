# DEC-015: `_seq` Is Topic-Monotonic, Not Per-Consumer

**Status:** Accepted

## Context

The envelope `_seq` field served two conflicting purposes. Its docstring and
DEC-004 describe it as "the canonical offset key" — implying a topic-monotonic
position stable across readers, like a Kafka/Kinesis offset. But the consumer
assigned it from a per-instance counter (`self._seq += 1`) at read time, and
`wrap()` overwrote any persisted value. Each new `Consumer` therefore restarted
numbering at 1.

After a gapless resume the second run's first delivered event reported `_seq=1`
even though it was the third event ever written to the topic. The `brooklet
watch` gapless-resume demo had to carry a footnote explaining that the `#N`
prefix was a per-run counter, contradicting the documented contract.

Crucially, the topic-monotonic value already exists on disk: `produce()` derives
a monotonic `next_seq` via the sidecar (`storage/sidecar.py`) and persists it
through `serialize()`. The bug was that the consumer destroyed that information
on read.

## Decision

Make `_seq` topic-monotonic by preserving the persisted produce-time value
instead of regenerating one per consumer instance.

- `serialize()` (produce time) remains the single point that assigns `_seq`. It
  is canonical.
- `wrap()` (read time) preserves an existing `_seq`. The `seq` argument becomes
  a fallback used only when a line carries no *usable* `_seq` — the guard is
  stricter than `setdefault()`: preservation requires a real `int`, so a
  persisted `_seq` that is a string, float, `None`, or `bool` violates the
  `_seq: int` contract and is replaced by the fallback. Legacy or
  externally-produced JSONL therefore degrades gracefully rather than crashing
  or propagating a non-int sequence number.
- `Consumer` no longer treats its internal counter as the source of truth; it
  is renamed `_fallback_seq` and only supplies the fallback for lines lacking a
  persisted `_seq`.

This was chosen over the additive option (a separate `_pos`/`_idx` field) because
the value already exists on disk under the `_seq` name; adding a third field
would leave `_seq` permanently mislabeled and add a redundant representation.

## Consequences

- `_seq` is now stable across readers, across restarts, and matches user
  intuition and the Kafka/Kinesis offset convention.
- The `brooklet watch` gapless-resume demo shows `#3 #4` after resume; the
  footnote is removed.
- **Breaking (behavioral):** consumers that relied on `_seq` resetting to 1 per
  read session now see topic-monotonic numbers. This is a pre-1.0 (v0.5.0)
  change made under explicit authorization (see brooklet-a2c). No on-disk schema
  changes; existing data files are read as-is.
