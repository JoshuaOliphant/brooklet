<!--
Unfiled Forge issue draft. File it with:

    python3 scripts/forge_issue.py create \
        --title 'Tamper-evident log: hash-chained envelope and `brooklet verify`' \
        --body "$(sed '1,/^-->$/d' docs/forge/issue-tamper-evident-log.md)" \
        --label type:feature --label P3

Delete this file once the issue exists on Forge.
-->

# Tamper-evident log: hash-chained envelope and `brooklet verify`

## Problem

Brooklet's append-only guarantee is real at the API level and absent at the
storage level.

At the API level it is enforced by omission: the only write path is
`Stream.produce()` (`src/brooklet/core/stream.py:87`), which opens the active
segment in append mode and writes one line
(`src/brooklet/core/stream.py:160-161`). There is no `delete`, `update`,
`truncate`, or `compact` anywhere in the library.

At the storage level nothing holds. The topic write lock
(`src/brooklet/storage/locking.py`) is `fcntl.flock(LOCK_EX|LOCK_NB)`, which
excludes a second *brooklet producer* and nothing else. Any process that can
reach the stream directory can `open(path, "w")` or `rm` a segment, and no
reader would notice: `Consumer` and `Stream.read()`
(`src/brooklet/core/stream.py:201`) both treat whatever bytes are on disk as
authoritative.

Two consequences worth separating:

1. **Silent tampering.** An edited or truncated segment is indistinguishable
   from a correct one. This matters most where the log's value comes from being
   trustworthy — audit trails, and any setup where the process being logged can
   also write to the log directory.
2. **Silent tail loss.** `produce()` writes and closes without `os.fsync()`.
   Closing flushes to the OS, so a process crash is survivable, but a power
   loss or kernel panic can lose recently appended events. Note the asymmetry:
   every JSON document under `.brooklet/` already goes through
   `atomic_write_text()` (`src/brooklet/storage/atomic.py`), so the *metadata*
   is crash-safe while the *data* is not.

## Proposed work

**A hash-chained envelope.** Add a `_prev` field carrying a digest of the
preceding event, computed and written in `serialize()`
(`src/brooklet/core/envelope.py:106`), which is already the single canonical
assignment point for `_seq`. A reader can then verify that event N follows
event N-1 and that neither has been altered.

**A `brooklet verify <topic>` command.** Walk the topic and report the first
break in the chain, so tampering and truncation surface as a specific event
number rather than as absence. Fits alongside the existing core commands in
`src/brooklet/cli/app.py`.

**`os.fsync()` after append** — worth deciding in the same breath, since it is
the durability half of the same claim, but it carries a per-event cost and may
belong behind a flag rather than on by default.

## Why this needs a decision record

This changes the envelope contract, so it warrants a `docs/decisions/DEC-016-*`
alongside DEC-004 (envelope) and DEC-015 (topic-monotonic `_seq`). The design
questions that record should settle:

- **Chain state.** Where the previous digest is kept between `produce()` calls.
  The sidecar (`src/brooklet/storage/sidecar.py`) already holds per-topic
  producer state with a re-derive-on-staleness path via `derive_next_seq()`
  (`:46`), and is the obvious place — but the re-derivation story is harder
  here, since recovering the last digest means reading the tail of the active
  segment.
- **Segment boundaries.** Whether the chain spans segment rotation or restarts
  per segment. Spanning is stronger; restarting keeps a segment independently
  verifiable and survives external archival of old segments.
- **Externally-produced and legacy lines.** Lines brooklet did not write carry
  no `_prev`. `wrap()` (`src/brooklet/core/envelope.py:25`) already has a
  precedent for this shape of problem — it validates a persisted `_seq` and
  falls back when the contract is violated, rather than crashing. Verification
  needs an equivalent "unchained, not corrupt" verdict so registered external
  sources do not all report as tampered.
- **What is hashed.** The serialized line, or a canonical form of the payload.
  Hashing the raw line is simpler and stricter; it also makes the digest
  sensitive to key ordering and whitespace.
- **Cost.** A digest per produced event, on a library whose selling point is
  being lightweight.

## Scope note

This makes tampering *detectable*, not impossible. Preventing it requires
filesystem permissions or an external store, which is out of scope for
brooklet. The goal is that a reader can tell the difference.
