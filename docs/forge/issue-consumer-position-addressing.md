<!--
Unfiled Forge issue draft. File it with:

    python3 scripts/forge_issue.py create \
        --title 'Address consumer read positions: fork_group() and consume(start_seq=)' \
        --body "$(sed '1,/^-->$/d' docs/forge/issue-consumer-position-addressing.md)" \
        --label type:feature --label P2

Delete this file once the issue exists on Forge.
-->

# Address consumer read positions: fork_group() and consume(start_seq=)

## Problem

A consumer group's read position can only ever be "wherever it last left off".
`Stream.consume()` (`src/brooklet/core/stream.py:176`) takes `topic`, `group`,
and `follow`, then hands the group name to `Consumer`, which loads the saved
offset in `_load_offset()` (`src/brooklet/core/consumer.py:375`). There is no
way for a caller to say "start this group at `_seq` 412" or "start this new
group where that other group is now".

That blocks two things brooklet is otherwise well shaped for:

- **Forking a stream.** Running two readers over the same immutable log from a
  common ancestor point. Independent per-group offsets already make this
  possible in principle — `.brooklet/offsets/<group>-<topic>.json`,
  `src/brooklet/storage/offsets.py:47` and `:78` — but only if a group's
  starting position can be set, and today it cannot.
- **Rewinding a stream.** Putting a group back to an earlier event to re-run
  from a known-good point.

Both operations are, mechanically, just offset writes. `offsets.load()` and
`offsets.save()` already do exactly the right thing and are crash-safe via
`atomic_write_text`. They are simply not reachable from the public API — the
only caller is `Consumer._persist_offset()`
(`src/brooklet/core/consumer.py:393`).

## Second obstacle: offsets are byte positions, not sequence numbers

Even with an API, "rewind to event 412" does not translate directly. The
persisted value is a byte offset (`SingleFileOffset`,
`src/brooklet/core/types.py:28`) or, in glob mode, `segment_number * 10**18 +
byte_offset` (`GlobOffset`, `src/brooklet/core/types.py:48`). Meanwhile `_seq`
is the stable, topic-monotonic event identity assigned at produce time and
preserved on read (DEC-015; `serialize()` at
`src/brooklet/core/envelope.py:106`, `wrap()` at `:25`).

So there is no seq-to-byte mapping. Locating the byte offset of `_seq` 412
currently requires scanning the topic from the beginning.

## Proposed work, in two independently shippable parts

**Part 1 — `Stream.fork_group(topic, src_group, dst_group)`.** Copy one group's
saved offset to another group name. Roughly ten lines over `offsets.load()` /
`offsets.save()`. Needs `validate_safe_name()` on the destination (already
called inside `offsets.save()`) and a decision on whether overwriting an
existing destination group raises or is allowed. This alone delivers
fork-at-current-position, which covers "resume this conversation two different
ways from here" without any new indexing.

**Part 2 — `Stream.consume(..., start_seq=N)`.** Requires a seq-to-position
index. Suggested shape, mirroring the existing sidecar
(`src/brooklet/storage/sidecar.py`, which already caches `next_seq` per topic
and re-derives on staleness): a sparse checkpoint file at
`.brooklet/seq_index/<topic>.json` recording `(segment, byte)` for every Kth
`_seq`, written during `produce()` (`src/brooklet/core/stream.py:87`). A
lookup seeks to the nearest checkpoint at or below the target and scans
forward at most K lines.

Open questions for part 2:

- Index maintenance for **external** registered sources, which brooklet never
  writes and therefore cannot checkpoint at produce time. A bounded forward
  scan with no index may be the honest answer there.
- Whether `start_seq` writes the offset immediately or only on first read.
- Behaviour when `start_seq` names an event that does not exist (past the end
  of the topic, or inside a segment removed by external cleanup).

Part 1 is worth landing on its own; part 2 should not block it.

## Motivation

This is the missing primitive for using brooklet as the durable event-log layer
beneath a self-modifying agent harness, where forking a run from an arbitrary
point and rolling back to a checkpoint are core operations. The log itself is
already append-only with stable event identity; only position addressing is
absent.
