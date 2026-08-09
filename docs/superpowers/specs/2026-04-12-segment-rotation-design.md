# Segment Rotation and Concurrent Writer Strategy — Design Spec

**Date:** 2026-04-12
**Decision Record:** DEC-014
**Beads Issue:** brooklet-vi7

## Summary

Add size-based segment rotation to locally-produced topics, replace O(n)
sequence counting with a sidecar cache, enforce single-writer via flock,
and unify consumer offsets on segment-number-based GlobOffset.

## Modules Affected

| Module | Change |
|--------|--------|
| `stream.py` | Segment rotation logic, sidecar read/write, flock acquisition, in-memory segment cache |
| `types.py` | GlobOffset semantics: `file_index` → `segment_number` |
| `consumer.py` | Binary search for starting segment instead of list indexing |
| `registry.py` | `register_local()` stores glob pattern + mode "glob" |
| `offsets.py` | No structural change (stores int); semantic meaning of encoded value changes |
| `envelope.py` | No change |

## Data Layout (After)

```
<stream_dir>/
├── <topic>/
│   ├── data-0001.jsonl       # Segment 1
│   ├── data-0002.jsonl       # Segment 2
│   └── data-0003.jsonl       # Active segment
└── .brooklet/
    ├── sources.json
    ├── seq/
    │   └── <topic>.json      # {"next_seq": N} — cache, not truth
    ├── locks/
    │   └── <topic>.lock      # flock target
    └── offsets/
        └── <group>-<topic>.json
```

## Detailed Design

### 1. Segment Rotation in `produce()`

```
def produce(topic, event, source=None, max_segment_bytes=10_000_000):
    acquire flock on .brooklet/locks/<topic>.lock (LOCK_EX | LOCK_NB)
        → raise on contention

    if topic not in self._segment_cache:
        discover active segment via sorted(glob("data-*.jsonl"))
        if bare data.jsonl exists:
            rename to data-0000.jsonl
            update registry entry
        populate cache: (path, size, segment_number)

    if cached_size >= max_segment_bytes:
        next_number = cached_segment_number + 1
        create data-{next_number:04d}.jsonl
        update cache

    read next_seq from sidecar (or re-derive from last line on miss)
    serialize event with _seq = next_seq
    append to active segment
    update sidecar atomically (next_seq + 1)
    update cached_size += len(line)

    release flock
```

### 2. Sequence Sidecar

- **Location:** `.brooklet/seq/<topic>.json`
- **Format:** `{"next_seq": 42}`
- **Write:** Atomic via temp + `os.replace()` (same pattern as offsets.py)
- **Crash recovery:** If sidecar says `next_seq=40` but last line of active
  segment has `_seq=41`, re-derive to 42. The sidecar is a cache.

### 3. GlobOffset Semantics Change

```python
@dataclass
class GlobOffset:
    segment_number: int = 0    # was: file_index
    byte_offset: int = 0

    def encode(self) -> int:
        return self.segment_number * self._SCALE + self.byte_offset

    @classmethod
    def decode(cls, raw: int) -> "GlobOffset":
        scale = 10**18
        return cls(segment_number=raw // scale, byte_offset=raw % scale)
```

Consumer changes:
- On load, parse segment numbers from filenames: `data-0003.jsonl` → `3`
- Binary search for the segment matching `offset.segment_number`
- If segment is missing (deleted/compacted), start from the next available

### 4. Legacy Migration

Triggered on first `produce()` to a topic that has `data.jsonl` but no
`data-*.jsonl` segments:

1. `os.rename(data.jsonl, data-0000.jsonl)`
2. Update `sources.json`: path → `…/data-*.jsonl`, mode → `"glob"`
3. Existing offsets decode correctly (segment_number=0)

### 5. Locking

- **Lock file:** `.brooklet/locks/<topic>.lock` (created on first produce)
- **Mechanism:** `fcntl.flock(fd, LOCK_EX | LOCK_NB)`
- **On contention:** Raise `BrookletWriteLockError` with clear message
- **Scope:** Held for the duration of produce() call, not across calls
- **Platform:** Local filesystems only (documented constraint)

## Test Impact

~15 produce-specific tests assert `data.jsonl` by name and will need updating.
~50 single-file references are mostly for external topic registration
(unaffected). New tests needed for:

- Rotation at size threshold
- Sequence monotonicity across segments
- Sidecar crash recovery (stale sidecar + newer data)
- Legacy migration (data.jsonl → data-0000.jsonl)
- Lock contention error
- Offset stability across segment deletion
- Consumer catch-up across multiple segments

## Non-Goals

- Time-based or event-count rotation (size-based is sufficient for now)
- Segment compaction/deletion (future work)
- Multi-writer support (explicitly out of scope per single-writer contract)
- Windows support for locking (fcntl is Unix-only; document constraint)
