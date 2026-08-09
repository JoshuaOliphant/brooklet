# DEC-014: Segment Rotation and Concurrent Writer Strategy

**Status:** Accepted

## Context

Before this decision, every locally-produced topic wrote to a single unbounded
`data.jsonl`. That created three problems as topics grew:

1. **O(n) sequence assignment** — `produce()` counted every line in the file to
   determine `_seq`, degrading without bound as the file grew.
2. **No write atomicity** — a bare `open("a")` allowed interleaved lines from
   concurrent writers.
3. **Unbounded files** — high-volume topics grew forever with no rotation.

## Decision

### Single-Writer Contract

Brooklet adopts an explicit single-writer-per-topic contract, matching SQLite's
single-writer model. A **mandatory** `fcntl.flock(LOCK_EX | LOCK_NB)` on
`.brooklet/locks/<topic>.lock` enforces this — if the lock cannot be acquired,
`produce()` raises `BrookletWriteLockError`. This is a safety mechanism, not
advisory: it prevents the TOCTOU race in rotation (check size → create segment)
and protects sequence number integrity. Consumer reads require no locking.

Path-style topic names are flattened for the lock filename (`/` → `--`), so
topic `scout/stats` locks `.brooklet/locks/scout--stats.lock` rather than
creating a nested directory.

Platform scope: local filesystems only (APFS, HFS+, ext4, btrfs). `flock` is
unreliable on NFS and some Docker storage drivers.

### Segment Rotation

Size-based rotation with zero-padded monotonic naming:

- **Naming:** `data-0001.jsonl`, `data-0002.jsonl`, … (zero-padded to a
  4-digit minimum).
- **Trigger:** When the active segment reaches a configurable threshold
  (default `max_segment_bytes=10_000_000`, i.e. 10 MB decimal), `produce()`
  rolls to the next segment before writing.
- **Glob pattern:** `<topic>/data-*.jsonl` — consumers discover segments via
  sorted glob. Lexicographic order equals chronological order due to
  zero-padding. The padding is a 4-digit *minimum*, so that equivalence holds
  up to segment 9999; beyond it the number widens and lexicographic order stops
  tracking segment order. The consumer's binary search assumes the ordering
  holds — it bisects segment numbers in sorted-glob order — so segment counts
  above 9999 are outside the range this design covers.
- **Caching:** The `Stream` instance caches `(active_segment_path,
  current_size_bytes, segment_number)` per topic in memory. Size is incremented
  after each write. Glob discovery only happens the first time a topic is
  produced to on that `Stream` instance — rotation derives the next segment
  path from the cached segment number, so neither path costs a per-write glob.

### Sequence Number Sidecar (Cache, Not Source of Truth)

Replace the whole-topic line-count with a sidecar file at
`.brooklet/seq/<topic>.json` containing `{"next_seq": N}`. As with lock files,
`/` in a path-style topic is flattened to `--`, so `scout/stats` caches to
`.brooklet/seq/scout--stats.json`.

- **Assignment:** Read the sidecar, and also re-derive `next_seq` from the
  active segment; the larger value wins. Write the event, then update the
  sidecar atomically (temp + `os.replace`).
- **Crash recovery:** If the process crashes between writing the event and
  updating the sidecar, `next_seq` is stale. Because the derived value is
  consulted on every write and takes precedence when it is higher, a stale
  sidecar self-heals on the next produce. The sidecar is a cache; the data file
  is always authoritative.
- **Cost:** Deriving scans the *active segment* for the last line carrying a
  `_seq`, so sequence assignment is bounded by `max_segment_bytes` rather than
  by total topic size. Rotation is what bounds the cost — the sidecar removes
  the need to scan retired segments, not the need to read the active one.
- **Cross-segment monotonicity:** The sidecar is per-topic, not per-segment,
  so `_seq` is globally monotonic across all segments.

### Consumer Offset Model

GlobOffset is the unified offset model for all locally-produced topics:

- **Encoding:** Store `segment_number * 10^18 + byte_offset` rather than
  `file_index * 10^18 + byte_offset`. The segment number is parsed from the
  filename (`data-0003.jsonl` → `3`). This makes offsets stable across segment
  deletion or compaction — a positional index would silently point to the wrong
  file if earlier segments were removed.
- **Consumer lookup:** Instead of list indexing, the consumer binary-searches
  the sorted segment list for the starting segment number.
- **External glob sources:** Paths that don't follow the `data-NNNN.jsonl`
  convention have no segment number to parse, so for those `segment_number`
  remains a positional file index. Segment-number semantics apply to local
  topics; positional semantics remain the fallback.
- **Transparent migration:** An existing SingleFileOffset (plain int =
  byte_offset) decodes as GlobOffset with `segment_number=0,
  byte_offset=N`, which correctly points to the first segment (the migrated
  `data-0000.jsonl`).

### Legacy Migration

Existing topics with a bare `data.jsonl` require a one-time migration:

- **File rename:** On first segmented write, `data.jsonl` is renamed to
  `data-0000.jsonl` before creating `data-0001.jsonl`.
- **Registry update:** The `sources.json` entry is updated from
  `{"path": "…/data.jsonl", "mode": "single-file", "type": "local"}` to
  `{"path": "…/data-*.jsonl", "mode": "glob", "type": "local"}`.
- **Offset continuity:** The SingleFileOffset → GlobOffset decode math works
  because `data-0000.jsonl` is segment 0, which is the default
  `segment_number` when decoding a legacy offset.

External registered topics are unaffected — they keep whatever mode and path
the user registered.

## Consequences

- **Bounded file sizes** — segments rotate at a configurable threshold
- **Bounded sequence assignment** — cost no longer grows with total topic size;
  it is capped by the active segment, and the data file stays authoritative
- **Safe single-writer** — lock prevents concurrent writes and rotation races
- **Stable offsets** — segment-number-based encoding survives deletion/compaction
- **Backward compatible** — legacy `data.jsonl` auto-migrates to `data-0000.jsonl`
- **Produce tests assert segment filenames** — they reference `data-NNNN.jsonl`
  rather than a bare `data.jsonl`
- **External topics unaffected** — only locally-produced topics change
- **Local filesystems only** — `flock` constraint matches SQLite's documented scope
