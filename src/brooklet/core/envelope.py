# ABOUTME: Thin envelope auto-injection for JSONL events
# ABOUTME: Adds _ts, _seq, _src metadata fields without clobbering existing values.
# ABOUTME: _seq is assigned once at produce time and is topic-monotonic on read.

import json
import logging
from datetime import UTC, datetime

from brooklet.core.types import Event

logger = logging.getLogger("brooklet")


def _valid_persisted_seq(value: object) -> bool:
    """Return True if a payload's _seq is a usable topic-monotonic int.

    The EnvelopeMeta contract is `_seq: int`. A persisted _seq is only trusted
    when it is a real int (bool is an int subclass but is not a sequence number,
    so it is rejected). Anything else — strings, floats, None — is treated as no
    usable persisted _seq, so wrap() falls back to the supplied seq.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def wrap(line: str, seq: int, source: str | None = None) -> Event | None:
    """Wrap a raw JSONL line with envelope metadata.

    Auto-injects _ts (ISO 8601 timestamp) and _seq (sequence number).
    Optionally sets _src (producer identifier). Existing _ts and _src in the
    payload are preserved unconditionally; a persisted _seq is preserved only
    when it is a valid int (see below).

    _seq is topic-monotonic: it is assigned once, at produce time (see
    serialize()), and flows through unchanged on read. The `seq` argument is a
    fallback used when a line carries no _seq, or carries one that violates the
    `_seq: int` contract (e.g. a non-int from a legacy/external source) — such a
    line is treated as having no usable persisted _seq. A valid persisted _seq
    is never overwritten.

    Args:
        line: A single JSON line string.
        seq: Fallback sequence number, used when the line has no valid int _seq.
        source: Optional producer identifier.

    Returns:
        Dict with envelope fields added, or None if the line is invalid JSON.
    """
    line = line.strip()
    if not line:
        return None

    try:
        event = json.loads(line)
    except json.JSONDecodeError as e:
        logger.warning("Skipping malformed JSON line (seq=%d): %s — %s", seq, line[:80], e)
        return None

    # _ts: auto-set if missing, preserve if present
    event.setdefault("_ts", datetime.now(UTC).isoformat())

    # _seq: preserve the persisted topic-monotonic value (set at produce time),
    # but only when it is a valid int — a non-int persisted _seq violates the
    # `_seq: int` contract and is replaced by the supplied fallback. Falling back
    # also covers lines that carry no _seq at all (legacy/external sources), so a
    # gapless resume reports the true topic position rather than a per-run count.
    if not _valid_persisted_seq(event.get("_seq")):
        event["_seq"] = seq

    # _src: set from parameter if missing, preserve if present
    if source is not None:
        event.setdefault("_src", source)

    return event


class SeqTracker:
    """Wraps a stream of JSONL lines, supplying a fallback _seq when needed.

    Produced lines already carry a topic-monotonic _seq assigned at produce
    time, which wrap() preserves. This tracker only matters for legacy/external
    lines that carry no valid persisted _seq: it hands wrap() a monotonically
    increasing fallback, then advances its counter to the high-water mark of
    every _seq it has seen. That way a legacy line following a persisted-_seq
    line is numbered *above* the last seen _seq — keeping _seq monotonic and
    collision-free across mixed persisted/legacy sources, rather than restarting
    from position-in-this-read.

    The counter is stateful across calls, so a single tracker must span the
    whole logical read of a topic (e.g. every segment file a glob consumer
    walks), not be recreated per file.
    """

    def __init__(self, source: str | None = None) -> None:
        self._seq = 0
        self._source = source

    def wrap(self, line: str) -> Event | None:
        """Wrap one line, advancing the high-water mark. Returns None if invalid."""
        self._seq += 1
        event = wrap(line, seq=self._seq, source=self._source)
        if event is not None:
            self._seq = max(self._seq, event["_seq"])
        return event


def serialize(event: dict, seq: int, source: str | None = None) -> str:
    """Serialize a dict to a JSON line with envelope fields.

    Inverse of wrap(): takes a dict and returns a JSON string line with
    envelope fields injected. _ts and _src share wrap()'s preserve-if-present
    semantics, but _seq does not — produce and read are deliberately asymmetric:
    - _ts: set to now() if missing, preserved if present
    - _seq: always overwritten here. Produce is the canonical assignment point —
      brooklet owns the topic-monotonic sequence, so the supplied seq wins. (At
      read time wrap() instead preserves a valid persisted _seq; see wrap().)
    - _src: set from source param if missing, preserved if present

    Returns a JSON string with trailing newline.
    """
    # _ts: auto-set if missing, preserve if present
    event.setdefault("_ts", datetime.now(UTC).isoformat())

    # _seq: produce-time canonical assignment — brooklet owns the topic-monotonic
    # offset key, so overwrite any caller-supplied value. (Read-time wrap()
    # preserves a valid persisted _seq instead; the asymmetry is intentional.)
    event["_seq"] = seq

    # _src: set from parameter if missing, preserve if present
    if source is not None:
        event.setdefault("_src", source)

    return json.dumps(event) + "\n"
