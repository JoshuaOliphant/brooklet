# ABOUTME: Thin envelope auto-injection for JSONL events
# ABOUTME: Adds _ts, _seq, _src metadata fields without clobbering existing values.
# ABOUTME: _seq is assigned once at produce time and is topic-monotonic on read.

import json
import logging
from datetime import UTC, datetime

from brooklet.core.types import Event

logger = logging.getLogger("brooklet")


def wrap(line: str, seq: int, source: str | None = None) -> Event | None:
    """Wrap a raw JSONL line with envelope metadata.

    Auto-injects _ts (ISO 8601 timestamp) and _seq (sequence number).
    Optionally sets _src (producer identifier). Existing _ts, _seq, and _src
    in the payload are all preserved.

    _seq is topic-monotonic: it is assigned once, at produce time (see
    serialize()), and flows through unchanged on read. The `seq` argument is
    only a fallback for legacy or externally-produced lines that carry no
    persisted _seq — it never overwrites an existing value.

    Args:
        line: A single JSON line string.
        seq: Fallback sequence number, used only when the line has no _seq.
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

    # _seq: preserve the persisted topic-monotonic value (set at produce time).
    # Only fall back to the supplied seq when the line carries no _seq, so a
    # gapless resume reports the true topic position rather than a per-run count.
    event.setdefault("_seq", seq)

    # _src: set from parameter if missing, preserve if present
    if source is not None:
        event.setdefault("_src", source)

    return event


def serialize(event: dict, seq: int, source: str | None = None) -> str:
    """Serialize a dict to a JSON line with envelope fields.

    Inverse of wrap(): takes a dict and returns a JSON string line with
    envelope fields injected. Same semantics as wrap():
    - _ts: set to now() if missing, preserved if present
    - _seq: always set by brooklet (overwrites)
    - _src: set from source param if missing, preserved if present

    Returns a JSON string with trailing newline.
    """
    # _ts: auto-set if missing, preserve if present
    event.setdefault("_ts", datetime.now(UTC).isoformat())

    # _seq: always set by brooklet — canonical offset key
    event["_seq"] = seq

    # _src: set from parameter if missing, preserve if present
    if source is not None:
        event.setdefault("_src", source)

    return json.dumps(event) + "\n"
