# ABOUTME: Thin envelope auto-injection for JSONL events
# ABOUTME: Adds _ts, _seq, _src metadata fields without clobbering existing values

import json
import logging
from datetime import UTC, datetime

from brooklet.types import Event

logger = logging.getLogger("brooklet")


def wrap(line: str, seq: int, source: str | None = None) -> Event | None:
    """Wrap a raw JSONL line with envelope metadata.

    Auto-injects _ts (ISO 8601 timestamp) and _seq (sequence number).
    Optionally sets _src (producer identifier). Existing _ts and _src
    in the payload are preserved; _seq is always overwritten by brooklet.

    Args:
        line: A single JSON line string.
        seq: Monotonic sequence number within the topic.
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

    # _seq: always set by brooklet — this is the canonical offset key
    event["_seq"] = seq

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
