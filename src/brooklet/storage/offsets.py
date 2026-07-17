# ABOUTME: Consumer offset persistence for tracking read positions
# ABOUTME: Stores byte offsets per consumer group in .brooklet/offsets/ directory

import json
from pathlib import Path

from brooklet.storage.atomic import atomic_write_text
from brooklet.storage.names import validate_safe_name


def _encode_field(value: str) -> str:
    """Percent-escape a group or topic so it can't collide or nest directories.

    ``validate_safe_name`` restricts names to ``[a-zA-Z0-9_./-]``, so '%' can
    never appear in the input. That makes '%' a safe escape sentinel: the only
    '%XX' sequences in the output come from this function. Escaping '/' keeps
    the name flat inside offsets_dir; escaping '-' reserves it as the field
    delimiter, so the group/topic boundary is unambiguous. The mapping is
    reversible, hence injective per field.
    """
    return value.replace("/", "%2F").replace("-", "%2D")


def _offset_path(offsets_dir: Path, group: str, topic: str) -> Path:
    """Build the offset file path for a group-topic pair.

    Encodes group and topic so that every distinct (group, topic) maps to a
    distinct file that stays directly inside offsets_dir. A single unescaped
    '-' delimits the two encoded fields; neither field can contain a raw '-'
    (they become '%2D'), so the boundary is unambiguous.
    """
    return offsets_dir / f"{_encode_field(group)}-{_encode_field(topic)}.json"


def _legacy_offset_path(offsets_dir: Path, group: str, topic: str) -> Path:
    """Build the pre-injectivity offset path for backward-compatible reads.

    The old scheme was ``f"{group}-{topic.replace('/', '--')}.json"``. It is not
    injective, but for a given (group, topic) it is deterministic, so we can
    still locate a legacy file written for that exact identity and read its
    offset instead of rewinding a live consumer to zero.
    """
    safe_topic = topic.replace("/", "--")
    return offsets_dir / f"{group}-{safe_topic}.json"


def load(offsets_dir: str | Path, group: str, topic: str) -> int:
    """Load the saved byte offset for a consumer group on a topic.

    Returns 0 if no offset has been saved yet.

    Raises:
        ValueError: If the offset file is corrupt or names contain unsafe characters.
    """
    validate_safe_name(group, "group")
    validate_safe_name(topic, "topic")

    offsets_dir = Path(offsets_dir)
    path = _offset_path(offsets_dir, group, topic)
    if not path.exists():
        # Backward compatibility: fall back to a file written under the old,
        # non-injective scheme so upgrading does not rewind live consumers to 0.
        legacy = _legacy_offset_path(offsets_dir, group, topic)
        if not legacy.exists():
            return 0
        path = legacy

    try:
        data = json.loads(path.read_text())
        return data["offset"]
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        raise ValueError(
            f"Corrupt offset file for group={group!r}, topic={topic!r} "
            f"at {path}: {e}. Delete this file to reset the consumer position."
        ) from e


def save(offsets_dir: str | Path, group: str, topic: str, offset: int) -> None:
    """Persist a byte offset for a consumer group on a topic.

    Uses atomic write (tmp file + os.replace) to prevent corruption.
    Creates parent directories if they don't exist.
    """
    validate_safe_name(group, "group")
    validate_safe_name(topic, "topic")

    path = _offset_path(Path(offsets_dir), group, topic)
    atomic_write_text(path, json.dumps({"offset": offset}))
