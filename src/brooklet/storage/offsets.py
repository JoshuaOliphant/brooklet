# ABOUTME: Consumer offset persistence for tracking read positions
# ABOUTME: Stores byte offsets per consumer group in .brooklet/offsets/ directory

import json
from pathlib import Path

from brooklet.storage.atomic import atomic_write_text
from brooklet.storage.names import validate_safe_name


def _offset_path(offsets_dir: Path, group: str, topic: str) -> Path:
    """Build the offset file path for a group-topic pair.

    Sanitizes '/' in topic names to '--' for safe filenames.
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

    path = _offset_path(Path(offsets_dir), group, topic)
    if not path.exists():
        return 0

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
