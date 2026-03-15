# ABOUTME: Consumer offset persistence for tracking read positions
# ABOUTME: Stores byte offsets per consumer group in .brooklet/offsets/ directory

import contextlib
import json
import os
import re
import tempfile
from pathlib import Path

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_\-\.]+$")


def _validate_name(value: str, label: str) -> None:
    """Reject names that could cause path traversal or filesystem issues."""
    if not _SAFE_NAME_RE.match(value):
        msg = (
            f"{label} must contain only safe characters "
            f"(alphanumeric, hyphens, underscores, dots), got {value!r}"
        )
        raise ValueError(msg)


def _offset_path(offsets_dir: Path, group: str, topic: str) -> Path:
    """Build the offset file path for a group-topic pair."""
    return offsets_dir / f"{group}-{topic}.json"


def load(offsets_dir: str | Path, group: str, topic: str) -> int:
    """Load the saved byte offset for a consumer group on a topic.

    Returns 0 if no offset has been saved yet.

    Raises:
        ValueError: If the offset file is corrupt or names contain unsafe characters.
    """
    _validate_name(group, "group")
    _validate_name(topic, "topic")

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
    _validate_name(group, "group")
    _validate_name(topic, "topic")

    offsets_dir = Path(offsets_dir)
    offsets_dir.mkdir(parents=True, exist_ok=True)

    path = _offset_path(offsets_dir, group, topic)
    data = json.dumps({"offset": offset})

    # Atomic write: write to temp file in the same directory, then rename
    fd, tmp_path = tempfile.mkstemp(dir=offsets_dir, suffix=".tmp")
    fd_closed = False
    try:
        os.write(fd, data.encode())
        os.close(fd)
        fd_closed = True
        os.replace(tmp_path, path)
    except BaseException:
        if not fd_closed:
            with contextlib.suppress(OSError):
                os.close(fd)
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
