# ABOUTME: Consumer offset persistence for tracking read positions
# ABOUTME: Stores byte offsets per consumer group in .brooklet/offsets/ directory

import json
import os
import tempfile
from pathlib import Path


def _offset_path(offsets_dir: Path, group: str, topic: str) -> Path:
    """Build the offset file path for a group-topic pair."""
    return offsets_dir / f"{group}-{topic}.json"


def load(offsets_dir: str | Path, group: str, topic: str) -> int:
    """Load the saved byte offset for a consumer group on a topic.

    Returns 0 if no offset has been saved yet.
    """
    path = _offset_path(Path(offsets_dir), group, topic)
    if not path.exists():
        return 0

    data = json.loads(path.read_text())
    return data["offset"]


def save(offsets_dir: str | Path, group: str, topic: str, offset: int) -> None:
    """Persist a byte offset for a consumer group on a topic.

    Uses atomic write (tmp file + os.replace) to prevent corruption.
    Creates parent directories if they don't exist.
    """
    offsets_dir = Path(offsets_dir)
    offsets_dir.mkdir(parents=True, exist_ok=True)

    path = _offset_path(offsets_dir, group, topic)
    data = json.dumps({"offset": offset})

    # Atomic write: write to temp file in the same directory, then rename
    fd, tmp_path = tempfile.mkstemp(dir=offsets_dir, suffix=".tmp")
    try:
        os.write(fd, data.encode())
        os.close(fd)
        os.replace(tmp_path, path)
    except BaseException:
        os.close(fd) if not os.get_inheritable(fd) else None
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
