# ABOUTME: Sequence number sidecar cache for O(1) next-seq lookups
# ABOUTME: Stores {"next_seq": N} in .brooklet/seq/<topic>.json with crash recovery

import json
from pathlib import Path

from brooklet.storage.atomic import atomic_write_text


def _sidecar_path(brooklet_dir: Path, topic: str) -> Path:
    """Build the sidecar file path for a topic.

    Sanitizes '/' in topic names to '--' for safe filenames.
    """
    safe_topic = topic.replace("/", "--")
    return brooklet_dir / "seq" / f"{safe_topic}.json"


def read_next_seq(brooklet_dir: Path, topic: str) -> int | None:
    """Read cached next_seq from the sidecar file.

    Returns the cached next_seq value, or None if the sidecar is missing
    or corrupt. A None result means the caller must derive the value from
    the data file using derive_next_seq().
    """
    path = _sidecar_path(brooklet_dir, topic)
    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text())
        return int(data["next_seq"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def write_next_seq(brooklet_dir: Path, topic: str, next_seq: int) -> None:
    """Write next_seq to the sidecar cache atomically.

    Crash-safe via atomic_write_text. Creates .brooklet/seq/ if it doesn't exist.
    """
    path = _sidecar_path(brooklet_dir, topic)
    atomic_write_text(path, json.dumps({"next_seq": next_seq}))


def derive_next_seq(data_path: Path) -> int:
    """Derive next_seq by reading the last valid line of a JSONL data file.

    Reads the last line with a parseable _seq field and returns _seq + 1.
    Scans backward if the final line is corrupt JSON.

    Returns 1 if the file is missing, empty, or has no lines with _seq.
    """
    if not data_path.exists():
        return 1

    content = data_path.read_bytes()
    if not content.strip():
        return 1

    # Split into lines and scan backward for the last valid _seq
    lines = content.decode(errors="replace").splitlines()
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
            seq = event.get("_seq")
            if seq is not None:
                return int(seq) + 1
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

    return 1
