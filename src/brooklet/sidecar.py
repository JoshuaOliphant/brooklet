# ABOUTME: Sequence number sidecar cache for O(1) next-seq lookups
# ABOUTME: Stores {"next_seq": N} in .brooklet/seq/<topic>.json with crash recovery

import contextlib
import json
import os
import tempfile
from pathlib import Path


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

    Uses tempfile.mkstemp + os.replace for crash safety — same pattern
    as offsets.py. Creates .brooklet/seq/ directory if it doesn't exist.
    """
    seq_dir = brooklet_dir / "seq"
    seq_dir.mkdir(parents=True, exist_ok=True)

    path = _sidecar_path(brooklet_dir, topic)
    data = json.dumps({"next_seq": next_seq})

    # Atomic write: write to temp file in the same directory, then rename
    fd, tmp_path = tempfile.mkstemp(dir=seq_dir, suffix=".tmp")
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
