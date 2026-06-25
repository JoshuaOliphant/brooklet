# ABOUTME: Crash-safe atomic file writes via tempfile + os.replace
# ABOUTME: Single home for the write-temp-then-rename dance shared across storage

import contextlib
import os
import tempfile
from pathlib import Path


def atomic_write_text(path: str | Path, text: str) -> None:
    """Atomically write text to a file, creating parent dirs as needed.

    Writes to a temp file in the destination's own directory, then os.replace()s
    it into place — so a reader (or a crash) never observes a half-written file;
    it sees either the old contents or the complete new ones. On any failure the
    temp file is cleaned up and the destination is left untouched.

    This is the crash-safety primitive behind every JSON document brooklet
    persists under .brooklet/ (offsets, sidecar seq, sources registry).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    fd_closed = False
    try:
        os.write(fd, text.encode())
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
