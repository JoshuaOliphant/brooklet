# ABOUTME: Topic-level file locking for single-writer enforcement
# ABOUTME: Uses fcntl.flock(LOCK_EX|LOCK_NB) to prevent concurrent producers

import contextlib
import fcntl
import os
from collections.abc import Generator
from pathlib import Path

from brooklet.types import BrookletWriteLockError


def _lock_path(brooklet_dir: Path, topic: str) -> Path:
    """Build the lock file path for a topic.

    Sanitizes '/' in topic names to '--' for safe filenames, matching
    the convention used in offsets.py.
    """
    safe_topic = topic.replace("/", "--")
    return brooklet_dir / "locks" / f"{safe_topic}.lock"


def acquire_topic_lock(brooklet_dir: Path, topic: str) -> int:
    """Acquire an exclusive non-blocking write lock for a topic.

    Creates the .brooklet/locks/ directory and lock file if they don't exist.
    Returns the open file descriptor that holds the lock.

    Raises:
        BrookletWriteLockError: If another process already holds the lock.
    """
    lock_file = _lock_path(brooklet_dir, topic)
    lock_file.parent.mkdir(parents=True, exist_ok=True)

    fd = os.open(str(lock_file), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise BrookletWriteLockError(f"topic {topic!r} is locked by another producer") from None

    return fd


def release_topic_lock(fd: int) -> None:
    """Release a write lock and close the file descriptor."""
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


@contextlib.contextmanager
def topic_lock(brooklet_dir: Path, topic: str) -> Generator[None, None, None]:
    """Context manager that acquires a topic write lock on enter and releases on exit."""
    fd = acquire_topic_lock(brooklet_dir, topic)
    try:
        yield
    finally:
        release_topic_lock(fd)
