# ABOUTME: Tests for topic-level file locking in produce()
# ABOUTME: Covers lock acquisition, contention error, and lock file creation

import multiprocessing
from pathlib import Path

from brooklet.core.types import BrookletWriteLockError
from brooklet.storage.locking import acquire_topic_lock, release_topic_lock, topic_lock


def test_lock_file_created_on_acquire(tmp_path: Path) -> None:
    """Acquiring a lock creates .brooklet/locks/<topic>.lock"""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    fd = acquire_topic_lock(brooklet_dir, "my-topic")
    try:
        lock_file = brooklet_dir / "locks" / "my-topic.lock"
        assert lock_file.exists(), "Lock file should be created on acquire"
    finally:
        release_topic_lock(fd)


def test_lock_released_after_context(tmp_path: Path) -> None:
    """Lock file exists but is unlocked after context exit, so another acquire succeeds."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    with topic_lock(brooklet_dir, "my-topic"):
        lock_file = brooklet_dir / "locks" / "my-topic.lock"
        assert lock_file.exists()

    # After the context exits, we should be able to re-acquire the lock
    fd = acquire_topic_lock(brooklet_dir, "my-topic")
    release_topic_lock(fd)


def _try_acquire_lock(brooklet_dir: Path, topic: str, result_queue: multiprocessing.Queue) -> None:
    """Helper for subprocess: tries to acquire a lock and reports result."""
    try:
        fd = acquire_topic_lock(brooklet_dir, topic)
        release_topic_lock(fd)
        result_queue.put("acquired")
    except BrookletWriteLockError as e:
        result_queue.put(f"locked:{e}")


def test_concurrent_lock_raises_write_lock_error(tmp_path: Path) -> None:
    """Second acquire on same topic raises BrookletWriteLockError."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    # Hold the lock in this process, then try to acquire from a subprocess
    fd = acquire_topic_lock(brooklet_dir, "contested-topic")
    try:
        result_queue: multiprocessing.Queue = multiprocessing.Queue()
        proc = multiprocessing.Process(
            target=_try_acquire_lock,
            args=(brooklet_dir, "contested-topic", result_queue),
        )
        proc.start()
        proc.join(timeout=5)
        assert proc.exitcode == 0, "Subprocess should exit cleanly"
        result = result_queue.get_nowait()
        assert result.startswith("locked:"), f"Expected lock error, got: {result}"
    finally:
        release_topic_lock(fd)


def test_nested_topic_lock_creates_dirs(tmp_path: Path) -> None:
    """Topics like 'scout/stats' create nested lock dirs with '--' separator."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    fd = acquire_topic_lock(brooklet_dir, "scout/stats")
    try:
        lock_file = brooklet_dir / "locks" / "scout--stats.lock"
        assert lock_file.exists(), "Nested topic should use '--' separator in lock filename"
    finally:
        release_topic_lock(fd)


def test_lock_error_message_includes_topic(tmp_path: Path) -> None:
    """The BrookletWriteLockError message includes the topic name."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    fd = acquire_topic_lock(brooklet_dir, "special-topic")
    try:
        result_queue: multiprocessing.Queue = multiprocessing.Queue()
        proc = multiprocessing.Process(
            target=_try_acquire_lock,
            args=(brooklet_dir, "special-topic", result_queue),
        )
        proc.start()
        proc.join(timeout=5)
        result = result_queue.get_nowait()
        assert "special-topic" in result, f"Error message should include topic name, got: {result}"
    finally:
        release_topic_lock(fd)


def test_topic_lock_context_manager_yields(tmp_path: Path) -> None:
    """topic_lock context manager can be entered and exited without error."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    entered = False
    with topic_lock(brooklet_dir, "ctx-topic"):
        entered = True
        lock_file = brooklet_dir / "locks" / "ctx-topic.lock"
        assert lock_file.exists()
    assert entered


def test_lock_creates_locks_dir_automatically(tmp_path: Path) -> None:
    """The locks directory is created automatically if it doesn't exist."""
    brooklet_dir = tmp_path / ".brooklet"
    brooklet_dir.mkdir()

    locks_dir = brooklet_dir / "locks"
    assert not locks_dir.exists(), "Locks dir should not exist yet"

    fd = acquire_topic_lock(brooklet_dir, "any-topic")
    release_topic_lock(fd)

    assert locks_dir.exists(), "Locks dir should be created by acquire"
