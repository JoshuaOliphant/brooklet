# ABOUTME: Event consumer with batch and follow modes
# ABOUTME: Reads JSONL lines from registered sources with offset tracking

import fnmatch
import glob as glob_module
import logging
import warnings
from collections.abc import Iterator
from pathlib import Path

from brooklet.envelope import wrap
from brooklet.offsets import load, save
from brooklet.types import Event, GlobOffset, Mode, SingleFileOffset

logger = logging.getLogger("brooklet")

_OBSERVER_JOIN_TIMEOUT = 5


class Consumer:
    """Iterator over JSONL events with offset tracking.

    Supports single-file and glob modes. Tracks byte offsets per consumer
    group so consumption resumes where it left off.
    """

    def __init__(
        self,
        path: str,
        mode: Mode,
        group: str,
        topic: str,
        offsets_dir: str | Path,
        source: str | None = None,
        follow: bool = False,
    ) -> None:
        self._path = path
        self._mode: Mode = mode
        self._group = group
        self._topic = topic
        self._offsets_dir = Path(offsets_dir)
        self._source = source
        self._follow = follow
        self._seq = 0
        self._closed = False
        self._file_handle = None
        self._observer = None

        self._offset: SingleFileOffset | GlobOffset = self._load_offset()
        # Per-file byte positions used during glob+follow tailing
        self._file_positions: dict[str, int] = {}

    def _load_offset(self) -> SingleFileOffset | GlobOffset:
        """Load offset from storage, returning the appropriate typed offset."""
        raw = load(self._offsets_dir, self._group, self._topic)
        if self._mode == "glob":
            return GlobOffset.decode(raw)
        return SingleFileOffset.decode(raw)

    def _save_offset(self) -> None:
        """Save the current offset to storage."""
        save(self._offsets_dir, self._group, self._topic, self._offset.encode())

    def _stop_observer(self, observer) -> None:
        """Stop a watchdog observer with a bounded join timeout."""
        observer.stop()
        observer.join(timeout=_OBSERVER_JOIN_TIMEOUT)
        if observer.is_alive():
            observer.daemon = True  # Allow process exit despite hung thread
            logger.error(
                "Watchdog observer did not stop within %ss "
                "(topic=%s, group=%s). Thread will be abandoned.",
                _OBSERVER_JOIN_TIMEOUT,
                self._topic,
                self._group,
            )

    def __iter__(self) -> Iterator[Event]:
        return self._iterate()

    def _iterate(self):
        """Yield events from the source."""
        if self._mode == "single-file":
            yield from self._iterate_single_file()
        elif self._mode == "glob":
            if self._follow:
                yield from self._iterate_glob_follow()
            else:
                yield from self._iterate_glob()
        else:
            raise ValueError(f"Unknown consumer mode: {self._mode!r}")

    def _iterate_single_file(self):
        """Read events from a single JSONL file."""
        path = Path(self._path).expanduser()
        if not path.exists():
            warnings.warn(
                f"Source file does not exist: {path} "
                f"(topic={self._topic!r}, group={self._group!r})",
                stacklevel=2,
            )
            return

        f = open(path)  # noqa: SIM115
        self._file_handle = f
        try:
            assert isinstance(self._offset, SingleFileOffset)
            f.seek(self._offset.byte_offset)

            if self._follow:
                yield from self._iterate_follow(f, path)
            else:
                yield from self._read_lines(f)

            self._offset = SingleFileOffset(byte_offset=f.tell())
            self._save_offset()
        finally:
            self._file_handle = None
            f.close()

    def _read_lines(self, f):
        """Read and yield all available lines from a file handle.

        Uses readline() instead of iteration to keep tell() available.
        """
        while True:
            line = f.readline()
            if not line:
                break
            self._seq += 1
            event = wrap(line, seq=self._seq, source=self._source)
            if event is not None:
                yield event

    def _catch_up_glob(self, files: list[str]) -> None:
        """Read all unread events from glob-matched files, updating offset.

        Shared between batch glob and glob+follow modes. During follow mode,
        also populates _file_positions for subsequent tailing.
        """
        assert isinstance(self._offset, GlobOffset)
        start_file_index = self._offset.file_index
        start_byte_offset = self._offset.byte_offset

        for i, filepath in enumerate(files):
            if i < start_file_index:
                # Still record position for follow mode
                if self._follow:
                    try:
                        self._file_positions[filepath] = Path(filepath).stat().st_size
                    except OSError as e:
                        logger.warning(
                            "Cannot stat skipped file %s (topic=%s, group=%s): %s",
                            filepath,
                            self._topic,
                            self._group,
                            e,
                        )
                continue

            try:
                f = open(filepath)  # noqa: SIM115
            except OSError as e:
                logger.warning(
                    "Cannot open file %s during catch-up (topic=%s, group=%s): %s",
                    filepath,
                    self._topic,
                    self._group,
                    e,
                )
                # Advance offset past this file
                if i == len(files) - 1:
                    self._offset = GlobOffset(file_index=i, byte_offset=0)
                else:
                    self._offset = GlobOffset(file_index=i + 1, byte_offset=0)
                continue

            try:
                if i == start_file_index:
                    f.seek(start_byte_offset)

                yield from self._read_lines(f)

                end_pos = f.tell()
                if self._follow:
                    self._file_positions[filepath] = end_pos

                # After reading this file, update offset to next file
                if i == len(files) - 1:
                    self._offset = GlobOffset(file_index=i, byte_offset=end_pos)
                else:
                    self._offset = GlobOffset(file_index=i + 1, byte_offset=0)
            finally:
                f.close()

    def _iterate_glob(self):
        """Read events across multiple files matched by glob pattern."""
        files = sorted(glob_module.glob(self._path))
        if not files:
            logger.warning(
                "Glob pattern matched no files: %s (topic=%s, group=%s)",
                self._path,
                self._topic,
                self._group,
            )
        yield from self._catch_up_glob(files)
        self._save_offset()

    def _iterate_glob_follow(self):
        """Catch up on existing glob files, then tail for changes and new files."""
        import queue

        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer

        assert isinstance(self._offset, GlobOffset)

        # Phase 1: catch-up on existing files
        files = sorted(glob_module.glob(self._path))
        yield from self._catch_up_glob(files)
        self._save_offset()

        # Phase 2: tail using watchdog on the parent directory
        glob_pattern = self._path
        watch_dir = str(Path(self._path).parent)
        event_queue = queue.Queue()

        class GlobHandler(FileSystemEventHandler):
            def on_modified(self, event):
                if not event.is_directory and fnmatch.fnmatch(event.src_path, glob_pattern):
                    event_queue.put(("modified", event.src_path))

            def on_created(self, event):
                if not event.is_directory and fnmatch.fnmatch(event.src_path, glob_pattern):
                    event_queue.put(("created", event.src_path))

        observer = Observer()
        observer.schedule(GlobHandler(), watch_dir, recursive=False)
        observer.start()
        self._observer = observer

        try:
            while not self._closed:
                try:
                    action, filepath = event_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                # Drain the queue to batch process notifications
                pending = [(action, filepath)]
                while not event_queue.empty():
                    try:
                        pending.append(event_queue.get_nowait())
                    except queue.Empty:
                        break

                for _action, filepath in pending:
                    known_pos = self._file_positions.get(filepath, 0)

                    try:
                        with open(filepath) as f:
                            f.seek(known_pos)
                            yield from self._read_lines(f)
                            self._file_positions[filepath] = f.tell()
                    except OSError as e:
                        logger.warning(
                            "Skipping file %s during glob+follow (topic=%s, group=%s): %s",
                            filepath,
                            self._topic,
                            self._group,
                            e,
                        )
                        continue

                    # Update GlobOffset: find this file's index in the sorted list
                    all_files = sorted(self._file_positions.keys())
                    file_idx = all_files.index(filepath)
                    self._offset = GlobOffset(
                        file_index=file_idx,
                        byte_offset=self._file_positions[filepath],
                    )

                self._save_offset()
        finally:
            self._save_offset()
            self._stop_observer(observer)

    def _iterate_follow(self, f, path):
        """Tail a file using watchdog for filesystem events."""
        import queue

        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer

        event_queue = queue.Queue()

        class Handler(FileSystemEventHandler):
            def on_modified(self, event):
                if Path(event.src_path).resolve() == path.resolve():
                    event_queue.put(True)

        observer = Observer()
        observer.schedule(Handler(), str(path.parent), recursive=False)
        observer.start()
        self._observer = observer

        try:
            # First, read any existing lines
            yield from self._read_lines(f)

            # Then tail for new lines
            while not self._closed:
                try:
                    event_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                # Drain the queue (multiple notifications may have arrived)
                while not event_queue.empty():
                    try:
                        event_queue.get_nowait()
                    except queue.Empty:
                        break

                yield from self._read_lines(f)
        finally:
            self._stop_observer(observer)

    def close(self) -> None:
        """Stop the consumer and save the current offset."""
        self._closed = True

        try:
            # Save offset from current file position if still open
            if self._file_handle is not None and not self._file_handle.closed:
                if isinstance(self._offset, GlobOffset):
                    self._offset = GlobOffset(
                        file_index=self._offset.file_index,
                        byte_offset=self._file_handle.tell(),
                    )
                else:
                    self._offset = SingleFileOffset(byte_offset=self._file_handle.tell())
                self._save_offset()
        finally:
            if self._observer is not None:
                self._stop_observer(self._observer)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
