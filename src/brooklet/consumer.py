# ABOUTME: Event consumer with batch and follow modes
# ABOUTME: Reads JSONL lines from registered sources with offset tracking

import glob as glob_module
import logging
import warnings
from collections.abc import Iterator
from pathlib import Path

from brooklet.envelope import wrap
from brooklet.offsets import load, save
from brooklet.types import Event, GlobOffset, Mode, SingleFileOffset

logger = logging.getLogger("brooklet")


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

        if follow and mode == "glob":
            msg = "follow mode is not supported for glob sources"
            raise NotImplementedError(msg)

        self._offset: SingleFileOffset | GlobOffset = self._load_offset()

    def _load_offset(self) -> SingleFileOffset | GlobOffset:
        """Load offset from storage, returning the appropriate typed offset."""
        raw = load(self._offsets_dir, self._group, self._topic)
        if self._mode == "glob":
            return GlobOffset.decode(raw)
        return SingleFileOffset.decode(raw)

    def _save_offset(self) -> None:
        """Save the current offset to storage."""
        save(self._offsets_dir, self._group, self._topic, self._offset.encode())

    def __iter__(self) -> Iterator[Event]:
        return self._iterate()

    def _iterate(self):
        """Yield events from the source."""
        if self._mode == "single-file":
            yield from self._iterate_single_file()
        elif self._mode == "glob":
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
        assert isinstance(self._offset, GlobOffset)
        start_file_index = self._offset.file_index
        start_byte_offset = self._offset.byte_offset

        for i, filepath in enumerate(files):
            if i < start_file_index:
                continue

            with open(filepath) as f:
                if i == start_file_index:
                    f.seek(start_byte_offset)

                yield from self._read_lines(f)

                # After reading this file, update offset to next file
                if i == len(files) - 1:
                    # Last file — save position within it
                    self._offset = GlobOffset(file_index=i, byte_offset=f.tell())
                else:
                    # Move to next file, start at byte 0
                    self._offset = GlobOffset(file_index=i + 1, byte_offset=0)

        self._save_offset()

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
            observer.stop()
            observer.join()

    def close(self) -> None:
        """Stop the consumer and save the current offset."""
        self._closed = True

        try:
            # Save offset from current file position if still open
            if self._file_handle is not None and not self._file_handle.closed:
                self._offset = SingleFileOffset(byte_offset=self._file_handle.tell())
                self._save_offset()
        finally:
            if self._observer is not None:
                self._observer.stop()
                self._observer.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
