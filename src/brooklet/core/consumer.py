# ABOUTME: Event consumer with batch and follow modes
# ABOUTME: Reads JSONL lines from registered sources with offset tracking

import bisect
import contextlib
import fnmatch
import glob as glob_module
import logging
import re
import sys
import warnings
from collections.abc import Iterator
from pathlib import Path

from brooklet.contrib import otel
from brooklet.core.envelope import wrap
from brooklet.core.types import Event, GlobOffset, Mode, SingleFileOffset
from brooklet.storage.offsets import load, save

logger = logging.getLogger("brooklet")

_OBSERVER_JOIN_TIMEOUT = 5

# Matches local segment filenames produced by brooklet (e.g. data-0003.jsonl)
_SEGMENT_RE = re.compile(r"data-(\d+)\.jsonl$")


def _parse_segment_number(filepath: str) -> int | None:
    """Extract segment number from a data-NNNN.jsonl filename.

    Returns None for files that don't follow the segment naming convention.
    """
    m = _SEGMENT_RE.search(filepath)
    return int(m.group(1)) if m else None


def _find_start_index(segment_numbers: list[int], target_segment: int) -> int:
    """Find the file index to start reading from using binary search.

    Uses bisect on segment_numbers to find the leftmost segment >= target_segment.
    Returns len(segment_numbers) if all segments are below target.
    """
    return bisect.bisect_left(segment_numbers, target_segment)


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
        # Active file handle + index during glob catch-up, so the finally
        # block can capture mid-file progress on exception paths.
        self._glob_active_file = None
        self._glob_active_index: int = 0

    def _load_offset(self) -> SingleFileOffset | GlobOffset:
        """Load offset from storage, returning the appropriate typed offset."""
        raw = load(self._offsets_dir, self._group, self._topic)
        if self._mode == "glob":
            return GlobOffset.decode(raw)
        return SingleFileOffset.decode(raw)

    def _save_offset(self, offset: SingleFileOffset | GlobOffset | None = None) -> None:
        """Persist an offset to storage.

        If `offset` is None, saves the current in-memory `self._offset`.
        Passing an explicit value lets callers save a candidate without
        first mutating instance state — see the single-file finally block
        for the save-before-assign contract this supports.
        """
        target = self._offset if offset is None else offset
        save(self._offsets_dir, self._group, self._topic, target.encode())

    def _report_save_failure(self, exc: BaseException) -> None:
        """Report an offset-save failure to both structured logs and stderr.

        brooklet never calls logging.basicConfig, so a bare logger.warning
        disappears into the null handler by default. For consumers like
        `brooklet watch`, whose entire value proposition is resume-across-
        restarts, the user must see the failure — so we also write a
        single line to stderr. The logger.warning stays so structured-log
        consumers still capture the event.
        """
        logger.warning(
            "Failed to save offset during cleanup (topic=%s, group=%s): %s",
            self._topic,
            self._group,
            exc,
        )
        print(
            f"brooklet: failed to save offset for topic={self._topic} group={self._group}: {exc}",
            file=sys.stderr,
            flush=True,
        )

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
        finally:
            # Must live in finally, not after the yield loop — follow-mode
            # iterators exit via exception (KeyboardInterrupt from SIGTERM,
            # typically from a supervisor like Claude Code's Monitor), never
            # by returning normally. Moving this save out of finally would
            # silently lose offsets on every non-normal termination and
            # break resume-across-restarts.
            if not f.closed:
                # Save-before-assign: compute the candidate locally, save
                # it first, and only rebind self._offset on success. If the
                # save raises, self._offset stays aligned with on-disk
                # state so close() and any later inspection see consistent
                # data.
                candidate = SingleFileOffset(byte_offset=f.tell())
                try:
                    self._save_offset(candidate)
                    self._offset = candidate
                except OSError as e:
                    self._report_save_failure(e)
            self._file_handle = None
            f.close()

    def _read_lines(self, f):
        """Read and yield all available lines from a file handle.

        Uses readline() instead of iteration to keep tell() available.
        """
        count = 0
        try:
            while True:
                line = f.readline()
                if not line:
                    break
                self._seq += 1
                event = wrap(line, seq=self._seq, source=self._source)
                if event is not None:
                    count += 1
                    yield event
        finally:
            if count:
                attrs = {"topic": self._topic}
                otel.meter.create_counter(
                    "brooklet.events_consumed", description="Total events consumed"
                ).add(count, attrs)
                otel.meter.create_histogram(
                    "brooklet.batch_size", description="Events per read_lines batch"
                ).record(count, attrs)

    def _catch_up_glob(self, files: list[str]) -> Iterator[Event]:
        """Read all unread events from glob-matched files, updating offset.

        Shared between batch glob and glob+follow modes. During follow mode,
        also populates _file_positions for subsequent tailing.

        For files following the data-NNNN.jsonl naming convention, uses
        segment numbers and binary search to find the starting position.
        This correctly handles gaps from segment deletion/compaction.
        For other glob sources, falls back to positional indexing.
        """
        assert isinstance(self._offset, GlobOffset)

        if not files:
            if self._offset.segment_number != 0 or self._offset.byte_offset != 0:
                logger.error(
                    "Glob matched no files but offset is non-zero "
                    "(segment_number=%d, byte_offset=%d). "
                    "Resetting offset (topic=%s, group=%s).",
                    self._offset.segment_number,
                    self._offset.byte_offset,
                    self._topic,
                    self._group,
                )
                self._offset = GlobOffset(segment_number=0, byte_offset=0)
            return

        # Determine whether all files follow the data-NNNN.jsonl convention
        parsed = [_parse_segment_number(f) for f in files]
        use_segments = all(sn is not None for sn in parsed)

        if use_segments:
            # Segment-number-based lookup via binary search — stable across deletion
            segment_numbers: list[int] = [sn for sn in parsed if sn is not None]
            start_idx = _find_start_index(segment_numbers, self._offset.segment_number)
            # Only apply saved byte_offset if the target segment is exactly the saved one
            if start_idx < len(files) and segment_numbers[start_idx] == self._offset.segment_number:
                start_byte_offset = self._offset.byte_offset
            else:
                start_byte_offset = 0
        else:
            # Positional fallback for external glob sources
            segment_numbers = list(range(len(files)))
            start_idx = self._offset.segment_number
            start_byte_offset = self._offset.byte_offset

            if start_idx >= len(files):
                logger.error(
                    "Saved segment_number %d is out of bounds (only %d files matched). "
                    "Files may have been added or removed between sessions. "
                    "Resetting to start of all files (topic=%s, group=%s).",
                    start_idx,
                    len(files),
                    self._topic,
                    self._group,
                )
                start_idx = 0
                start_byte_offset = 0
                self._offset = GlobOffset(segment_number=0, byte_offset=0)

        for i, filepath in enumerate(files):
            if i < start_idx:
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
                seg_num = segment_numbers[i]
                if i == len(files) - 1:
                    self._offset = GlobOffset(segment_number=seg_num, byte_offset=0)
                else:
                    next_seg = segment_numbers[i + 1]
                    self._offset = GlobOffset(segment_number=next_seg, byte_offset=0)
                continue

            try:
                if i == start_idx:
                    f.seek(start_byte_offset)

                # Track active file so the batch finally block can capture
                # mid-file progress if iteration is interrupted (e.g.
                # KeyboardInterrupt from a supervisor like Claude Code's
                # Monitor).
                self._glob_active_file = f
                self._glob_active_index = i

                yield from self._read_lines(f)

                end_pos = f.tell()
                if self._follow:
                    self._file_positions[filepath] = end_pos

                # After reading this file, update offset to next file
                seg_num = segment_numbers[i]
                if i == len(files) - 1:
                    self._offset = GlobOffset(segment_number=seg_num, byte_offset=end_pos)
                else:
                    next_seg = segment_numbers[i + 1]
                    self._offset = GlobOffset(segment_number=next_seg, byte_offset=0)
                # Normal path: release the active-file tracker so the
                # finally below leaves self._offset at the advanced value.
                self._glob_active_file = None
            finally:
                # On exception paths (GeneratorExit / user exception raised
                # through the yield), capture the mid-file position into
                # self._offset so the outer _iterate_glob finally can persist
                # it. On the normal path the tracker was already cleared, so
                # we leave self._offset at its advanced value.
                if self._glob_active_file is f:
                    with contextlib.suppress(OSError, ValueError):
                        self._offset = GlobOffset(
                            segment_number=segment_numbers[i], byte_offset=f.tell()
                        )
                    self._glob_active_file = None
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
        try:
            yield from self._catch_up_glob(files)
        finally:
            # Must live in finally, not after the yield loop — batch
            # consumers can be interrupted mid-iteration (KeyboardInterrupt
            # from SIGTERM, typically from a supervisor like Claude Code's
            # Monitor). Moving this save out of finally would silently lose
            # offsets on every non-normal termination and break
            # resume-across-restarts. _catch_up_glob's inner finally has
            # already captured any mid-file progress into self._offset.
            try:
                self._save_offset()
            except OSError as e:
                self._report_save_failure(e)

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
                    # Poll all known files even without a watchdog event —
                    # macOS FSEvents coalesces rapid writes.
                    for filepath in list(self._file_positions):
                        known_pos = self._file_positions.get(filepath, 0)
                        try:
                            with open(filepath) as f:
                                f.seek(known_pos)
                                yield from self._read_lines(f)
                                self._file_positions[filepath] = f.tell()
                        except OSError:
                            pass
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

                    # Update GlobOffset: use segment number if the file follows
                    # the data-NNNN.jsonl convention, otherwise use positional index
                    seg_num = _parse_segment_number(filepath)
                    if seg_num is None:
                        all_files = sorted(self._file_positions.keys())
                        seg_num = all_files.index(filepath)
                    self._offset = GlobOffset(
                        segment_number=seg_num,
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

            # Then tail for new lines — poll on every iteration because
            # macOS FSEvents coalesces rapid writes, so relying solely on
            # watchdog events would miss intermediate lines.
            while not self._closed:
                with contextlib.suppress(queue.Empty):
                    event_queue.get(timeout=0.5)

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
                        segment_number=self._offset.segment_number,
                        byte_offset=self._file_handle.tell(),
                    )
                else:
                    self._offset = SingleFileOffset(byte_offset=self._file_handle.tell())
                self._save_offset()
            elif isinstance(self._offset, GlobOffset) and self._offset.encode() > 0:
                # Glob-mode consumers track progress via self._offset rather than
                # _file_handle (which is local to the sub-generators). Save any
                # progress accumulated so far so restarts resume correctly.
                self._save_offset()
        finally:
            if self._observer is not None:
                self._stop_observer(self._observer)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
