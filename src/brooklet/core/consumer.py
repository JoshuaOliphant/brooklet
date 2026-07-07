# ABOUTME: Event consumer with batch and follow modes
# ABOUTME: Reads JSONL lines from registered sources with offset tracking

import bisect
import contextlib
import fnmatch
import glob as glob_module
import logging
import sys
import warnings
from collections.abc import Iterator
from pathlib import Path

from brooklet.contrib import otel
from brooklet.core.envelope import SeqTracker
from brooklet.core.types import Event, GlobOffset, Mode, SingleFileOffset
from brooklet.storage import segments
from brooklet.storage.offsets import load, save

logger = logging.getLogger("brooklet")

_OBSERVER_JOIN_TIMEOUT = 5


def _drain_queue(q) -> None:
    """Empty a queue with a single producer (watchdog handler).

    Used to coalesce a burst of filesystem-event notifications so the
    consumer reads the file once instead of once per notification.
    """
    while not q.empty():
        q.get_nowait()


def _find_start_index(segment_numbers: list[int], target_segment: int) -> int:
    """Find the file index to start reading from using binary search.

    Uses bisect on segment_numbers to find the leftmost segment >= target_segment.
    Returns len(segment_numbers) if all segments are below target.
    """
    return bisect.bisect_left(segment_numbers, target_segment)


class _GlobCatchUp:
    """Reads all unread events across glob-matched files, tracking a GlobOffset.

    Owns its own coordination state — the active file handle and the running
    GlobOffset — so the mid-file position can be captured internally when the
    generator is torn down mid-iteration (KeyboardInterrupt/GeneratorExit from
    a supervisor). Callers read the offset reached so far via `.offset` after
    (or during) iteration, instead of the state machine reaching back into a
    Consumer's mutable attributes.

    For files following the data-NNNN.jsonl naming convention, uses segment
    numbers and binary search to find the starting position; this correctly
    handles gaps from segment deletion/compaction. For other glob sources,
    falls back to positional indexing.

    `file_positions` is the shared follow-mode buffer owned by the calling
    Consumer: during follow mode this unit seeds it with each file's end
    position so the Consumer's subsequent tailing loop can resume from there.
    """

    def __init__(
        self,
        *,
        offset: GlobOffset,
        follow: bool,
        topic: str,
        group: str,
        read_lines,
        file_positions: dict[str, int],
    ) -> None:
        self._offset = offset
        self._follow = follow
        self._topic = topic
        self._group = group
        self._read_lines = read_lines
        self._file_positions = file_positions
        # Active file handle during a read, so the per-file finally can
        # capture mid-file progress if iteration is interrupted.
        self._active_file = None

    @property
    def offset(self) -> GlobOffset:
        """The GlobOffset reached so far (advanced live during iteration)."""
        return self._offset

    def events(self, files: list[str]) -> Iterator[Event]:
        """Yield all unread events across `files`, advancing `self._offset`."""
        if not files:
            self._handle_no_files()
            return

        segment_numbers, start_idx, start_byte_offset = self._plan(files)

        for i, filepath in enumerate(files):
            if i < start_idx:
                self._record_skipped(filepath)
                continue
            seek = start_byte_offset if i == start_idx else 0
            yield from self._read_file(files, segment_numbers, i, filepath, seek)

    def _handle_no_files(self) -> None:
        """Reset a non-zero offset (with a logged error) when nothing matched."""
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

    def _plan(self, files: list[str]) -> tuple[list[int], int, int]:
        """Resolve (segment_numbers, start_idx, start_byte_offset) for `files`."""
        parsed = [segments.parse_number(f) for f in files]
        if all(sn is not None for sn in parsed):
            # Segment-number-based lookup via binary search — stable across deletion
            segment_numbers: list[int] = [sn for sn in parsed if sn is not None]
            start_idx = _find_start_index(segment_numbers, self._offset.segment_number)
            # Only apply saved byte_offset if the target segment is exactly the saved one
            if start_idx < len(files) and segment_numbers[start_idx] == self._offset.segment_number:
                start_byte_offset = self._offset.byte_offset
            else:
                start_byte_offset = 0
            return segment_numbers, start_idx, start_byte_offset
        return self._plan_positional(files)

    def _plan_positional(self, files: list[str]) -> tuple[list[int], int, int]:
        """Positional fallback for external glob sources not using segment names."""
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
        return segment_numbers, start_idx, start_byte_offset

    def _record_skipped(self, filepath: str) -> None:
        """Record a skipped file's size in the follow-mode position buffer."""
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

    def _advance(
        self, files: list[str], segment_numbers: list[int], i: int, end_pos: int = 0
    ) -> GlobOffset:
        """Offset positioned after file `i`: at end_pos if last, else next segment start."""
        if i == len(files) - 1:
            return GlobOffset(segment_number=segment_numbers[i], byte_offset=end_pos)
        return GlobOffset(segment_number=segment_numbers[i + 1], byte_offset=0)

    def _read_file(
        self,
        files: list[str],
        segment_numbers: list[int],
        i: int,
        filepath: str,
        seek: int,
    ) -> Iterator[Event]:
        """Yield events from one file, advancing the offset past it when done."""
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
            self._offset = self._advance(files, segment_numbers, i)
            return

        try:
            f.seek(seek)
            # Track active file so the finally can capture mid-file progress
            # if iteration is interrupted (e.g. KeyboardInterrupt from a
            # supervisor like Claude Code's Monitor).
            self._active_file = f

            yield from self._read_lines(f)

            end_pos = f.tell()
            if self._follow:
                self._file_positions[filepath] = end_pos
            self._offset = self._advance(files, segment_numbers, i, end_pos)
            # Normal path: release the tracker so the finally leaves the
            # advanced offset in place.
            self._active_file = None
        finally:
            # On exception paths (GeneratorExit / user exception raised through
            # the yield), capture the mid-file position so callers persist it.
            # On the normal path the tracker was already cleared, so the
            # advanced offset stands.
            if self._active_file is f:
                with contextlib.suppress(OSError, ValueError):
                    self._offset = GlobOffset(
                        segment_number=segment_numbers[i], byte_offset=f.tell()
                    )
                self._active_file = None
            f.close()


class _SingleFileReader:
    """Reads unread lines from one JSONL file, tracking a SingleFileOffset.

    Owns the open file handle and the running SingleFileOffset so the byte
    position reached is captured internally when the generator is torn down
    mid-iteration (a KeyboardInterrupt/GeneratorExit raised through the yield by
    a supervisor like Claude Code's Monitor). The calling Consumer reads the
    position reached via `.offset` and persists it, mirroring how `_GlobCatchUp`
    hands its offset back instead of the state machine reaching into a
    Consumer's mutable attributes.

    Batch mode reads to EOF and returns; follow mode tails the file via the
    caller-supplied `observe` context manager, polling on every wakeup because
    macOS FSEvents coalesces rapid writes. `file_handle` is exposed only so a
    concurrent Consumer.close() can snapshot the live position from another
    thread while iteration is suspended at a yield.
    """

    def __init__(
        self,
        *,
        path: Path,
        offset: SingleFileOffset,
        follow: bool,
        read_lines,
        observe,
        is_closed,
    ) -> None:
        self._path = path
        self._offset = offset
        self._follow = follow
        self._read_lines = read_lines
        self._observe = observe
        self._is_closed = is_closed
        self._file = None

    @property
    def offset(self) -> SingleFileOffset:
        """The SingleFileOffset reached so far (byte position after teardown)."""
        return self._offset

    @property
    def file_handle(self):
        """The active file handle, or None outside a read.

        Exposed so Consumer.close() can snapshot the live byte position while
        iteration is suspended at a yield in another thread.
        """
        return self._file

    def events(self) -> Iterator[Event]:
        """Yield unread lines, capturing the final byte position on teardown."""
        f = open(self._path)  # noqa: SIM115
        self._file = f
        try:
            f.seek(self._offset.byte_offset)
            if self._follow:
                yield from self._tail(f)
            else:
                yield from self._read_lines(f)
        finally:
            # Capture the position reached — EOF on the normal path, the
            # mid-file position on interruption — so the caller persists it.
            # Follow-mode iterators exit via exception (KeyboardInterrupt from
            # SIGTERM), never by returning, so this must run in finally or every
            # non-normal termination would silently lose the offset.
            if not f.closed:
                self._offset = SingleFileOffset(byte_offset=f.tell())
            self._file = None
            f.close()

    def _tail(self, f) -> Iterator[Event]:
        """Tail the file for appended lines using a watchdog observer."""
        import queue

        from watchdog.events import FileSystemEventHandler

        path = self._path
        event_queue = queue.Queue()

        class Handler(FileSystemEventHandler):
            def on_modified(self, event):
                if Path(event.src_path).resolve() == path.resolve():
                    event_queue.put(True)

        with self._observe(str(path.parent), Handler()):
            # First, read any existing lines.
            yield from self._read_lines(f)

            # Then tail for new lines — poll on every iteration because macOS
            # FSEvents coalesces rapid writes, so relying solely on watchdog
            # events would miss intermediate lines.
            while not self._is_closed():
                with contextlib.suppress(queue.Empty):
                    event_queue.get(timeout=0.5)

                _drain_queue(event_queue)

                yield from self._read_lines(f)


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
        # One tracker spans the whole logical read of this topic (every segment
        # a glob consumer walks, every follow-mode batch), supplying a fallback
        # _seq only for legacy/external lines that carry none. Produced lines
        # already hold a topic-monotonic _seq that wrap() preserves. See
        # brooklet-a2c.
        self._seq_tracker = SeqTracker(source=source)
        self._closed = False
        # Set for the duration of single-file iteration so close() can snapshot
        # the reader's live file position from another thread; None otherwise.
        self._single_reader: _SingleFileReader | None = None
        self._observer = None

        self._offset: SingleFileOffset | GlobOffset = self._load_offset()
        # Shared follow-mode buffer: _GlobCatchUp seeds each file's end
        # position here during catch-up so the tailing loop can resume from it.
        self._file_positions: dict[str, int] = {}

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

    def _persist_offset(self, candidate: SingleFileOffset | GlobOffset) -> None:
        """Persist `candidate` at teardown, reporting — not raising — on OSError.

        The single home for how brooklet survives an offset-save failure when a
        read winds down. Saves the candidate first and rebinds self._offset only
        on success (save-before-assign), so a failed write leaves self._offset
        aligned with the last value actually on disk rather than a phantom
        position that was never persisted. Shared by single-file teardown (where
        the candidate is the fresh f.tell() position the reader reached) and
        glob-batch teardown (where the candidate is the offset _GlobCatchUp
        already advanced into self._offset).
        """
        try:
            self._save_offset(candidate)
            self._offset = candidate
        except OSError as e:
            self._report_save_failure(e)

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

    @contextlib.contextmanager
    def _observe(self, watch_dir: str, handler) -> Iterator[None]:
        """Run a watchdog observer over watch_dir for the duration of the block.

        Schedules `handler` (a FileSystemEventHandler) non-recursively, starts
        the observer, records it on self._observer so close() can stop it from
        another thread, and guarantees the observer is stopped on exit. The
        body owns the tailing loop; anything it must persist on exit (e.g. a
        final offset save) belongs in its own try/finally inside the `with`, so
        it runs before the observer is torn down.
        """
        from watchdog.observers import Observer

        observer = Observer()
        observer.schedule(handler, watch_dir, recursive=False)
        observer.start()
        self._observer = observer
        try:
            yield
        finally:
            self._stop_observer(observer)

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
        """Read events from a single JSONL file via a `_SingleFileReader`.

        The reader owns the file handle and byte-offset tracking; this method
        only guards the nonexistent-file case, exposes the reader to close() for
        the duration of the read, and persists the position the reader reached.
        """
        path = Path(self._path).expanduser()
        if not path.exists():
            warnings.warn(
                f"Source file does not exist: {path} "
                f"(topic={self._topic!r}, group={self._group!r})",
                stacklevel=2,
            )
            return

        assert isinstance(self._offset, SingleFileOffset)
        reader = _SingleFileReader(
            path=path,
            offset=self._offset,
            follow=self._follow,
            read_lines=self._read_lines,
            observe=self._observe,
            is_closed=lambda: self._closed,
        )
        self._single_reader = reader
        try:
            yield from reader.events()
        finally:
            # Must live in finally, not after the yield loop — follow-mode
            # iterators exit via exception (KeyboardInterrupt from SIGTERM,
            # typically from a supervisor like Claude Code's Monitor), never by
            # returning normally. Moving this save out of finally would silently
            # lose offsets on every non-normal termination and break
            # resume-across-restarts. _persist_offset applies the save-before-
            # assign contract so a failed write leaves self._offset aligned with
            # on-disk state.
            self._persist_offset(reader.offset)
            self._single_reader = None

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
                # SeqTracker preserves any valid persisted _seq and supplies a
                # high-water-mark fallback only for legacy/external lines that
                # carry none — keeping _seq monotonic across mixed sources.
                event = self._seq_tracker.wrap(line)
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

        Delegates to `_GlobCatchUp`, which owns the catch-up coordination
        state internally. Shared between batch glob and glob+follow modes;
        during follow mode `_GlobCatchUp` seeds `_file_positions` for the
        subsequent tailing loop. The offset reached — including any mid-file
        position captured on interruption — is synced back into `self._offset`
        so the callers' `finally` blocks persist it.
        """
        assert isinstance(self._offset, GlobOffset)

        catch_up = _GlobCatchUp(
            offset=self._offset,
            follow=self._follow,
            topic=self._topic,
            group=self._group,
            read_lines=self._read_lines,
            file_positions=self._file_positions,
        )
        try:
            yield from catch_up.events(files)
        finally:
            self._offset = catch_up.offset

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
            # already captured any mid-file progress into self._offset, so the
            # candidate to persist is simply the offset reached.
            self._persist_offset(self._offset)

    def _iterate_glob_follow(self):
        """Catch up on existing glob files, then tail for changes and new files."""
        import queue

        from watchdog.events import FileSystemEventHandler

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

        with self._observe(watch_dir, GlobHandler()):
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

                    # Drain the queue to batch process notifications. The queue
                    # only has one producer (the watchdog handler), so empty() is
                    # reliable here — get_nowait() cannot race against a remover.
                    pending = [(action, filepath)]
                    while not event_queue.empty():
                        pending.append(event_queue.get_nowait())

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
                        seg_num = segments.parse_number(filepath)
                        if seg_num is None:
                            all_files = sorted(self._file_positions.keys())
                            seg_num = all_files.index(filepath)
                        self._offset = GlobOffset(
                            segment_number=seg_num,
                            byte_offset=self._file_positions[filepath],
                        )

                    self._save_offset()
            finally:
                # Runs before _observe stops the observer, preserving the
                # save-then-stop order the previous explicit finally had.
                self._save_offset()

    def close(self) -> None:
        """Stop the consumer and save the current offset."""
        self._closed = True

        try:
            # Save offset from the current file position if a single-file read
            # is in flight. _single_reader owns the handle during single-file
            # iteration; glob-mode sub-generators keep their own local handles,
            # so glob progress is read from self._offset instead.
            handle = self._single_reader.file_handle if self._single_reader else None
            if handle is not None and not handle.closed:
                self._offset = SingleFileOffset(byte_offset=handle.tell())
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
