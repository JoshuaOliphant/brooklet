# ABOUTME: Stream orchestrator — main entry point for brooklet operations
# ABOUTME: Coordinates registry, consumer, and offset modules into a unified API

import glob as glob_module
import re
from pathlib import Path

from brooklet.contrib import otel
from brooklet.core.consumer import Consumer
from brooklet.core.envelope import serialize
from brooklet.core.types import Mode
from brooklet.storage.locking import topic_lock
from brooklet.storage.registry import Registry
from brooklet.storage.sidecar import derive_next_seq, read_next_seq, write_next_seq


class Stream:
    """A brooklet stream directory for registering sources and consuming events.

    Manages the .brooklet/ metadata directory, source registry, and consumer
    creation. This is the primary API surface — most users interact with
    brooklet through this class (via brooklet.open()).
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path).resolve()
        self._brooklet_dir = self._path / ".brooklet"
        self._offsets_dir = self._brooklet_dir / "offsets"

        # Create metadata directories
        self._brooklet_dir.mkdir(parents=True, exist_ok=True)
        self._offsets_dir.mkdir(exist_ok=True)

        self._registry = Registry(self._brooklet_dir)
        # In-memory segment cache per topic: {topic: (active_path, cached_size, segment_number)}
        self._segment_cache: dict[str, tuple[Path, int, int]] = {}

    def register(self, name: str, path: str, mode: Mode) -> None:
        """Register an external JSONL path as a named topic.

        Args:
            name: Topic name for consumers to reference.
            path: Filesystem path or glob pattern.
            mode: Either "single-file" or "glob".
        """
        self._registry.register(name, path, mode)

    def _discover_or_migrate_segments(self, topic: str, topic_dir: Path) -> None:
        """Populate the segment cache for a topic, handling legacy migration if needed.

        If only a bare data.jsonl exists (no segment files), it is renamed to
        data-0000.jsonl to migrate it into the segment numbering scheme.
        Sets self._segment_cache[topic] = (active_path, cached_size, seg_num).
        """
        segments = sorted(glob_module.glob(str(topic_dir / "data-*.jsonl")))
        bare = topic_dir / "data.jsonl"

        if not segments and bare.exists():
            # Legacy migration: rename data.jsonl → data-0000.jsonl.
            # New writes start in data-0001.jsonl so 0000 is the historical archive.
            migrated = topic_dir / "data-0000.jsonl"
            bare.rename(migrated)
            # Don't add to segments — fall through to brand-new topic logic at 0001
            seg_num = 1
            active = topic_dir / f"data-{seg_num:04d}.jsonl"
            active.touch()
            self._segment_cache[topic] = (active, 0, seg_num)
            return

        if segments:
            # Parse segment number from the last (active) segment
            m = re.search(r"data-(\d+)\.jsonl$", segments[-1])
            seg_num = int(m.group(1)) if m else 0
            active = Path(segments[-1])
            size = active.stat().st_size
        else:
            # Brand new topic: start at segment 0001
            seg_num = 1
            active = topic_dir / f"data-{seg_num:04d}.jsonl"
            active.touch()
            size = 0

        self._segment_cache[topic] = (active, size, seg_num)

    def produce(
        self,
        topic: str,
        event: dict,
        source: str | None = None,
        max_segment_bytes: int = 10_000_000,
    ) -> None:
        """Produce an event to a local topic using segment rotation.

        Creates the topic directory and auto-registers it on first write.
        Envelope fields (_ts, _seq, _src) are injected automatically.
        Segments are rotated when the active segment exceeds max_segment_bytes.

        Args:
            topic: Topic name. Supports path-style nesting (e.g. "scout/stats").
            event: Event payload as a dict.
            source: Optional producer identifier for _src field.
            max_segment_bytes: Rotate to a new segment when active file exceeds
                this size in bytes.

        Raises:
            TypeError: If event is not a dict.
            ValueError: If topic name contains path traversal or collides with
                an external registered source.
            BrookletWriteLockError: If another process holds the write lock.
        """
        with otel.tracer.start_as_current_span("produce") as span:
            span.set_attribute("brooklet.topic", topic)

            if not isinstance(event, dict):
                msg = f"event must be a dict, got {type(event).__name__}"
                raise TypeError(msg)

            # Reject path traversal
            if ".." in Path(topic).parts:
                msg = f"topic name must not contain path traversal (got {topic!r})"
                raise ValueError(msg)

            # Check namespace collision with external sources
            if self._registry.is_external(topic):
                msg = f"topic {topic!r} is already registered as an external source"
                raise ValueError(msg)

            topic_dir = self._path / topic
            topic_dir.mkdir(parents=True, exist_ok=True)

            # Acquire exclusive write lock for this topic
            with topic_lock(self._brooklet_dir, topic):
                # Populate segment cache if not already present
                if topic not in self._segment_cache:
                    self._discover_or_migrate_segments(topic, topic_dir)

                active_path, cached_size, seg_num = self._segment_cache[topic]

                # Rotate to next segment if the active one exceeds the size threshold
                if cached_size >= max_segment_bytes:
                    seg_num += 1
                    active_path = topic_dir / f"data-{seg_num:04d}.jsonl"
                    active_path.touch()
                    cached_size = 0

                # Get next_seq from sidecar, re-derive if missing or stale
                next_seq = read_next_seq(self._brooklet_dir, topic)
                if next_seq is None:
                    next_seq = derive_next_seq(active_path)
                else:
                    # Verify sidecar against active segment; re-derive if stale
                    derived = derive_next_seq(active_path)
                    if derived > next_seq:
                        next_seq = derived

                # Serialize with envelope and append to the active segment
                line = serialize(dict(event), seq=next_seq, source=source)
                with open(active_path, "a") as f:
                    f.write(line)

                # Update sidecar cache
                write_next_seq(self._brooklet_dir, topic, next_seq + 1)

                # Update in-memory segment cache
                cached_size += len(line.encode())
                self._segment_cache[topic] = (active_path, cached_size, seg_num)

            # Auto-register with glob pattern in the unified namespace
            glob_pattern = str(topic_dir / "data-*.jsonl")
            self._registry.register_local(topic, glob_pattern, mode="glob")
            otel.meter.create_counter(
                "brooklet.events_produced", description="Total events produced"
            ).add(1, {"topic": topic})

    def consume(self, topic: str, group: str, follow: bool = False) -> Consumer:
        """Create a consumer iterator for a registered topic.

        Args:
            topic: Registered topic name.
            group: Consumer group name for independent offset tracking.
            follow: If True, tail for new events (single-file and glob modes).

        Returns:
            A Consumer iterator yielding event dicts with envelope fields.

        Raises:
            KeyError: If the topic is not registered.
        """
        source = self._registry.get(topic)
        return Consumer(
            path=source["path"],
            mode=source["mode"],
            group=group,
            topic=topic,
            offsets_dir=self._offsets_dir,
            source=topic,
            follow=follow,
        )

    def topics(self) -> list[str]:
        """Return names of all registered topics."""
        return self._registry.list_topics()
