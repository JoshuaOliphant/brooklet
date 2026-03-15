# ABOUTME: Stream orchestrator — main entry point for brooklet operations
# ABOUTME: Coordinates registry, consumer, and offset modules into a unified API

from pathlib import Path

from brooklet.consumer import Consumer
from brooklet.envelope import serialize
from brooklet.registry import Registry
from brooklet.types import Mode


class Stream:
    """A brooklet stream directory for registering sources and consuming events.

    Manages the .brooklet/ metadata directory, source registry, and consumer
    creation. This is the primary API surface — most users interact with
    brooklet through this class (via brooklet.open()).
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._brooklet_dir = self._path / ".brooklet"
        self._offsets_dir = self._brooklet_dir / "offsets"

        # Create metadata directories
        self._brooklet_dir.mkdir(parents=True, exist_ok=True)
        self._offsets_dir.mkdir(exist_ok=True)

        self._registry = Registry(self._brooklet_dir)

    def register(self, name: str, path: str, mode: Mode) -> None:
        """Register an external JSONL path as a named topic.

        Args:
            name: Topic name for consumers to reference.
            path: Filesystem path or glob pattern.
            mode: Either "single-file" or "glob".
        """
        self._registry.register(name, path, mode)

    def produce(self, topic: str, event: dict, source: str | None = None) -> None:
        """Produce an event to a local topic.

        Creates the topic directory and auto-registers it on first write.
        Envelope fields (_ts, _seq, _src) are injected automatically.

        Args:
            topic: Topic name. Supports path-style nesting (e.g. "scout/stats").
            event: Event payload as a dict.
            source: Optional producer identifier for _src field.

        Raises:
            TypeError: If event is not a dict.
            ValueError: If topic name contains path traversal or collides with
                an external registered source.
        """
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

        # Create topic directory and data file path
        topic_dir = self._path / topic
        topic_dir.mkdir(parents=True, exist_ok=True)
        data_path = topic_dir / "data.jsonl"

        # Determine next sequence number from existing line count
        next_seq = 0
        if data_path.exists():
            with open(data_path) as f:
                next_seq = sum(1 for _ in f)
        next_seq += 1

        # Serialize with envelope and append
        line = serialize(dict(event), seq=next_seq, source=source)
        with open(data_path, "a") as f:
            f.write(line)

        # Auto-register in the unified namespace
        self._registry.register_local(topic, str(data_path))

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
