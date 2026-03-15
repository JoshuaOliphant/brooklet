# ABOUTME: Stream orchestrator — main entry point for brooklet operations
# ABOUTME: Coordinates registry, consumer, and offset modules into a unified API

from pathlib import Path

from brooklet.consumer import Consumer
from brooklet.registry import Registry


class Stream:
    """A brooklet stream directory for registering sources and consuming events.

    Manages the .brooklet/ metadata directory, source registry, and consumer
    creation. This is the primary API surface — most users interact with
    brooklet through this class (via brooklet.open()).
    """

    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._brooklet_dir = self._path / ".brooklet"
        self._offsets_dir = self._brooklet_dir / "offsets"

        # Create metadata directories
        self._brooklet_dir.mkdir(parents=True, exist_ok=True)
        self._offsets_dir.mkdir(exist_ok=True)

        self._registry = Registry(self._brooklet_dir)

    def register(self, name: str, path: str, mode: str) -> None:
        """Register an external JSONL path as a named topic.

        Args:
            name: Topic name for consumers to reference.
            path: Filesystem path or glob pattern.
            mode: Either "single-file" or "glob".
        """
        self._registry.register(name, path, mode)

    def consume(self, topic: str, group: str, follow: bool = False) -> Consumer:
        """Create a consumer iterator for a registered topic.

        Args:
            topic: Registered topic name.
            group: Consumer group name for independent offset tracking.
            follow: If True, tail the file for new events (single-file only).

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
            follow=follow,
        )

    def topics(self) -> list[str]:
        """Return names of all registered topics."""
        return self._registry.list_topics()
