# ABOUTME: Shared "tee" helper for contrib scan commands that mirror stats to a topic
# ABOUTME: Produces each item to a brooklet topic as a passthrough, warning on failure

from collections.abc import Callable, Iterable, Iterator
from typing import Any

import typer


def tee_to_topic(
    items: Iterable[Any],
    stream: Any,
    topic: str,
    source: str,
    describe: Callable[[Any], str],
) -> Iterator[Any]:
    """Yield each item unchanged while producing its ``to_dict()`` to a topic.

    The passthrough sink behind contrib scan commands' ``--output`` mode: stats
    still flow on to the renderer, and each is mirrored into ``topic`` as it
    passes. A produce failure is reported as a warning on stderr — never raised —
    so one bad record can't abort a long-running live scan.

    Args:
        items: Source iterable of objects exposing a ``to_dict()`` method.
        stream: An open brooklet Stream to produce into.
        topic: Destination topic name.
        source: Producer identifier for the ``_src`` envelope field.
        describe: Renders a short label for an item, used only in the failure
            warning (e.g. ``lambda s: f"session {s.session_id}"``).
    """
    for item in items:
        try:
            stream.produce(topic, item.to_dict(), source=source)
        except (OSError, ValueError, TypeError) as e:
            typer.echo(
                f"Warning: failed to produce {describe(item)} to topic {topic!r}: {e}",
                err=True,
            )
        yield item
