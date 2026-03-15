# ABOUTME: Public API for brooklet — lightweight JSONL event streaming library
# ABOUTME: Exports open() convenience function and __version__

from pathlib import Path

from brooklet.stream import Stream
from brooklet.types import Event, Mode, SourceDef  # noqa: F401

__version__ = "0.1.0"


def open(path: str | Path) -> Stream:
    """Open a brooklet stream directory.

    Creates the directory and .brooklet/ metadata if they don't exist.

    Args:
        path: Path to the stream directory.

    Returns:
        A Stream instance for registering sources and consuming events.
    """
    return Stream(path)
