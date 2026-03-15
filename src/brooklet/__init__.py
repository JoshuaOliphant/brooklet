# ABOUTME: Public API for brooklet — lightweight JSONL event streaming library
# ABOUTME: Exports open() convenience function and __version__

from brooklet.stream import Stream

__version__ = "0.1.0"


def open(path: str) -> Stream:
    """Open a brooklet stream directory.

    Creates the directory and .brooklet/ metadata if they don't exist.

    Args:
        path: Path to the stream directory.

    Returns:
        A Stream instance for registering sources and consuming events.
    """
    return Stream(path)
