# ABOUTME: Source registration mapping external JSONL paths to topic names
# ABOUTME: Persists registrations in .brooklet/sources.json for cross-session use

import contextlib
import json
import os
import re
import tempfile
from pathlib import Path

VALID_MODES = {"single-file", "glob"}

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_\-\.]+$")


def _validate_topic_name(name: str) -> None:
    """Reject topic names that could cause path traversal or filesystem issues."""
    if not _SAFE_NAME_RE.match(name):
        msg = (
            f"topic name must contain only safe characters "
            f"(alphanumeric, hyphens, underscores, dots), got {name!r}"
        )
        raise ValueError(msg)


class Registry:
    """Manages topic-to-source mappings persisted in sources.json."""

    def __init__(self, brooklet_dir: str | Path) -> None:
        self._brooklet_dir = Path(brooklet_dir)
        self._sources_path = self._brooklet_dir / "sources.json"
        self._sources = self._load()

    def _load(self) -> dict:
        """Load sources from disk, or return empty dict.

        Raises:
            ValueError: If sources.json exists but contains invalid JSON.
        """
        if self._sources_path.exists():
            try:
                return json.loads(self._sources_path.read_text())
            except (json.JSONDecodeError, TypeError) as e:
                raise ValueError(
                    f"Corrupt sources file at {self._sources_path}: {e}. "
                    f"Delete this file and re-register your sources."
                ) from e
        return {}

    def _save(self) -> None:
        """Persist sources to disk atomically."""
        self._brooklet_dir.mkdir(parents=True, exist_ok=True)
        data = json.dumps(self._sources, indent=2)

        fd, tmp_path = tempfile.mkstemp(dir=self._brooklet_dir, suffix=".tmp")
        fd_closed = False
        try:
            os.write(fd, data.encode())
            os.close(fd)
            fd_closed = True
            os.replace(tmp_path, self._sources_path)
        except BaseException:
            if not fd_closed:
                with contextlib.suppress(OSError):
                    os.close(fd)
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def register(self, name: str, path: str, mode: str) -> None:
        """Register an external JSONL path as a named topic.

        Args:
            name: Topic name for consumers to reference.
            path: Filesystem path (absolute) or glob pattern.
            mode: Either "single-file" or "glob".

        Raises:
            ValueError: If mode is not "single-file" or "glob", or name is invalid.
        """
        _validate_topic_name(name)

        if mode not in VALID_MODES:
            msg = f"mode must be one of {VALID_MODES}, got {mode!r}"
            raise ValueError(msg)

        self._sources[name] = {"path": path, "mode": mode}
        self._save()

    def get(self, name: str) -> dict:
        """Get the source definition for a registered topic.

        Returns a copy to prevent mutation of internal state.

        Raises:
            KeyError: If the topic is not registered.
        """
        return dict(self._sources[name])

    def list_topics(self) -> list[str]:
        """Return names of all registered topics."""
        return list(self._sources.keys())
