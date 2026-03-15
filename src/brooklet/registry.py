# ABOUTME: Source registration mapping external JSONL paths to topic names
# ABOUTME: Persists registrations in .brooklet/sources.json for cross-session use

import json
import os
import tempfile
from pathlib import Path

VALID_MODES = {"single-file", "glob"}


class Registry:
    """Manages topic-to-source mappings persisted in sources.json."""

    def __init__(self, brooklet_dir: str | Path) -> None:
        self._brooklet_dir = Path(brooklet_dir)
        self._sources_path = self._brooklet_dir / "sources.json"
        self._sources = self._load()

    def _load(self) -> dict:
        """Load sources from disk, or return empty dict."""
        if self._sources_path.exists():
            return json.loads(self._sources_path.read_text())
        return {}

    def _save(self) -> None:
        """Persist sources to disk atomically."""
        self._brooklet_dir.mkdir(parents=True, exist_ok=True)
        data = json.dumps(self._sources, indent=2)

        fd, tmp_path = tempfile.mkstemp(dir=self._brooklet_dir, suffix=".tmp")
        try:
            os.write(fd, data.encode())
            os.close(fd)
            os.replace(tmp_path, self._sources_path)
        except BaseException:
            os.close(fd) if not os.get_inheritable(fd) else None
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
            ValueError: If mode is not "single-file" or "glob".
        """
        if mode not in VALID_MODES:
            msg = f"mode must be one of {VALID_MODES}, got {mode!r}"
            raise ValueError(msg)

        self._sources[name] = {"path": path, "mode": mode}
        self._save()

    def get(self, name: str) -> dict:
        """Get the source definition for a registered topic.

        Raises:
            KeyError: If the topic is not registered.
        """
        return self._sources[name]

    def list_topics(self) -> list[str]:
        """Return names of all registered topics."""
        return list(self._sources.keys())
