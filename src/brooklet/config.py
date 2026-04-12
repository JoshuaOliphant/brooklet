# ABOUTME: Config precedence chain for stream directory resolution
# ABOUTME: Layers: CLI flag > .brooklet.toml > BROOKLET_DIR env > user config > git root

import os
import tomllib
from pathlib import Path


class ConfigError(Exception):
    """Raised when a brooklet config file is invalid or unreadable."""


def resolve_stream_dir(cli_flag: Path | None = None) -> Path:
    """Resolve the stream directory using a 5-layer config precedence chain.

    Precedence (highest to lowest):
        1. Explicit CLI flag (--stream-dir)
        2. .brooklet.toml (walk up from cwd, git root is the ceiling)
        3. BROOKLET_DIR environment variable
        4. ~/.config/brooklet/config.toml (user-wide)
        5. Git repo root, or cwd if not in a repo

    Args:
        cli_flag: Explicit path from CLI --stream-dir option. None to skip.

    Returns:
        Resolved Path for the stream directory.

    Raises:
        ConfigError: If a config file exists but is invalid, unreadable,
            or missing the required ``stream_dir`` key.
    """
    # Layer 1: explicit CLI flag
    if cli_flag is not None:
        return cli_flag

    # Layer 2: .brooklet.toml (walk up to git root)
    cwd = Path.cwd()
    local_config = find_config_file(".brooklet.toml", start=cwd)
    if local_config is not None:
        return _read_stream_dir(local_config)

    # Layer 3: BROOKLET_DIR env var
    env_dir = os.environ.get("BROOKLET_DIR")
    if env_dir:
        return Path(env_dir)

    # Layer 4: user-wide config
    user_config = _user_config_path()
    if user_config.exists():
        return _read_stream_dir(user_config)

    # Layer 5: git repo root, or cwd
    git_root = _find_git_root(cwd)
    if git_root is not None:
        return git_root

    return cwd


def find_config_file(filename: str, start: Path) -> Path | None:
    """Walk up parent directories looking for a config file.

    Stops at the git boundary (.git file or directory). The config file
    at the git root level is included in the search.

    Args:
        filename: Name of the config file to find (e.g. ".brooklet.toml").
        start: Directory to start searching from.

    Returns:
        Path to the config file, or None if not found.
    """
    current = start.resolve()
    while True:
        candidate = current / filename
        if candidate.exists():
            return candidate

        # If this directory has .git, it's the repo root — stop here
        if (current / ".git").exists():
            return None

        parent = current.parent
        if parent == current:
            # Reached filesystem root
            return None
        current = parent


def _read_stream_dir(config_path: Path) -> Path:
    """Read the stream_dir value from a TOML config file.

    Relative paths are resolved relative to the config file's parent directory.

    Raises:
        ConfigError: If the file is unreadable, has invalid TOML syntax,
            is missing the ``stream_dir`` key, or the value is not a string.
    """
    try:
        with open(config_path, "rb") as f:
            data = tomllib.load(f)
    except PermissionError as e:
        raise ConfigError(f"Cannot read config file {config_path}: {e}") from e
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"Invalid TOML in {config_path}: {e}") from e

    if "stream_dir" not in data:
        raise ConfigError(
            f"Config file {config_path} is missing the required 'stream_dir' key. "
            f"Found keys: {sorted(data.keys())}"
        )

    raw = data["stream_dir"]
    if not isinstance(raw, str):
        raise ConfigError(
            f"'stream_dir' in {config_path} must be a string, "
            f"got {type(raw).__name__}: {raw!r}"
        )

    path = Path(raw)
    if not path.is_absolute():
        path = config_path.parent / path
    return path.resolve()


def _user_config_path() -> Path:
    """Return the path to the user-wide brooklet config file."""
    return Path.home() / ".config" / "brooklet" / "config.toml"


def _find_git_root(start: Path) -> Path | None:
    """Walk up from start looking for a .git file or directory."""
    current = start.resolve()
    while True:
        if (current / ".git").exists():
            return current
        parent = current.parent
        if parent == current:
            return None
        current = parent
