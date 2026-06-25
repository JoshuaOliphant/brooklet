# ABOUTME: Safe-name validation for values that become filesystem paths
# ABOUTME: One home for the path-traversal / unsafe-character guard used by storage

import re
from pathlib import Path

# Topic, group, and source names are interpolated into file paths under
# .brooklet/, so they must be restricted to characters that can't escape that
# directory or confuse the filesystem. Slashes are allowed (path-style topics
# like "scout/stats" nest directories); ".." is rejected separately so a slash
# can't be used to climb out.
_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_\-\./]+$")


def validate_safe_name(value: str, label: str) -> None:
    """Reject a name that could cause path traversal or filesystem issues.

    Args:
        value: The name to validate.
        label: What the name is (e.g. "topic", "group"), used in error messages.

    Raises:
        ValueError: If the name contains unsafe characters or path traversal.
    """
    if not _SAFE_NAME_RE.match(value):
        msg = (
            f"{label} must contain only safe characters "
            f"(alphanumeric, hyphens, underscores, dots, slashes), got {value!r}"
        )
        raise ValueError(msg)
    if ".." in Path(value).parts:
        msg = f"{label} must not contain path traversal (got {value!r})"
        raise ValueError(msg)
