# ABOUTME: Shared Typer option definitions reused across contrib adapter CLIs
# ABOUTME: Provides the reusable --stream-dir option so adapters don't retype it

from pathlib import Path
from typing import Annotated

import typer

StreamDirOption = Annotated[
    Path | None,
    typer.Option(
        "--stream-dir",
        envvar="BROOKLET_DIR",
        help=(
            "Stream directory for brooklet state (offsets, --output). Defaults to "
            "the source directory (or its parent, if the source is a file)."
        ),
    ),
]
"""Reusable ``--stream-dir`` option for adapters with an ``--output`` flag.

Every contrib adapter takes an optional stream directory that locates brooklet's
offset state (follow mode). Adapters that also support ``--output`` (mirroring
parsed stats into a topic) use this variant — currently ``scout scan`` and
``pytest scan``. The default is the source path itself when the source is already
a directory (scout), or the source file's parent directory when the source is a
single file (pytest).
"""

StreamDirOptionFollowOnly = Annotated[
    Path | None,
    typer.Option(
        "--stream-dir",
        envvar="BROOKLET_DIR",
        help=(
            "Stream directory for brooklet offset state (follow mode). "
            "Defaults to the source directory."
        ),
    ),
]
"""Reusable ``--stream-dir`` option for adapters without an ``--output`` flag.

The otel subcommands (``traces``/``metrics``/``logs``) only use ``--stream-dir``
for follow-mode offset state — they have no ``--output`` topic, and their source
is always a directory, never a single file — so their help text shouldn't mention
either.
"""
