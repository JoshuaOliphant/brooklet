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
"""Reusable ``--stream-dir`` option shared by contrib scan commands.

Every contrib adapter takes an optional stream directory that locates brooklet's
offset state (follow mode) and any ``--output`` topic. The flag and ``BROOKLET_DIR``
env fallback are identical everywhere, so they live here as one ``Annotated`` alias
instead of being retyped per command. The *default* differs slightly by adapter: it's
the source path itself when the source is already a directory (scout, otel), or the
source file's parent directory when the source is a single file (pytest) — the help
text above covers both without needing adapter-specific wording.
"""
