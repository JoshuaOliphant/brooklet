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
        help="Stream directory for brooklet state (offsets, --output). Defaults to source path.",
    ),
]
"""Reusable ``--stream-dir`` option shared by contrib scan commands.

Every contrib adapter takes an optional stream directory that locates brooklet's
offset state (follow mode) and any ``--output`` topic. The flag, ``BROOKLET_DIR``
env fallback, and default-to-source-path semantics are identical everywhere, so
they live here as one ``Annotated`` alias instead of being retyped per command.
"""
