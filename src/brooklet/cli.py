# ABOUTME: Unified CLI for brooklet — Typer app with core commands and plugin loading
# ABOUTME: Entry point for the `brooklet` command, wrapping the library API

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

import brooklet
from brooklet.plugins import get_plugin_manager

app = typer.Typer(
    name="brooklet",
    help="The SQLite of event streaming — consumer coordination on top of JSONL files.",
    no_args_is_help=True,
)

STREAM_DIR_OPTION = Annotated[
    Path,
    typer.Option(
        "--stream-dir",
        envvar="BROOKLET_DIR",
        help="Path to the brooklet stream directory.",
    ),
]


@app.command(rich_help_panel="Core Commands")
def register(
    name: Annotated[str, typer.Argument(help="Topic name to register.")],
    path: Annotated[str, typer.Argument(help="File path or glob pattern.")],
    mode: Annotated[str, typer.Option(help="Source mode: single-file or glob.")] = "single-file",
    stream_dir: STREAM_DIR_OPTION = Path("."),
) -> None:
    """Register an external JSONL source as a named topic."""
    stream = brooklet.open(stream_dir)
    stream.register(name, path, mode)


@app.command(rich_help_panel="Core Commands")
def topics(
    stream_dir: STREAM_DIR_OPTION = Path("."),
    json_output: Annotated[bool, typer.Option("--json", help="Output as JSON array.")] = False,
) -> None:
    """List registered topics."""
    stream = brooklet.open(stream_dir)
    topic_list = stream.topics()
    if json_output:
        typer.echo(json.dumps(topic_list))
    else:
        for name in topic_list:
            typer.echo(name)


def _load_plugins() -> None:
    pm = get_plugin_manager()
    pm.hook.brooklet_commands(cli=app)


_load_plugins()


def main() -> None:
    """CLI entry point for the `brooklet` command."""
    app()
