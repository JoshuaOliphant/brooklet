# ABOUTME: Unified CLI for brooklet — Typer app with core commands and plugin loading
# ABOUTME: Entry point for the `brooklet` command, wrapping the library API

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated

import pluggy
import typer

import brooklet
from brooklet.plugins import get_plugin_manager
from brooklet.types import Mode

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
    mode: Annotated[Mode, typer.Option(help="Source mode: single-file or glob.")] = "single-file",
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


@app.command(rich_help_panel="Core Commands")
def produce(
    topic: Annotated[str, typer.Argument(help="Topic name to produce events to.")],
    stream_dir: STREAM_DIR_OPTION = Path("."),
    source: Annotated[str | None, typer.Option(help="Producer identifier for _src field.")] = None,
) -> None:
    """Produce events to a topic from stdin (one JSON object per line)."""
    stream = brooklet.open(stream_dir)
    line_num = 0
    for line in sys.stdin:
        line_num += 1
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as e:
            typer.echo(f"Warning: skipping line {line_num}: {e}", err=True)
            continue
        if not isinstance(event, dict):
            typer.echo(
                f"Warning: skipping line {line_num}: "
                f"expected JSON object, got {type(event).__name__}",
                err=True,
            )
            continue
        try:
            stream.produce(topic, event, source=source)
        except (OSError, ValueError, TypeError) as e:
            typer.echo(f"Error: failed to produce to {topic!r}: {e}", err=True)
            raise typer.Exit(code=1) from None


@app.command(rich_help_panel="Core Commands")
def consume(
    topic: Annotated[str, typer.Argument(help="Topic name to consume events from.")],
    group: Annotated[str, typer.Option(help="Consumer group name for offset tracking.")],
    stream_dir: STREAM_DIR_OPTION = Path("."),
    follow: Annotated[bool, typer.Option("--follow", help="Tail for new events.")] = False,
) -> None:
    """Consume events from a topic to stdout (one JSON object per line)."""
    stream = brooklet.open(stream_dir)
    try:
        consumer_ctx = stream.consume(topic, group=group, follow=follow)
    except KeyError:
        typer.echo(f"Error: topic {topic!r} is not registered", err=True)
        raise typer.Exit(code=1) from None
    try:
        with consumer_ctx as consumer:
            for event in consumer:
                typer.echo(json.dumps(event))
    except KeyboardInterrupt:
        pass


def _load_plugins() -> None:
    """Load built-in and third-party plugins onto the app.

    Called at module level so the app always has plugin commands registered.
    """
    try:
        pm = get_plugin_manager()
        pm.hook.brooklet_commands(cli=app)
    except (ImportError, pluggy.PluginValidationError) as e:
        print(f"Warning: failed to load plugins: {e}", file=sys.stderr)


_load_plugins()


def main() -> None:
    """CLI entry point for the `brooklet` command."""
    app()
