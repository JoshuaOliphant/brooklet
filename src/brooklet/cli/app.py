# ABOUTME: Unified CLI for brooklet — Typer app with core commands and plugin loading
# ABOUTME: Entry point for the `brooklet` command, wrapping the library API

from __future__ import annotations

import json
import signal
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Annotated, TextIO

import pluggy
import typer

import brooklet
from brooklet.cli.plugins import get_plugin_manager
from brooklet.cli.watch_format import format_event
from brooklet.contrib import otel
from brooklet.core.types import Event, Mode


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"brooklet {brooklet.__version__}")
        raise typer.Exit()


app = typer.Typer(
    name="brooklet",
    help="The SQLite of event streaming — consumer coordination on top of JSONL files.",
    no_args_is_help=True,
)


@app.callback(invoke_without_command=True)
def _app_callback(
    version: Annotated[
        bool, typer.Option("--version", help="Show version and exit.", is_eager=True)
    ] = False,
) -> None:
    """The SQLite of event streaming — consumer coordination on top of JSONL files."""
    _version_callback(version)


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


def _watch_impl(events: Iterable[Event], out: TextIO) -> None:
    """Write one compact line per event to ``out``, flushing after each.

    Decoupled from Stream/Consumer so tests can inject a plain iterable.

    Per-event failures (e.g. a non-dict payload, a field whose ``__repr__``
    raises) are isolated: the offending event gets a fallback line matching
    the one-line-per-event Monitor contract, the full traceback is echoed to
    stderr for diagnosis, and iteration continues. One malformed event must
    never take down a long-running watcher.
    """
    for event in events:
        try:
            line = format_event(event)
        except Exception as exc:
            msg = str(exc)
            if len(msg) > 80:
                msg = msg[:80]
            line = f"#? ??:??:?? <format error: {type(exc).__name__}: {msg}>"
            print(
                f"brooklet watch: format error: {type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
        out.write(line + "\n")
        out.flush()


@app.command(rich_help_panel="Core Commands")
def watch(
    topic: Annotated[str, typer.Argument(help="Topic name to tail.")],
    group: Annotated[
        str,
        typer.Option(
            help=(
                "Consumer group name for offset tracking. "
                "Default 'watch' enables resume across restarts — "
                "use a distinct group if running multiple concurrent watchers."
            ),
        ),
    ] = "watch",
    stream_dir: STREAM_DIR_OPTION = Path("."),
) -> None:
    """Tail a topic, emitting one compact line per event.

    Always follows — designed for Claude Code's Monitor tool, which turns each
    stdout line into a chat notification. Output is line-buffered so events
    reach the reader immediately.
    """

    # Install the SIGTERM-to-KeyboardInterrupt handler BEFORE any other work.
    # Monitor's TaskStop sends SIGTERM, and Python's default SIGTERM action
    # is to exit immediately without unwinding — skipping Consumer.__exit__
    # and leaving offsets unsaved. If the signal arrives during setup
    # (reconfigure, brooklet.open, stream.consume) before the handler is
    # registered, resume-across-restarts silently breaks. Register first so
    # every subsequent line is covered.
    #
    # Note: there is still an untestable micro-race between process start
    # and this line, but it is bounded to a few Python bytecodes. The
    # integration assertion in test_watch_saves_offset_on_sigterm pins
    # post-setup behavior (full offset catch-up on SIGTERM).
    def _sigterm_to_interrupt(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _sigterm_to_interrupt)

    try:
        # Monitor captures stdout via a pipe; Python defaults to block
        # buffering on pipes, which would hide events until the buffer fills.
        # This call is essential, not cosmetic.
        sys.stdout.reconfigure(line_buffering=True)

        stream = brooklet.open(stream_dir)
        try:
            consumer_ctx = stream.consume(topic, group=group, follow=True)
        except KeyError:
            typer.echo(f"Error: topic {topic!r} is not registered", err=True)
            raise typer.Exit(code=1) from None

        with consumer_ctx as consumer:
            _watch_impl(consumer, sys.stdout)
    except KeyboardInterrupt:
        # SIGTERM/Ctrl-C during setup or runtime — the `with` block above
        # (if entered) already ran Consumer.__exit__ and saved offsets.
        pass


@app.command(rich_help_panel="Core Commands")
def cat(
    topic: Annotated[str, typer.Argument(help="Topic name to read.")],
    stream_dir: STREAM_DIR_OPTION = Path("."),
) -> None:
    """Dump all events from a topic without advancing offsets (read-only)."""
    stream = brooklet.open(stream_dir)
    try:
        source = stream._registry.get(topic)
    except KeyError:
        typer.echo(f"Error: topic {topic!r} is not registered", err=True)
        raise typer.Exit(code=1) from None

    import glob as glob_module

    from brooklet.core.envelope import SeqTracker

    file_path = source["path"]
    file_mode = source["mode"]

    filepaths = sorted(glob_module.glob(file_path)) if file_mode == "glob" else [file_path]

    # One tracker spans every segment file so legacy/external lines without a
    # persisted _seq stay monotonic across files (same contract as Consumer).
    tracker = SeqTracker(source=topic)
    for fp in filepaths:
        try:
            with open(fp) as f:
                for line in f:
                    event = tracker.wrap(line)
                    if event is not None:
                        typer.echo(json.dumps(event))
        except OSError as e:
            typer.echo(f"Warning: cannot read {fp}: {e}", err=True)


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
    with otel.tracer.start_as_current_span("brooklet-cli"):
        app()
