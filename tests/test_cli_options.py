# ABOUTME: Tests for the shared contrib CLI option definitions
# ABOUTME: Verifies the reusable --stream-dir Typer option parses the flag and BROOKLET_DIR env

import typer
from typer.testing import CliRunner

from brooklet.contrib.cli_options import StreamDirOption

runner = CliRunner()


def _app() -> typer.Typer:
    app = typer.Typer()

    @app.command()
    def cmd(stream_dir: StreamDirOption = None) -> None:
        typer.echo(f"stream_dir={stream_dir}")

    return app


def test_stream_dir_defaults_to_none():
    result = runner.invoke(_app(), [])
    assert result.exit_code == 0
    assert "stream_dir=None" in result.stdout


def test_stream_dir_from_flag():
    result = runner.invoke(_app(), ["--stream-dir", "/tmp/streams"])
    assert result.exit_code == 0
    assert "stream_dir=/tmp/streams" in result.stdout


def test_stream_dir_from_envvar():
    result = runner.invoke(_app(), [], env={"BROOKLET_DIR": "/tmp/from-env"})
    assert result.exit_code == 0
    assert "stream_dir=/tmp/from-env" in result.stdout
