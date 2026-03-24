# ABOUTME: Tests for brooklet plugin system — hookspec and plugin discovery
# ABOUTME: Verifies built-in and third-party plugins register commands correctly

import pytest
import typer
from typer.testing import CliRunner

from brooklet.plugins import (  # noqa: F401
    BrookletSpec,
    get_plugin_manager,
    hookimpl,
    reset_plugin_manager,
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def _clean_plugin_manager():
    """Reset plugin manager singleton after each test to prevent state leaks."""
    yield
    reset_plugin_manager()


def test_get_plugin_manager_returns_manager():
    pm = get_plugin_manager()
    assert pm is not None
    assert pm.get_plugins()


def test_builtin_plugins_registered():
    pm = get_plugin_manager()
    plugin_names = [type(p).__name__ for p in pm.get_plugins()]
    assert "ScoutPlugin" in plugin_names
    assert "PytestPlugin" in plugin_names


def test_hookimpl_is_reexported():
    assert callable(hookimpl)


def test_third_party_plugin_registers_command():
    # Use a callback so Typer treats this as a multi-command app
    app = typer.Typer(invoke_without_command=True)

    @app.callback()
    def main():
        pass

    class MockPlugin:
        @hookimpl
        def brooklet_commands(self, cli):
            @cli.command(name="mock-cmd")
            def mock_cmd():
                print("mock output")

    pm = get_plugin_manager()
    pm.register(MockPlugin())
    pm.hook.brooklet_commands(cli=app)

    result = runner.invoke(app, ["mock-cmd"])
    assert result.exit_code == 0
    assert "mock output" in result.output


def test_scout_plugin_registers_commands():
    from brooklet.contrib.claude_analytics import ScoutPlugin

    app = typer.Typer()
    plugin = ScoutPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["scout", "--help"])
    assert result.exit_code == 0
    assert "scan" in result.output


def test_scout_scan_delegates_to_scan_sessions(session_dir):
    from brooklet.cli import app

    result = runner.invoke(app, ["scout", "scan", str(session_dir)])
    assert result.exit_code == 0
    assert "session" in result.output.lower() or "events" in result.output.lower()


def test_scout_scan_output_uses_stream_dir(session_dir, tmp_path):
    """scout scan --output writes to --stream-dir, not the sessions directory."""
    import brooklet
    from brooklet.cli import app

    stream_dir = tmp_path / "streams"
    stream_dir.mkdir()

    result = runner.invoke(
        app,
        [
            "scout",
            "scan",
            str(session_dir),
            "--output",
            "scout/stats",
            "--stream-dir",
            str(stream_dir),
        ],
    )
    assert result.exit_code == 0

    # Topic should be in the stream dir, not the sessions dir
    stream = brooklet.open(stream_dir)
    assert "scout/stats" in stream.topics()

    # Should NOT have created .brooklet in the sessions dir
    assert (
        not (session_dir / ".brooklet" / "sources.json").exists()
        or "scout/stats" not in brooklet.open(session_dir).topics()
    )


def test_pytest_scan_output_uses_stream_dir(tmp_path):
    """pytest scan --output writes to --stream-dir, not the report file's parent."""
    import brooklet
    from brooklet.cli import app
    from tests.pytest_fixtures import SINGLE_RUN_EVENTS, write_run_file

    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    write_run_file(report_dir, "test-run", SINGLE_RUN_EVENTS)
    report_path = report_dir / "test-run.jsonl"

    stream_dir = tmp_path / "streams"
    stream_dir.mkdir()

    result = runner.invoke(
        app,
        [
            "pytest",
            "scan",
            str(report_path),
            "--output",
            "pytest/summaries",
            "--stream-dir",
            str(stream_dir),
        ],
    )
    assert result.exit_code == 0

    stream = brooklet.open(stream_dir)
    assert "pytest/summaries" in stream.topics()


def test_cli_help_shows_core_and_plugin_commands():
    """brooklet --help shows core commands and plugin subcommands."""
    from brooklet.cli import app

    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    # Core commands
    assert "register" in result.output
    assert "produce" in result.output
    assert "consume" in result.output
    assert "topics" in result.output
    # Plugin commands
    assert "scout" in result.output
    assert "pytest" in result.output
