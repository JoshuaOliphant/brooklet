# ABOUTME: Tests for the pytest analytics Typer plugin commands
# ABOUTME: Verifies PytestPlugin registers and delegates to scan_runs correctly

import typer
from typer.testing import CliRunner

from tests.pytest_fixtures import SINGLE_RUN_EVENTS, write_run_file

runner = CliRunner()


def test_pytest_plugin_registers_commands():
    from brooklet.contrib.pytest_analytics import PytestPlugin

    app = typer.Typer()
    plugin = PytestPlugin()
    plugin.brooklet_commands(cli=app)
    result = runner.invoke(app, ["pytest", "--help"])
    assert result.exit_code == 0
    assert "scan" in result.output


def test_pytest_scan_with_fixture(tmp_path):
    from brooklet.cli import app

    write_run_file(tmp_path, "test-run", SINGLE_RUN_EVENTS)
    report_path = tmp_path / "test-run.jsonl"
    result = runner.invoke(app, ["pytest", "scan", str(report_path)])
    assert result.exit_code == 0
    assert "test-run" in result.output
