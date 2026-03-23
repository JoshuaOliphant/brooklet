# ABOUTME: Tests for brooklet plugin system — hookspec and plugin discovery
# ABOUTME: Verifies built-in and third-party plugins register commands correctly

import typer
from typer.testing import CliRunner

from brooklet.plugins import BrookletSpec, get_plugin_manager, hookimpl  # noqa: F401

runner = CliRunner()


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
