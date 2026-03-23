# ABOUTME: Plugin system for brooklet CLI — hookspec definitions and plugin manager
# ABOUTME: Uses pluggy for discovery of built-in and third-party command plugins

from __future__ import annotations

import pluggy

hookspec = pluggy.HookspecMarker("brooklet")
hookimpl = pluggy.HookimplMarker("brooklet")


class BrookletSpec:
    @hookspec
    def brooklet_commands(self, cli) -> None:
        """Register subcommands on the brooklet Typer app."""


_pm: pluggy.PluginManager | None = None


def get_plugin_manager() -> pluggy.PluginManager:
    """Return the singleton plugin manager, creating it on first call."""
    global _pm
    if _pm is not None:
        return _pm
    _pm = pluggy.PluginManager("brooklet")
    _pm.add_hookspecs(BrookletSpec)
    from brooklet.contrib.claude_analytics import ScoutPlugin
    from brooklet.contrib.pytest_analytics import PytestPlugin

    _pm.register(ScoutPlugin())
    _pm.register(PytestPlugin())
    _pm.load_setuptools_entrypoints("brooklet")
    return _pm


def reset_plugin_manager() -> None:
    """Reset the singleton plugin manager. Used by tests to avoid state leaks."""
    global _pm
    _pm = None
