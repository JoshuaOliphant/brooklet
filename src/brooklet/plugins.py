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


def get_plugin_manager() -> pluggy.PluginManager:
    pm = pluggy.PluginManager("brooklet")
    pm.add_hookspecs(BrookletSpec)
    from brooklet.contrib.claude_analytics import ScoutPlugin
    from brooklet.contrib.pytest_analytics import PytestPlugin

    pm.register(ScoutPlugin())
    pm.register(PytestPlugin())
    pm.load_setuptools_entrypoints("brooklet")
    return pm
