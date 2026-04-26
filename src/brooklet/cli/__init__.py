# ABOUTME: CLI surface — Typer app, plugin discovery, watch output formatting
# ABOUTME: Lazily exposes app/main/_watch_impl so cli.plugins is importable without app load


def __getattr__(name: str):
    if name in {"app", "main", "_watch_impl"}:
        import brooklet.cli.app as _app_module

        return getattr(_app_module, name)
    raise AttributeError(f"module 'brooklet.cli' has no attribute {name!r}")
