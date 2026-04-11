# ABOUTME: Compact one-line-per-event formatter for `brooklet watch`
# ABOUTME: Pure function suitable for Claude Code Monitor-compatible output

from __future__ import annotations

from datetime import datetime
from typing import Any

from brooklet.types import Event

_ENVELOPE_FIELDS = frozenset({"_ts", "_seq", "_src"})
_TIME_FALLBACK = "??:??:??"


def _scrub(text: str) -> str:
    """Replace control characters that would break the one-line contract."""
    return text.replace("\n", " ").replace("\r", " ").replace("\t", " ")


def _render_scalar(value: Any) -> str:
    if isinstance(value, bool):
        # bool check must come before int (bool is an int subclass)
        return str(value)
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return _scrub(value)
    return _scrub(repr(value))


def _render_dict_compact(d: dict[str, Any]) -> str:
    """One level deep only — nested dicts/lists collapse to '{...}'/'[...]'."""
    parts: list[str] = []
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, dict):
            parts.append(f"{k}:{{...}}")
        elif isinstance(v, list):
            parts.append(f"{k}:[...]")
        else:
            parts.append(f"{k}:{_render_scalar(v)}")
    return "{" + ",".join(parts) + "}"


def _render_value(value: Any) -> str | None:
    """Render a top-level field value, or return None to skip."""
    if value is None:
        return None
    if isinstance(value, dict):
        return _render_dict_compact(value)
    if isinstance(value, list):
        return f"[...{len(value)} items]"
    return _render_scalar(value)


def _extract_time(event: Event) -> str:
    raw = event.get("_ts")
    if not isinstance(raw, str):
        return _TIME_FALLBACK
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return _TIME_FALLBACK
    return dt.strftime("%H:%M:%S")


def _extract_seq(event: Event) -> str:
    seq = event.get("_seq")
    if isinstance(seq, int):
        return str(seq)
    return "?"


def format_event(event: Event, max_len: int = 200) -> str:
    """Format an event as a single compact line for Monitor-style consumption.

    Shape: ``#<seq> <HH:MM:SS> key=val key=val …``

    - Envelope fields (``_ts``, ``_seq``, ``_src``) are lifted into the prefix
      and skipped from the body.
    - ``None`` values are skipped.
    - ``dict`` values render their top-level keys as ``key:value`` pairs inside
      braces — e.g. ``meta={host:web1,port:8080}``. Any dict or list nested
      inside that top level is collapsed to the literal string ``{...}`` or
      ``[...]`` (so ``{"a": {"b": 1}}`` becomes ``{a:{...}}``).
    - ``list`` values render as the literal ``key=[...N items]`` where the
      three dots are printed verbatim and ``N`` is ``len(value)`` — e.g. a
      three-element list becomes ``items=[...3 items]``.
    - Newlines (``\\n``), carriage returns (``\\r``), and tabs (``\\t``) in
      string values are replaced with spaces — a stray newline would otherwise
      split one event into two Monitor notifications.
    - The final line is truncated to ``max_len`` characters with ``…`` suffix.
    """
    seq = _extract_seq(event)
    time_str = _extract_time(event)

    body_parts: list[str] = []
    for key, value in event.items():
        if key in _ENVELOPE_FIELDS:
            continue
        rendered = _render_value(value)
        if rendered is None:
            continue
        body_parts.append(f"{key}={rendered}")

    prefix = f"#{seq} {time_str}"
    line = f"{prefix} {' '.join(body_parts)}" if body_parts else prefix

    if len(line) > max_len:
        # Leave room for the single-char ellipsis.
        line = line[: max_len - 1] + "…"
    return line
