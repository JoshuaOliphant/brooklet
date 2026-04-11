# ABOUTME: Unit tests for the pure compact-line formatter used by `brooklet watch`
# ABOUTME: Validates Monitor-compatible one-line-per-event output shape

from brooklet.watch_format import format_event


def test_format_basic_event():
    event = {"_seq": 1, "_ts": "2026-04-10T14:03:22Z", "type": "ping", "n": 42}
    assert format_event(event) == "#1 14:03:22 type=ping n=42"


def test_format_skips_envelope_fields():
    event = {
        "_seq": 7,
        "_ts": "2026-04-10T14:03:22Z",
        "_src": "producer",
        "foo": "bar",
    }
    result = format_event(event)
    # Envelope fields must not appear in the body (but #7 prefix is OK)
    assert "_ts=" not in result
    assert "_seq=" not in result
    assert "_src=" not in result
    assert "foo=bar" in result
    assert result.startswith("#7 ")


def test_format_skips_none_values():
    event = {
        "_seq": 1,
        "_ts": "2026-04-10T14:03:22Z",
        "a": None,
        "b": "present",
    }
    result = format_event(event)
    assert "a=" not in result
    assert "b=present" in result


def test_format_nested_dict_compact():
    event = {
        "_seq": 1,
        "_ts": "2026-04-10T14:03:22Z",
        "stats": {"passed": 10, "failed": 2},
    }
    result = format_event(event)
    assert "stats={passed:10,failed:2}" in result


def test_format_list_value_compact():
    event = {
        "_seq": 1,
        "_ts": "2026-04-10T14:03:22Z",
        "errors": ["a", "b", "c"],
    }
    result = format_event(event)
    assert "errors=[...3 items]" in result


def test_format_truncates_long_line():
    event = {
        "_seq": 1,
        "_ts": "2026-04-10T14:03:22Z",
        "data": "x" * 500,
    }
    result = format_event(event, max_len=50)
    assert len(result) <= 50
    assert result.endswith("…")


def test_format_strips_control_chars_in_values():
    event = {
        "_seq": 1,
        "_ts": "2026-04-10T14:03:22Z",
        "msg": "a\nb\tc\rd",
    }
    result = format_event(event)
    # A stray newline would split one event into two Monitor notifications —
    # control chars must be scrubbed to spaces.
    assert "\n" not in result
    assert "\t" not in result
    assert "\r" not in result
    assert "msg=a b c d" in result


def test_format_handles_bool_int_float_str():
    event = {
        "_seq": 1,
        "_ts": "2026-04-10T14:03:22Z",
        "flag": True,
        "n": 42,
        "pi": 3.14,
        "s": "hi",
    }
    result = format_event(event)
    assert "flag=True" in result
    assert "n=42" in result
    assert "pi=3.14" in result
    assert "s=hi" in result


def test_format_missing_ts_fallback():
    # wrap() always injects _ts, but be defensive — missing or unparseable
    # shouldn't crash the formatter.
    event = {"_seq": 1, "type": "x"}
    result = format_event(event)
    assert "??:??:??" in result
    assert "type=x" in result


def test_format_unparseable_ts_fallback():
    event = {"_seq": 1, "_ts": "not-a-timestamp", "type": "x"}
    result = format_event(event)
    assert "??:??:??" in result
    assert "type=x" in result


def test_format_unicode_value():
    event = {
        "_seq": 1,
        "_ts": "2026-04-10T14:03:22Z",
        "name": "café",
    }
    result = format_event(event)
    assert "name=café" in result


def test_format_missing_seq_fallback():
    # Defensive: wrap() always sets _seq, but formatter shouldn't crash.
    event = {"_ts": "2026-04-10T14:03:22Z", "x": 1}
    result = format_event(event)
    assert result.startswith("#?")
    assert "x=1" in result


def test_format_empty_body():
    # Only envelope fields — prefix should still render cleanly.
    event = {"_seq": 5, "_ts": "2026-04-10T14:03:22Z"}
    result = format_event(event)
    assert result == "#5 14:03:22"
