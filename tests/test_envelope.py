# ABOUTME: Tests for the envelope module — thin metadata auto-injection
# ABOUTME: Verifies _ts, _seq, _src fields are added without clobbering existing values

import json

from brooklet.envelope import wrap


class TestWrap:
    def test_wrap_adds_missing_fields(self):
        """Bare JSON gets _ts, _seq, _src added."""
        line = json.dumps({"type": "hello", "message": "world"})
        result = wrap(line, seq=1, source="test-source")

        assert result is not None
        assert result["type"] == "hello"
        assert result["message"] == "world"
        assert "_ts" in result
        assert result["_seq"] == 1
        assert result["_src"] == "test-source"

    def test_wrap_preserves_existing_ts(self):
        """Existing _ts is not clobbered."""
        line = json.dumps({"_ts": "2026-01-01T00:00:00Z", "type": "hello"})
        result = wrap(line, seq=1)

        assert result["_ts"] == "2026-01-01T00:00:00Z"

    def test_wrap_preserves_existing_src(self):
        """Existing _src is not clobbered even when source param is given."""
        line = json.dumps({"_src": "original-producer", "type": "hello"})
        result = wrap(line, seq=1, source="override-attempt")

        assert result["_src"] == "original-producer"

    def test_wrap_invalid_json_returns_none(self):
        """Malformed JSON line returns None."""
        result = wrap("not valid json {{{", seq=1)
        assert result is None

    def test_wrap_empty_line_returns_none(self):
        """Empty string returns None."""
        result = wrap("", seq=1)
        assert result is None

    def test_wrap_seq_always_set(self):
        """_seq is always set from the parameter, even if the line has one."""
        line = json.dumps({"_seq": 999, "type": "hello"})
        result = wrap(line, seq=5)

        # _seq is always overwritten by brooklet — it's the canonical offset key
        assert result["_seq"] == 5

    def test_wrap_source_none_no_src_field(self):
        """When source is None and line has no _src, no _src is added."""
        line = json.dumps({"type": "hello"})
        result = wrap(line, seq=1, source=None)

        assert "_src" not in result

    def test_wrap_ts_is_iso_format(self):
        """Auto-injected _ts is a valid ISO 8601 string."""
        line = json.dumps({"type": "hello"})
        result = wrap(line, seq=1)

        # Should be parseable as ISO format
        from datetime import datetime

        datetime.fromisoformat(result["_ts"])
