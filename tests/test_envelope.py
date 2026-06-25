# ABOUTME: Tests for the envelope module — thin metadata auto-injection
# ABOUTME: Verifies _ts, _seq, _src fields are added without clobbering existing values

import json

from brooklet.core.envelope import SeqTracker, wrap


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

    def test_wrap_preserves_existing_seq(self):
        """A persisted _seq is preserved; the seq param is only a fallback.

        _seq is topic-monotonic, assigned once at produce time. wrap() must not
        clobber it on read, or a gapless resume would renumber from the per-run
        counter instead of the true topic position (brooklet-a2c).
        """
        line = json.dumps({"_seq": 999, "type": "hello"})
        result = wrap(line, seq=5)

        assert result["_seq"] == 999

    def test_wrap_seq_fallback_when_absent(self):
        """When the line carries no _seq, wrap() falls back to the seq param.

        Covers legacy/external JSONL produced outside brooklet (AC-6): derive
        gracefully from the supplied counter rather than crashing or omitting.
        """
        line = json.dumps({"type": "hello"})
        result = wrap(line, seq=5)

        assert result["_seq"] == 5

    def test_wrap_non_int_seq_falls_back(self):
        """A persisted _seq that is not a valid int gets the fallback, not garbage.

        The EnvelopeMeta contract is _seq: int. An external/legacy line carrying
        a non-int _seq (e.g. a string) must not flow through untouched — wrap()
        treats it as having no usable persisted _seq and uses the supplied seq.
        """
        line = json.dumps({"_seq": "oops", "type": "hello"})
        result = wrap(line, seq=7)

        assert result["_seq"] == 7

    def test_wrap_bool_seq_falls_back(self):
        """A bool _seq is rejected too — bool is an int subclass but not a seq."""
        line = json.dumps({"_seq": True, "type": "hello"})
        result = wrap(line, seq=9)

        assert result["_seq"] == 9

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


class TestSeqTracker:
    def test_assigns_incrementing_fallback_for_legacy_lines(self):
        """Lines without a persisted _seq get a 1-based incrementing fallback."""
        tracker = SeqTracker()
        a = tracker.wrap(json.dumps({"type": "a"}))
        b = tracker.wrap(json.dumps({"type": "b"}))

        assert a["_seq"] == 1
        assert b["_seq"] == 2

    def test_preserves_persisted_seq(self):
        """A valid persisted _seq flows through unchanged."""
        tracker = SeqTracker()
        result = tracker.wrap(json.dumps({"type": "a", "_seq": 42}))

        assert result["_seq"] == 42

    def test_advances_high_water_mark_past_persisted_seq(self):
        """A legacy line after a persisted _seq is numbered above it, not from position."""
        tracker = SeqTracker()
        tracker.wrap(json.dumps({"type": "a", "_seq": 100}))
        legacy = tracker.wrap(json.dumps({"type": "b"}))  # no _seq

        # Without high-water tracking this would be 2; it must exceed 100.
        assert legacy["_seq"] == 101

    def test_passes_source_through(self):
        """The configured source is applied as _src when the line lacks one."""
        tracker = SeqTracker(source="topic-x")
        result = tracker.wrap(json.dumps({"type": "a"}))

        assert result["_src"] == "topic-x"

    def test_invalid_line_returns_none_and_still_counts(self):
        """A malformed line yields None; the counter still advanced for it."""
        tracker = SeqTracker()
        assert tracker.wrap("not json") is None
        # The next valid line is numbered after the consumed slot.
        result = tracker.wrap(json.dumps({"type": "a"}))
        assert result["_seq"] == 2
