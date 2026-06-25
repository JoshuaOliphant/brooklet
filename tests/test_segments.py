# ABOUTME: Tests for the segment-file naming convention module
# ABOUTME: Verifies filename formatting, number parsing, and glob pattern building

from pathlib import Path

from brooklet.storage import segments


class TestFilename:
    def test_zero_pads_to_four_digits(self):
        assert segments.filename(1) == "data-0001.jsonl"
        assert segments.filename(42) == "data-0042.jsonl"

    def test_does_not_truncate_large_numbers(self):
        assert segments.filename(12345) == "data-12345.jsonl"


class TestParseNumber:
    def test_parses_segment_number(self):
        assert segments.parse_number("/some/path/data-0003.jsonl") == 3

    def test_parses_bare_filename(self):
        assert segments.parse_number("data-0007.jsonl") == 7

    def test_returns_none_for_non_segment(self):
        assert segments.parse_number("data.jsonl") is None
        assert segments.parse_number("/path/other.jsonl") is None

    def test_roundtrips_with_filename(self):
        assert segments.parse_number(segments.filename(99)) == 99


class TestGlobPattern:
    def test_builds_pattern_under_topic_dir(self):
        assert segments.glob_pattern(Path("/streams/events")) == "/streams/events/data-*.jsonl"
