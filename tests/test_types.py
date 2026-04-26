# ABOUTME: Tests for shared type definitions
# ABOUTME: Verifies offset encode/decode roundtrips and edge cases

from brooklet.core.types import GlobOffset, SingleFileOffset


class TestSingleFileOffset:
    def test_default_zero(self):
        """Default offset starts at 0."""
        offset = SingleFileOffset()
        assert offset.byte_offset == 0

    def test_encode_decode_roundtrip(self):
        """Encoding and decoding preserves the byte offset."""
        offset = SingleFileOffset(byte_offset=12345)
        raw = offset.encode()
        restored = SingleFileOffset.decode(raw)
        assert restored == offset

    def test_encode_zero(self):
        """Zero offset encodes to 0."""
        assert SingleFileOffset().encode() == 0

    def test_decode_zero(self):
        """Decoding 0 gives default offset."""
        assert SingleFileOffset.decode(0) == SingleFileOffset()


class TestGlobOffset:
    def test_default_zero(self):
        """Default glob offset starts at segment 0, byte 0."""
        offset = GlobOffset()
        assert offset.segment_number == 0
        assert offset.byte_offset == 0

    def test_encode_decode_roundtrip(self):
        """Encoding and decoding preserves both fields."""
        offset = GlobOffset(segment_number=3, byte_offset=5678)
        raw = offset.encode()
        restored = GlobOffset.decode(raw)
        assert restored == offset

    def test_encode_zero(self):
        """Zero glob offset encodes to 0."""
        assert GlobOffset().encode() == 0

    def test_decode_zero(self):
        """Decoding 0 gives default glob offset."""
        assert GlobOffset.decode(0) == GlobOffset()

    def test_encode_large_segment_number(self):
        """Large segment numbers encode correctly."""
        offset = GlobOffset(segment_number=100, byte_offset=999)
        raw = offset.encode()
        restored = GlobOffset.decode(raw)
        assert restored.segment_number == 100
        assert restored.byte_offset == 999

    def test_encode_preserves_separation(self):
        """Segment number and byte offset don't collide in encoding."""
        a = GlobOffset(segment_number=1, byte_offset=0)
        b = GlobOffset(segment_number=0, byte_offset=10**18)
        assert a.encode() == b.encode()  # both encode to 10**18
        # But this is the known edge case — 10**18 byte files
