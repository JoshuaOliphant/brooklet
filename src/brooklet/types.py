# ABOUTME: Shared type definitions for brooklet
# ABOUTME: Defines Mode literal, offset dataclasses, SourceDef, and Event type alias

from dataclasses import dataclass, field
from typing import Any, Literal, NotRequired, TypedDict

Mode = Literal["single-file", "glob"]

Event = dict[str, Any]


class SourceDef(TypedDict):
    """Schema for a registered source in the registry."""

    path: str
    mode: Mode


class EnvelopeMeta(TypedDict):
    """Guaranteed envelope fields injected by brooklet on read."""

    _ts: str
    _seq: int
    _src: NotRequired[str]


@dataclass
class SingleFileOffset:
    """Offset state for a single-file consumer."""

    byte_offset: int = 0

    def encode(self) -> int:
        """Encode to a single integer for storage."""
        return self.byte_offset

    @classmethod
    def decode(cls, raw: int) -> "SingleFileOffset":
        """Decode from stored integer."""
        return cls(byte_offset=raw)


@dataclass
class GlobOffset:
    """Offset state for a glob-pattern consumer.

    Encodes segment_number and byte_offset into a single integer using
    segment_number * 10**18 + byte_offset for storage.

    For local topics, segment_number is the numeric value parsed from
    data-NNNN.jsonl filenames. For external glob sources that don't follow
    that naming convention, segment_number is a positional file index.
    """

    segment_number: int = 0
    byte_offset: int = 0

    _SCALE: int = field(default=10**18, init=False, repr=False)

    def encode(self) -> int:
        """Encode to a single integer for storage."""
        return self.segment_number * self._SCALE + self.byte_offset

    @classmethod
    def decode(cls, raw: int) -> "GlobOffset":
        """Decode from stored integer."""
        scale = 10**18
        return cls(segment_number=raw // scale, byte_offset=raw % scale)
