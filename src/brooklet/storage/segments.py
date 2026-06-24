# ABOUTME: Owns the data-NNNN.jsonl segment-file naming convention for local topics
# ABOUTME: Single source of truth shared by the producer (stream) and consumer

import re
from pathlib import Path

# Local topics store events in zero-padded, monotonically numbered segment
# files (e.g. data-0003.jsonl). This module is the only place that knows that
# convention — the producer that writes segments and the consumer that reads
# them both go through here so the format can never drift between the two.

GLOB = "data-*.jsonl"
_SEGMENT_RE = re.compile(r"data-(\d+)\.jsonl$")


def filename(number: int) -> str:
    """Return the segment filename for a given segment number (zero-padded)."""
    return f"data-{number:04d}.jsonl"


def parse_number(filepath: str) -> int | None:
    """Extract the segment number from a data-NNNN.jsonl path.

    Returns None for files that don't follow the segment naming convention.
    """
    m = _SEGMENT_RE.search(filepath)
    return int(m.group(1)) if m else None


def glob_pattern(topic_dir: Path) -> str:
    """Return the glob pattern matching every segment file under a topic dir."""
    return str(topic_dir / GLOB)
