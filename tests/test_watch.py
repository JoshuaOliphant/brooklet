# ABOUTME: Tests for the `brooklet watch` CLI command — compact tailing for Monitor
# ABOUTME: Covers _watch_impl unit tests plus a subprocess smoke test for real buffering

import io
import json
import queue
import shutil
import subprocess
import sys
import threading
from pathlib import Path

import pytest
from typer.testing import CliRunner

import brooklet
from brooklet.cli import _watch_impl, app

runner = CliRunner()


# ---------------------------------------------------------------------------
# _watch_impl unit tests — decoupled from real Consumer/follow-mode
# ---------------------------------------------------------------------------


def test_watch_impl_formats_events():
    """_watch_impl writes one compact line per event to the given output."""
    events = [
        {"_seq": 1, "_ts": "2026-04-10T14:03:22Z", "type": "a", "n": 1},
        {"_seq": 2, "_ts": "2026-04-10T14:03:23Z", "type": "b", "n": 2},
    ]
    buf = io.StringIO()
    _watch_impl(iter(events), buf)
    lines = buf.getvalue().splitlines()
    assert len(lines) == 2
    assert lines[0] == "#1 14:03:22 type=a n=1"
    assert lines[1] == "#2 14:03:23 type=b n=2"


def test_watch_impl_flushes_after_each_event():
    """Each event must be flushed so Monitor sees it immediately."""
    flush_count = 0

    class CountingBuf(io.StringIO):
        def flush(self) -> None:
            nonlocal flush_count
            flush_count += 1
            super().flush()

    events = [
        {"_seq": 1, "_ts": "2026-04-10T14:03:22Z", "type": "a"},
        {"_seq": 2, "_ts": "2026-04-10T14:03:23Z", "type": "b"},
        {"_seq": 3, "_ts": "2026-04-10T14:03:24Z", "type": "c"},
    ]
    _watch_impl(iter(events), CountingBuf())
    # At least one flush per event — pipe-buffered stdout would hide events
    # from Monitor without explicit flushing.
    assert flush_count >= 3


def test_watch_impl_empty_iterator_writes_nothing():
    buf = io.StringIO()
    _watch_impl(iter([]), buf)
    assert buf.getvalue() == ""


def test_watch_impl_isolates_per_event_format_errors(capsys):
    """A bad event must not kill the stream — emit a fallback line and continue.

    For a long-running Monitor watcher, one event with a broken value (e.g. a
    non-dict payload slipping through, or a field whose ``__repr__`` raises)
    should never take down the entire tail. The bad event gets a fallback
    error line, and the surrounding good events still render normally.
    """

    class BoomRepr:
        def __repr__(self) -> str:
            raise RuntimeError("boom")

    good_a = {"_seq": 1, "_ts": "2026-04-10T14:03:22Z", "type": "a"}
    # A non-dict slipping in would cause format_event to AttributeError on
    # `.items()` — exactly the kind of drift we want to survive.
    bad = "this is not an event dict"
    good_b = {"_seq": 3, "_ts": "2026-04-10T14:03:24Z", "type": "c"}
    # Also exercise the broken-__repr__ path via a dict with a poisoned value.
    bad_repr = {"_seq": 2, "_ts": "2026-04-10T14:03:23Z", "payload": BoomRepr()}

    buf = io.StringIO()
    _watch_impl(iter([good_a, bad, bad_repr, good_b]), buf)

    lines = buf.getvalue().splitlines()
    assert len(lines) == 4, f"expected 4 output lines (none silently skipped), got {lines}"

    # Good events render normally.
    assert lines[0] == "#1 14:03:22 type=a"
    assert lines[3] == "#3 14:03:24 type=c"

    # Bad events produce a fallback error line that still fits the
    # one-line-per-event Monitor contract.
    assert "format error" in lines[1]
    assert "format error" in lines[2]
    assert "\n" not in lines[1]
    assert "\n" not in lines[2]

    # The full error goes to stderr so the user can diagnose.
    err = capsys.readouterr().err
    assert "format error" in err or "AttributeError" in err or "RuntimeError" in err


# ---------------------------------------------------------------------------
# CliRunner tests
# ---------------------------------------------------------------------------


def test_watch_missing_topic_exits_nonzero(tmp_path):
    result = runner.invoke(
        app,
        ["watch", "nonexistent", "--stream-dir", str(tmp_path)],
    )
    assert result.exit_code != 0
    assert "nonexistent" in result.output


# ---------------------------------------------------------------------------
# Subprocess smoke test — only way to validate real line-buffered stdout
# ---------------------------------------------------------------------------


def _find_brooklet_script() -> str | None:
    """Locate the brooklet CLI entry-point script for subprocess testing."""
    via_path = shutil.which("brooklet")
    if via_path:
        return via_path
    candidate = Path(sys.executable).parent / "brooklet"
    if candidate.exists():
        return str(candidate)
    return None


def _readline_with_timeout(stream, timeout: float) -> str:
    """Read one line from a subprocess stream with a timeout.

    Uses a reader thread + Queue so tests fail cleanly instead of hanging
    when stdout buffering is broken.
    """
    q: queue.Queue = queue.Queue()

    def reader() -> None:
        line = stream.readline()
        q.put(line)

    threading.Thread(target=reader, daemon=True).start()
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        raise TimeoutError(f"no line received in {timeout}s") from None


def test_watch_subprocess_line_buffered(tmp_path):
    """Spawn `brooklet watch` as a subprocess and verify real line-buffered
    output reaches the reader before the process exits.

    This is the only way to validate Python stdout buffering behavior under a
    pipe — CliRunner does not exercise real stdout. If `reconfigure(
    line_buffering=True)` is missing, this test will time out instead of
    hanging forever.
    """
    brooklet_script = _find_brooklet_script()
    if brooklet_script is None:
        pytest.skip("brooklet CLI script not found on PATH")

    stream = brooklet.open(tmp_path)
    stream.produce("smoke", {"type": "hello", "n": 1})

    proc = subprocess.Popen(
        [
            brooklet_script,
            "watch",
            "smoke",
            "--stream-dir",
            str(tmp_path),
            "--group",
            "smoke-test",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # First event was produced before the subprocess started — it should
        # arrive via the initial read of the topic file.
        line1 = _readline_with_timeout(proc.stdout, timeout=10.0)
        assert line1, "no line read from subprocess stdout"
        assert "type=hello" in line1
        assert "n=1" in line1

        # Produce a second event live — verify it arrives via follow mode
        # and is flushed to stdout before the process exits.
        stream.produce("smoke", {"type": "ping", "n": 2})
        line2 = _readline_with_timeout(proc.stdout, timeout=10.0)
        assert "type=ping" in line2
        assert "n=2" in line2
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def test_watch_saves_offset_on_sigterm(tmp_path):
    """SIGTERM (what Monitor's TaskStop sends) must unwind the Consumer
    context manager so offsets are saved to disk.

    Without an explicit signal handler, Python exits on SIGTERM without
    running `__exit__`, which would leave `close()` uncalled and offsets
    unsaved — defeating the resumability that is the whole reason `watch`
    exists as a separate command from `consume --follow`.
    """
    brooklet_script = _find_brooklet_script()
    if brooklet_script is None:
        pytest.skip("brooklet CLI script not found on PATH")

    stream = brooklet.open(tmp_path)
    stream.produce("sigterm", {"type": "a", "n": 1})
    stream.produce("sigterm", {"type": "b", "n": 2})

    proc = subprocess.Popen(
        [
            brooklet_script,
            "watch",
            "sigterm",
            "--stream-dir",
            str(tmp_path),
            "--group",
            "sigterm-test",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(tmp_path),  # work around the pre-existing relative-path bug
    )
    try:
        # Drain both initial events so the Consumer has advanced the file
        # position past them.
        _readline_with_timeout(proc.stdout, timeout=10.0)
        _readline_with_timeout(proc.stdout, timeout=10.0)

        # Send SIGTERM — simulates Monitor's TaskStop.
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        proc.kill()
        proc.wait()
        raise

    offset_file = tmp_path / ".brooklet" / "offsets" / "sigterm-test-sigterm.json"
    assert offset_file.exists(), (
        f"offset file not saved on SIGTERM at {offset_file} — resumability is broken"
    )
    data = json.loads(offset_file.read_text())
    # Require full catch-up, not just "something saved". A partial offset
    # would pass `> 0` but still break resumability on restart.
    # Offsets use GlobOffset encoding: segment_number * 10**18 + byte_offset.
    # Decode to get the active segment number and byte position within it.
    scale = 10**18
    raw_offset = data["offset"]
    seg_num = raw_offset // scale
    byte_offset = raw_offset % scale
    segments = sorted((tmp_path / "sigterm").glob("data-*.jsonl"))
    assert segments, "Expected at least one segment file"
    active_seg = segments[seg_num - 1] if seg_num > 0 else segments[0]
    file_size = active_seg.stat().st_size
    assert byte_offset == file_size, (
        f"expected byte offset to equal active segment size after full catch-up, "
        f"got byte_offset={byte_offset}, file_size={file_size}"
    )
