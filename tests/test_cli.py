# ABOUTME: Tests for brooklet CLI core commands — register, produce, consume, topics
# ABOUTME: Uses Typer CliRunner to test commands without subprocess spawning

import json

from typer.testing import CliRunner

import brooklet
from brooklet.cli.app import app

runner = CliRunner()


def test_topics_empty(tmp_path):
    result = runner.invoke(app, ["topics", "--stream-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert result.output.strip() == ""


def test_topics_json_empty(tmp_path):
    result = runner.invoke(app, ["topics", "--stream-dir", str(tmp_path), "--json"])
    assert result.exit_code == 0
    assert json.loads(result.output) == []


def test_register_and_topics(tmp_path, sample_jsonl):
    result = runner.invoke(
        app,
        [
            "register",
            "my-events",
            str(sample_jsonl),
            "--stream-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(app, ["topics", "--stream-dir", str(tmp_path)])
    assert "my-events" in result.output


def test_register_glob_mode(tmp_path):
    result = runner.invoke(
        app,
        [
            "register",
            "all-logs",
            str(tmp_path / "*.jsonl"),
            "--stream-dir",
            str(tmp_path),
            "--mode",
            "glob",
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(app, ["topics", "--stream-dir", str(tmp_path), "--json"])
    topics = json.loads(result.output)
    assert "all-logs" in topics


def test_produce_reads_stdin(tmp_path):
    events = [
        json.dumps({"type": "hello", "value": 1}),
        json.dumps({"type": "world", "value": 2}),
    ]
    input_text = "\n".join(events) + "\n"

    result = runner.invoke(
        app,
        [
            "produce",
            "my-topic",
            "--stream-dir",
            str(tmp_path),
        ],
        input=input_text,
    )
    assert result.exit_code == 0

    data_files = sorted((tmp_path / "my-topic").glob("data-*.jsonl"))
    assert data_files
    all_lines = []
    for df in data_files:
        all_lines.extend(df.read_text().strip().split("\n"))
    assert len(all_lines) == 2
    for line in all_lines:
        event = json.loads(line)
        assert "_ts" in event
        assert "_seq" in event


def test_produce_skips_invalid_json(tmp_path):
    input_text = '{"valid": true}\nnot-json\n{"also": "valid"}\n'

    result = runner.invoke(
        app,
        [
            "produce",
            "my-topic",
            "--stream-dir",
            str(tmp_path),
        ],
        input=input_text,
    )
    assert result.exit_code == 0

    data_files = sorted((tmp_path / "my-topic").glob("data-*.jsonl"))
    assert data_files
    all_lines = []
    for df in data_files:
        all_lines.extend(df.read_text().strip().split("\n"))
    assert len(all_lines) == 2


def test_produce_with_source(tmp_path):
    input_text = json.dumps({"type": "test"}) + "\n"

    result = runner.invoke(
        app,
        [
            "produce",
            "my-topic",
            "--stream-dir",
            str(tmp_path),
            "--source",
            "my-app",
        ],
        input=input_text,
    )
    assert result.exit_code == 0

    data_files = sorted((tmp_path / "my-topic").glob("data-*.jsonl"))
    assert data_files
    event = json.loads(data_files[0].read_text().strip())
    assert event["_src"] == "my-app"


def test_consume_outputs_jsonl(tmp_path):
    stream = brooklet.open(tmp_path)
    stream.produce("test-topic", {"type": "a", "n": 1})
    stream.produce("test-topic", {"type": "b", "n": 2})

    result = runner.invoke(
        app,
        [
            "consume",
            "test-topic",
            "--group",
            "test-reader",
            "--stream-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0

    lines = result.output.strip().split("\n")
    assert len(lines) == 2
    for line in lines:
        event = json.loads(line)
        assert "type" in event


def test_consume_missing_topic(tmp_path):
    result = runner.invoke(
        app,
        [
            "consume",
            "nonexistent",
            "--group",
            "reader",
            "--stream-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code != 0


def test_produce_then_consume_roundtrip(tmp_path):
    events = [{"type": "ping", "seq": i} for i in range(3)]
    input_text = "\n".join(json.dumps(e) for e in events) + "\n"

    runner.invoke(
        app,
        [
            "produce",
            "roundtrip",
            "--stream-dir",
            str(tmp_path),
        ],
        input=input_text,
    )

    result = runner.invoke(
        app,
        [
            "consume",
            "roundtrip",
            "--group",
            "test",
            "--stream-dir",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0

    consumed = [json.loads(line) for line in result.output.strip().split("\n")]
    assert len(consumed) == 3
    for orig, got in zip(events, consumed, strict=True):
        assert got["type"] == orig["type"]
        assert got["seq"] == orig["seq"]


def test_consume_seq_is_topic_monotonic_after_gapless_resume(tmp_path):
    """AC-4: `consume` JSON output emits the persisted topic-monotonic _seq.

    Reproduces brooklet-a2c: produce 2, consume (saving the offset), produce 2
    more, consume again with the same group. The second consume must report
    _seq 3 and 4 — not 1 and 2 from a per-run reset.
    """

    def produce(payloads):
        runner.invoke(
            app,
            ["produce", "demo", "--stream-dir", str(tmp_path)],
            input="\n".join(json.dumps(p) for p in payloads) + "\n",
        )

    def consume():
        result = runner.invoke(
            app,
            ["consume", "demo", "--group", "g", "--stream-dir", str(tmp_path)],
        )
        assert result.exit_code == 0
        out = result.output.strip()
        return [json.loads(line) for line in out.split("\n")] if out else []

    produce([{"n": 1}, {"n": 2}])
    first = consume()
    assert [e["_seq"] for e in first] == [1, 2]

    produce([{"n": 3}, {"n": 4}])
    second = consume()
    assert [e["n"] for e in second] == [3, 4]
    assert [e["_seq"] for e in second] == [3, 4]


def test_version_flag(tmp_path):
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "brooklet" in result.output
    assert brooklet.__version__ in result.output


def test_cat_outputs_all_events(tmp_path):
    """cat dumps all events without advancing any offsets."""
    stream = brooklet.open(tmp_path)
    stream.produce("cat-topic", {"type": "a"})
    stream.produce("cat-topic", {"type": "b"})

    result = runner.invoke(app, ["cat", "cat-topic", "--stream-dir", str(tmp_path)])
    assert result.exit_code == 0

    lines = result.output.strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["type"] == "a"
    assert json.loads(lines[1])["type"] == "b"


def test_cat_does_not_advance_offsets(tmp_path):
    """cat is read-only — running it twice yields the same events."""
    stream = brooklet.open(tmp_path)
    stream.produce("cat-topic", {"type": "x"})

    result1 = runner.invoke(app, ["cat", "cat-topic", "--stream-dir", str(tmp_path)])
    result2 = runner.invoke(app, ["cat", "cat-topic", "--stream-dir", str(tmp_path)])
    assert result1.output == result2.output


def test_cat_missing_topic(tmp_path):
    result = runner.invoke(app, ["cat", "nonexistent", "--stream-dir", str(tmp_path)])
    assert result.exit_code != 0


def test_cat_mixed_topic_seq_is_monotonic(tmp_path):
    """cat numbering tracks the topic high-water mark across mixed sources.

    A persisted-_seq line followed by a legacy (no-_seq) line: the legacy line
    must get a _seq above the persisted value, not its position-in-the-file.
    """
    external = tmp_path / "external.jsonl"
    external.write_text(
        json.dumps({"_seq": 100, "type": "persisted"})
        + "\n"
        + json.dumps({"type": "legacy"})
        + "\n"
    )
    register = runner.invoke(
        app,
        ["register", "ext", str(external), "--stream-dir", str(tmp_path)],
    )
    assert register.exit_code == 0

    result = runner.invoke(app, ["cat", "ext", "--stream-dir", str(tmp_path)])
    assert result.exit_code == 0

    lines = result.output.strip().split("\n")
    seqs = [json.loads(line)["_seq"] for line in lines]
    assert seqs[0] == 100
    assert seqs[1] > 100


def test_stream_dir_env_var(tmp_path, monkeypatch):
    monkeypatch.setenv("BROOKLET_DIR", str(tmp_path))

    stream = brooklet.open(tmp_path)
    stream.produce("env-topic", {"type": "test"})

    result = runner.invoke(app, ["topics"])
    assert "env-topic" in result.output
