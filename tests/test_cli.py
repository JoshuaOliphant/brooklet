# ABOUTME: Tests for brooklet CLI core commands — register, produce, consume, topics
# ABOUTME: Uses Typer CliRunner to test commands without subprocess spawning

import json

from typer.testing import CliRunner

import brooklet
from brooklet.cli import app

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

    data_file = tmp_path / "my-topic" / "data.jsonl"
    assert data_file.exists()
    lines = data_file.read_text().strip().split("\n")
    assert len(lines) == 2
    for line in lines:
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

    data_file = tmp_path / "my-topic" / "data.jsonl"
    lines = data_file.read_text().strip().split("\n")
    assert len(lines) == 2


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

    data_file = tmp_path / "my-topic" / "data.jsonl"
    event = json.loads(data_file.read_text().strip())
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


def test_stream_dir_env_var(tmp_path, monkeypatch):
    monkeypatch.setenv("BROOKLET_DIR", str(tmp_path))

    stream = brooklet.open(tmp_path)
    stream.produce("env-topic", {"type": "test"})

    result = runner.invoke(app, ["topics"])
    assert "env-topic" in result.output
