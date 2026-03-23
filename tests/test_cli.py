# ABOUTME: Tests for brooklet CLI core commands — register, produce, consume, topics
# ABOUTME: Uses Typer CliRunner to test commands without subprocess spawning

import json

from typer.testing import CliRunner

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
