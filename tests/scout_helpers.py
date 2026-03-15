# ABOUTME: Shared helpers for building Claude Code session JSONL test data
# ABOUTME: Used by both conftest.py fixtures and BDD step definitions

import json


def make_session_event(event_type, session_id, timestamp, **kwargs):
    """Build a Claude Code session JSONL event dict."""
    event = {
        "type": event_type,
        "timestamp": timestamp,
        "sessionId": session_id,
    }
    if event_type == "assistant":
        model = kwargs.get("model", "claude-opus-4-6")
        usage = kwargs.get("usage", {
            "input_tokens": 1000,
            "output_tokens": 500,
            "cache_read_input_tokens": 300,
            "cache_creation_input_tokens": 200,
        })
        content = kwargs.get("content", [{"type": "text", "text": "response"}])
        event["message"] = {
            "model": model,
            "role": "assistant",
            "content": content,
            "usage": usage,
        }
    return event


def write_session_file(directory, session_id, events):
    """Write a list of event dicts as JSONL to a session file."""
    path = directory / f"{session_id}.jsonl"
    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return path
