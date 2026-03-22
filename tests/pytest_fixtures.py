# ABOUTME: Hardcoded pytest-reportlog JSONL fixtures for testing
# ABOUTME: Provides realistic test data without depending on pytest-reportlog

import json
from pathlib import Path


def _line(obj: dict) -> str:
    return json.dumps(obj) + "\n"


# --- Single run: 5 tests (3 pass, 1 fail, 1 skip) ---

SINGLE_RUN_EVENTS = [
    {"$report_type": "SessionStart", "exitstatus": None},
    {"$report_type": "CollectReport", "nodeid": "", "outcome": "passed", "result": []},
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_add",
        "outcome": "passed",
        "when": "setup",
        "duration": 0.0001,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_add",
        "outcome": "passed",
        "when": "call",
        "duration": 0.0032,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_add",
        "outcome": "passed",
        "when": "teardown",
        "duration": 0.0001,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_subtract",
        "outcome": "passed",
        "when": "call",
        "duration": 0.0150,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_multiply",
        "outcome": "passed",
        "when": "call",
        "duration": 0.0510,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_divide",
        "outcome": "failed",
        "when": "call",
        "duration": 0.0024,
        "longrepr": "AssertionError: assert 1 / 0 == 0\nZeroDivisionError",
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_math.py::test_power",
        "outcome": "skipped",
        "when": "call",
        "duration": 0.0001,
    },
    {"$report_type": "SessionFinish", "exitstatus": 1},
]

# --- All-pass run: 3 tests ---

ALL_PASS_EVENTS = [
    {"$report_type": "SessionStart", "exitstatus": None},
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_utils.py::test_strip",
        "outcome": "passed",
        "when": "call",
        "duration": 0.001,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_utils.py::test_join",
        "outcome": "passed",
        "when": "call",
        "duration": 0.002,
    },
    {
        "$report_type": "TestReport",
        "nodeid": "tests/test_utils.py::test_split",
        "outcome": "passed",
        "when": "call",
        "duration": 0.003,
    },
    {"$report_type": "SessionFinish", "exitstatus": 0},
]

# --- Empty run: session lifecycle only ---

EMPTY_RUN_EVENTS = [
    {"$report_type": "SessionStart", "exitstatus": None},
    {"$report_type": "SessionFinish", "exitstatus": 5},
]

# --- Multi-run: 3 separate runs with varying outcomes (for glob mode) ---

MULTI_RUN_EVENTS = {
    "run-001": [
        {"$report_type": "SessionStart", "exitstatus": None},
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_a",
            "outcome": "passed",
            "when": "call",
            "duration": 0.010,
        },
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_b",
            "outcome": "failed",
            "when": "call",
            "duration": 0.020,
            "longrepr": "AssertionError: expected True",
        },
        {"$report_type": "SessionFinish", "exitstatus": 1},
    ],
    "run-002": [
        {"$report_type": "SessionStart", "exitstatus": None},
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_a",
            "outcome": "passed",
            "when": "call",
            "duration": 0.012,
        },
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_b",
            "outcome": "passed",
            "when": "call",
            "duration": 0.018,
        },
        {"$report_type": "SessionFinish", "exitstatus": 0},
    ],
    "run-003": [
        {"$report_type": "SessionStart", "exitstatus": None},
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_a",
            "outcome": "passed",
            "when": "call",
            "duration": 0.011,
        },
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_b",
            "outcome": "skipped",
            "when": "call",
            "duration": 0.001,
        },
        {
            "$report_type": "TestReport",
            "nodeid": "tests/test_core.py::test_c",
            "outcome": "passed",
            "when": "call",
            "duration": 0.005,
        },
        {"$report_type": "SessionFinish", "exitstatus": 0},
    ],
}


def write_run_file(directory: Path, name: str, events: list[dict]) -> Path:
    """Write a list of events to a JSONL file in the given directory.

    Args:
        directory: Parent directory to write the file into.
        name: Filename without extension (e.g. "run-001").
        events: List of event dicts to serialize.

    Returns:
        Path to the written file.
    """
    path = directory / f"{name}.jsonl"
    with open(path, "w") as f:
        for event in events:
            f.write(_line(event))
    return path
