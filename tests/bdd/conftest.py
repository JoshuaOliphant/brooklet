# ABOUTME: Shared BDD fixtures for brooklet acceptance tests
# ABOUTME: Provides stream instances and temporary directories for feature tests

import pytest

import brooklet


@pytest.fixture
def stream_dir(tmp_path):
    """Provide a temporary directory for stream data."""
    return tmp_path / "streams"


@pytest.fixture
def stream(stream_dir):
    """Provide a brooklet Stream opened at a temporary directory."""
    return brooklet.open(str(stream_dir))
