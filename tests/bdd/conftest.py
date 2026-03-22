# ABOUTME: Shared BDD fixtures for brooklet acceptance tests
# ABOUTME: Provides stream instances, temporary directories, and shared step definitions

import pytest
from pytest_bdd import given, parsers, then

import brooklet


@pytest.fixture
def stream_dir(tmp_path):
    """Provide a temporary directory for stream data."""
    d = tmp_path / "streams"
    d.mkdir()
    return d


@pytest.fixture
def stream(stream_dir):
    """Provide a brooklet Stream opened at a temporary directory."""
    return brooklet.open(str(stream_dir))


@given("a stream opened at an empty directory", target_fixture="stream")
def given_stream_at_empty_dir(stream):
    """Shared Background step — opens a brooklet stream at a fresh temp directory."""
    return stream


@then(parsers.parse('"{topic}" appears in the stream topics list'))
def then_topic_in_topics(stream, topic):
    """Verify topic appears in stream.topics()."""
    assert topic in stream.topics()
