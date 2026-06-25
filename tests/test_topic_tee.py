# ABOUTME: Tests for the contrib tee_to_topic passthrough sink
# ABOUTME: Verifies items pass through unchanged, are produced, and failures warn without raising

from dataclasses import dataclass

from brooklet.contrib.topic_tee import tee_to_topic


@dataclass
class _Item:
    id: str
    value: int

    def to_dict(self) -> dict:
        return {"id": self.id, "value": self.value}


class _RecordingStream:
    """Minimal Stream stand-in capturing produce() calls."""

    def __init__(self, fail_on: set[str] | None = None) -> None:
        self.produced: list[tuple[str, dict, str]] = []
        self._fail_on = fail_on or set()

    def produce(self, topic: str, event: dict, source: str | None = None) -> None:
        if event["id"] in self._fail_on:
            raise ValueError(f"boom for {event['id']}")
        self.produced.append((topic, event, source))


def _describe(item: _Item) -> str:
    return f"item {item.id}"


def test_yields_items_unchanged_and_produces_each():
    stream = _RecordingStream()
    items = [_Item("a", 1), _Item("b", 2)]

    out = list(tee_to_topic(items, stream, "t", "src", _describe))

    assert out == items  # passthrough, same objects in order
    assert stream.produced == [
        ("t", {"id": "a", "value": 1}, "src"),
        ("t", {"id": "b", "value": 2}, "src"),
    ]


def test_produce_failure_warns_and_continues(capsys):
    stream = _RecordingStream(fail_on={"a"})
    items = [_Item("a", 1), _Item("b", 2)]

    out = list(tee_to_topic(items, stream, "mytopic", "src", _describe))

    # Both items still flow through despite the failure on "a".
    assert out == items
    # Only the good one was produced.
    assert stream.produced == [("mytopic", {"id": "b", "value": 2}, "src")]
    # The failure was reported on stderr, not raised.
    err = capsys.readouterr().err
    assert "failed to produce item a to topic 'mytopic'" in err


def test_is_lazy_passthrough():
    """The sink is a generator — nothing is produced until iterated."""
    stream = _RecordingStream()
    gen = tee_to_topic([_Item("a", 1)], stream, "t", "src", _describe)
    assert stream.produced == []  # not yet consumed
    next(gen)
    assert len(stream.produced) == 1


def test_describe_only_called_on_failure():
    calls: list[str] = []

    def describe(item: _Item) -> str:
        calls.append(item.id)
        return item.id

    stream = _RecordingStream(fail_on={"b"})
    list(tee_to_topic([_Item("a", 1), _Item("b", 2)], stream, "t", "src", describe))

    # describe runs only for the failing item, not the successful one.
    assert calls == ["b"]


def test_empty_iterable_produces_nothing():
    stream = _RecordingStream()
    assert list(tee_to_topic([], stream, "t", "src", _describe)) == []
    assert stream.produced == []


def test_reraises_nothing_for_handled_error_types():
    """OSError/ValueError/TypeError are caught; other errors would propagate."""
    stream = _RecordingStream(fail_on={"a"})  # raises ValueError
    # Should not raise:
    list(tee_to_topic([_Item("a", 1)], stream, "t", "src", _describe))
