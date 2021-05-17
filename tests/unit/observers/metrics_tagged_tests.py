from __future__ import annotations

import time

from typing import Any
from typing import Dict
from typing import Optional

import pytest

from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate import Span
from baseplate.lib.metrics import Counter
from baseplate.lib.metrics import Gauge
from baseplate.lib.metrics import Histogram
from baseplate.lib.metrics import Timer
from baseplate.observers.metrics_tagged import TaggedMetricsClientSpanObserver
from baseplate.observers.metrics_tagged import TaggedMetricsLocalSpanObserver
from baseplate.observers.metrics_tagged import TaggedMetricsServerSpanObserver


class TestException(Exception):
    pass


class FakeTimer:
    def __init__(self, batch: FakeBatch, name: str, tags: Dict[str, Any]):
        self.batch = batch
        self.name = name
        self.tags = tags

        self.start_time: Optional[float] = None
        self.sample_rate: float = 1.0

    def start(self, sample_rate: float = 1.0) -> None:
        self.start_time = time.time()
        self.sample_rate = sample_rate

    def stop(self) -> None:
        self.send(time.time() - self.start_time, self.sample_rate)

    def __enter__(self) -> None:
        self.start()

    def __exit__(self, *args) -> None:
        self.stop()
        return None

    def send(self, elapsed: float, sample_rate: float = 1.0) -> None:
        self.batch.timers.append(
            {"name": self.name, "elapsed": elapsed, "sample_rate": sample_rate, "tags": self.tags}
        )

    def update_tags(self, tags: Dict[str, Any]) -> None:
        self.tags.update(tags)


class FakeCounter:
    def __init__(self, batch: FakeBatch, name: str, tags: Dict[str, Any]):
        self.batch = batch
        self.name = name
        self.tags = tags

    def increment(self, delta: float = 1.0, sample_rate: float = 1.0) -> None:
        self.send(delta, sample_rate)

    def decrement(self, delta: float = 1.0, sample_rate: float = 1.0) -> None:
        self.increment(-delta, sample_rate)

    def send(self, delta: float, sample_rate: float) -> None:
        self.batch.counters.append(
            {"name": self.name, "delta": delta, "sample_rate": sample_rate, "tags": self.tags}
        )


class FakeBatch:
    def __init__(self):
        self.timers = []
        self.counters = []
        self.flushed = False

    def timer(self, name: str, tags: Optional[Dict[str, Any]] = None) -> Timer:
        return FakeTimer(self, name, tags or {})

    def counter(self, name: str, tags: Optional[Dict[str, Any]] = None) -> Counter:
        return FakeCounter(self, name, tags or {})

    def gauge(self, name: str, tags: Optional[Dict[str, Any]] = None) -> Gauge:
        raise NotImplementedError

    def histogram(self, name: str, tags: Optional[Dict[str, Any]] = None) -> Histogram:
        raise NotImplementedError

    def flush(self):
        self.flushed = True


@pytest.mark.parametrize(
    "observer_cls,name",
    (
        (TaggedMetricsServerSpanObserver, "server"),
        (TaggedMetricsClientSpanObserver, "client"),
        (TaggedMetricsLocalSpanObserver, "local"),
    ),
)
def test_observer(observer_cls, name):
    batch = FakeBatch()
    span = Span(
        trace_id=1234,
        parent_id=2345,
        span_id=3456,
        sampled=None,
        flags=None,
        name="fancy.span",
        context=RequestContext({}),
    )
    allow_list = {"client", "endpoint", "tag2"}
    sample_rate = 0.3
    observer = observer_cls(batch, span, allow_list, sample_rate=0.3)

    observer.on_start()
    observer.on_incr_tag("my.tag", 2)
    observer.on_set_tag("tag1", "foo")
    observer.on_set_tag("tag2", "bar")
    observer.on_finish(None)

    assert batch.timers
    timer = batch.timers.pop()
    assert batch.timers == []
    assert timer["sample_rate"] == sample_rate
    assert timer["name"] == f"baseplate.{name}.latency"

    if name in ("server", "local"):
        assert timer["tags"].pop("endpoint") == "fancy.span"
    else:
        assert timer["tags"].pop("client") == "fancy"
        assert timer["tags"].pop("endpoint") == "span"
    assert timer["tags"].pop("tag2") == "bar"
    assert timer["tags"] == {}

    for _ in range(2):
        counter = batch.counters.pop()
        assert counter["sample_rate"] == sample_rate
        if counter["name"] == f"baseplate.{name}.rate":
            assert counter["delta"] == 1
            assert counter["tags"].pop("success") is True
        elif counter["name"] == "my.tag":
            assert counter["delta"] == 2
        else:
            raise Exception(f"unexpected counter: {counter}")

        if name in ("server", "local"):
            assert counter["tags"].pop("endpoint") == "fancy.span"
        else:
            assert counter["tags"].pop("client") == "fancy"
            assert counter["tags"].pop("endpoint") == "span"
        assert counter["tags"].pop("tag2") == "bar"
        assert counter["tags"] == {}


def test_nested():
    batch = FakeBatch()
    span = ServerSpan(
        trace_id=1234,
        parent_id=2345,
        span_id=3456,
        sampled=None,
        flags=None,
        name="fancy.span",
        context=RequestContext({}),
    )
    observer = TaggedMetricsLocalSpanObserver(batch, span, {"client", "endpoint"})
    span.register(observer)

    with span.make_child("foo", local=True, component_name="foo") as child_span:
        assert len(child_span.observers) == 1
