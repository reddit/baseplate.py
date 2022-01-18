from random import random
from typing import Any
from typing import Dict
from typing import Optional
from typing import Set

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import LocalSpan
from baseplate import RequestContext
from baseplate import Span
from baseplate import SpanObserver
from baseplate.lib import config
from baseplate.lib import metrics


class TaggedMetricsBaseplateObserver(BaseplateObserver):
    """Metrics collecting observer.

    This observer reports metrics to statsd in the Influx StatsD format. It does three important things:

    * it tracks the time taken in serving each request.
    * it batches all metrics generated during a request into as few packets
      as possible.
    * it adds tags to the metric if they are in the config tag allowlist

    The batch is accessible to your application during requests as the
    ``metrics`` attribute on the :py:class:`~baseplate.RequestContext`.

    :param client: The client where metrics will be sent.
    :param cfg: the parsed application config with the tag allowlist

    """

    def __init__(self, client: metrics.Client, allowlist: Set[str], sample_rate: float = 1.0):
        self._client = client
        self._allowlist = allowlist
        self._sample_rate = sample_rate

    @classmethod
    def from_config_and_client(
        cls, raw_config: config.RawConfig, client: metrics.Client
    ) -> "TaggedMetricsBaseplateObserver":
        cfg = config.parse_config(
            raw_config,
            {
                "metrics": {
                    "allowlist": config.Optional(config.TupleOf(config.String), default=[]),
                },
                "metrics_observer": {"sample_rate": config.Optional(config.Percent, default=1.0)},
            },
        )
        return cls(
            client,
            allowlist=set(cfg.metrics.allowlist) | {"client", "endpoint"},
            sample_rate=cfg.metrics_observer.sample_rate,
        )

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        batch = self._client.batch()
        context.metrics = batch
        if self._sample_rate == 1.0 or random() < self._sample_rate:
            observer: SpanObserver = TaggedMetricsServerSpanObserver(
                batch, server_span, self._allowlist, self._sample_rate
            )
        else:
            observer = TaggedMetricsServerSpanDummyObserver(batch)
        server_span.register(observer)


class TaggedMetricsServerSpanDummyObserver(SpanObserver):
    # for requests that aren't sampled
    def __init__(self, batch: metrics.Batch):
        self._batch = batch

    def on_start(self) -> None:
        pass

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self._batch.flush()

    def on_child_span_created(self, span: Span) -> None:
        pass


class TaggedMetricsServerSpanObserver(SpanObserver):
    def __init__(
        self, batch: metrics.Batch, server_span: Span, allowlist: Set[str], sample_rate: float = 1.0
    ):
        self._batch = batch
        self._span = server_span
        self._base_name = "baseplate.server"
        self._allowlist = allowlist
        self._tags: Dict[str, Any] = {}
        self._timer = batch.timer(f"{self._base_name}.latency")
        self._counters: Dict[str, float] = {}
        self._sample_rate = sample_rate

    def on_start(self) -> None:
        self._tags["endpoint"] = self._span.name
        self._timer.start(self._sample_rate)

    def on_incr_tag(self, key: str, delta: float) -> None:
        self._counters[key] = delta

    def on_set_tag(self, key: str, value: Any) -> None:
        self._tags[key] = value

    def on_child_span_created(self, span: Span) -> None:
        observer: SpanObserver
        if isinstance(span, LocalSpan):
            observer = TaggedMetricsLocalSpanObserver(
                self._batch, span, self._allowlist, self._sample_rate
            )
        else:
            observer = TaggedMetricsClientSpanObserver(
                self._batch, span, self._allowlist, self._sample_rate
            )
        span.register(observer)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        filtered_tags = {k: v for (k, v) in self._tags.items() if k in self._allowlist}

        for key, delta in self._counters.items():
            self._batch.counter(key, filtered_tags).increment(delta, sample_rate=self._sample_rate)

        self._timer.update_tags(filtered_tags)
        self._timer.stop()

        self._batch.counter(
            f"{self._base_name}.rate", {**filtered_tags, "success": not exc_info}
        ).increment(sample_rate=self._sample_rate)

        self._batch.flush()


class TaggedMetricsLocalSpanObserver(SpanObserver):
    def __init__(
        self, batch: metrics.Batch, span: Span, allowlist: Set[str], sample_rate: float = 1.0
    ):
        self._batch = batch
        self._span = span
        self._tags: Dict[str, Any] = {}
        self._base_name = "baseplate.local"
        self._timer = batch.timer(f"{self._base_name}.latency")
        self._allowlist = allowlist
        self._counters: Dict[str, float] = {}
        self._sample_rate = sample_rate

    def on_start(self) -> None:
        self._timer.start(self._sample_rate)
        self._tags["endpoint"] = self._span.name

    def on_incr_tag(self, key: str, delta: float) -> None:
        self._counters[key] = delta

    def on_set_tag(self, key: str, value: Any) -> None:
        self._tags[key] = value

    def on_child_span_created(self, span: Span) -> None:
        observer: SpanObserver
        if isinstance(span, LocalSpan):
            observer = TaggedMetricsLocalSpanObserver(
                self._batch, span, self._allowlist, self._sample_rate
            )
        else:
            observer = TaggedMetricsClientSpanObserver(
                self._batch, span, self._allowlist, self._sample_rate
            )
        span.register(observer)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        filtered_tags = {k: v for (k, v) in self._tags.items() if k in self._allowlist}

        for key, delta in self._counters.items():
            self._batch.counter(key, filtered_tags).increment(delta, sample_rate=self._sample_rate)

        self._timer.update_tags(filtered_tags)
        self._timer.stop()

        self._batch.counter(
            f"{self._base_name}.rate",
            {**filtered_tags, "success": not exc_info},
        ).increment(sample_rate=self._sample_rate)

        self._batch.flush()


class TaggedMetricsClientSpanObserver(SpanObserver):
    def __init__(
        self, batch: metrics.Batch, span: Span, allowlist: Set[str], sample_rate: float = 1.0
    ):
        self._batch = batch
        self._span = span
        self._base_name = "baseplate.client"
        self._tags: Dict[str, Any] = {}
        self._timer = batch.timer(f"{self._base_name}.latency")
        self._allowlist = allowlist
        self._counters: Dict[str, float] = {}
        self._sample_rate = sample_rate

    def on_start(self) -> None:
        self._timer.start(self._sample_rate)
        self._tags["client"], _, self._tags["endpoint"] = self._span.name.rpartition(".")

    def on_incr_tag(self, key: str, delta: float) -> None:
        self._counters[key] = delta

    def on_set_tag(self, key: str, value: Any) -> None:
        self._tags[key] = value

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        filtered_tags = {k: v for (k, v) in self._tags.items() if k in self._allowlist}

        for key, delta in self._counters.items():
            self._batch.counter(key, filtered_tags).increment(delta, sample_rate=self._sample_rate)

        self._timer.update_tags(filtered_tags)
        self._timer.stop()

        self._batch.counter(
            f"{self._base_name}.rate",
            {**filtered_tags, "success": not exc_info},
        ).increment(sample_rate=self._sample_rate)

        self._batch.flush()
