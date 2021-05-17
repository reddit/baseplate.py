from enum import Enum
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
from baseplate.observers.timeout import ServerTimeout


class Errors(Enum):
    TIMED_OUT = "timed_out"
    EXCEPTION = "internal_server_error"


class TaggedMetricsBaseplateObserver(BaseplateObserver):
    """Metrics collecting observer.

    This observer reports metrics to statsd in the Influx StatsD format. It does three important things:

    * it tracks the time taken in serving each request.
    * it batches all metrics generated during a request into as few packets
      as possible.
    * it adds tags to the metric if they are in the config tag whitelist

    The batch is accessible to your application during requests as the
    ``metrics`` attribute on the :py:class:`~baseplate.RequestContext`.

    :param client: The client where metrics will be sent.
    :param cfg: the parsed application config with the tag whitelist

    """

    def __init__(self, client: metrics.Client, whitelist: Set[str], sample_rate: float = 1.0):
        self.client = client
        self.whitelist = whitelist
        self.sample_rate = sample_rate

    @classmethod
    def from_config_and_client(
        cls, raw_config: config.RawConfig, client: metrics.Client
    ) -> "TaggedMetricsBaseplateObserver":
        cfg = config.parse_config(
            raw_config,
            {
                "metrics": {
                    "whitelist": config.Optional(
                        config.TupleOf(config.String),
                        default=["client", "endpoint", "success", "error"],
                    ),
                },
                "metrics_observer": {"sample_rate": config.Optional(config.Percent, default=1.0)},
            },
        )
        return cls(
            client,
            whitelist=set(cfg.metrics.whitelist),
            sample_rate=cfg.metrics_observer.sample_rate,
        )

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        batch = self.client.batch()
        context.metrics = batch
        if self.sample_rate == 1.0 or random() < self.sample_rate:
            observer: SpanObserver = TaggedMetricsServerSpanObserver(
                batch, server_span, self.whitelist, self.sample_rate
            )
        else:
            observer = TaggedMetricsServerSpanDummyObserver(batch)
        server_span.register(observer)


class TaggedMetricsServerSpanDummyObserver(SpanObserver):
    # for requests that aren't sampled
    def __init__(self, batch: metrics.Batch):
        self.batch = batch

    def on_start(self) -> None:
        pass

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.batch.flush()

    def on_child_span_created(self, span: Span) -> None:
        pass


class TaggedMetricsServerSpanObserver(SpanObserver):
    def __init__(
        self, batch: metrics.Batch, server_span: Span, whitelist: Set[str], sample_rate: float = 1.0
    ):
        self.batch = batch
        self.span = server_span
        self.base_name = "baseplate.server"
        self.whitelist = whitelist
        self.tags: Dict[str, Any] = {}
        self.timer = batch.timer(self.base_name)
        self.counters: Dict[str, float] = {}
        self.sample_rate = sample_rate

    def on_start(self) -> None:
        self.tags["endpoint"] = self.span.name
        self.timer.start(self.sample_rate)

    def on_incr_tag(self, key: str, delta: float) -> None:
        self.counters[key] = delta

    def on_set_tag(self, key: str, value: Any) -> None:
        new_tags = {}
        if key == "error":
            if isinstance(value, Errors):
                new_tags[key] = value.value
        else:
            new_tags[key] = value
        self.tags.update(new_tags)

    def on_child_span_created(self, span: Span) -> None:
        observer: SpanObserver
        if isinstance(span, LocalSpan):
            observer = TaggedMetricsLocalSpanObserver(
                self.batch, span, self.whitelist, self.sample_rate
            )
        else:
            observer = TaggedMetricsClientSpanObserver(
                self.batch, span, self.whitelist, self.sample_rate
            )
        span.register(observer)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        if not exc_info:
            self.tags["success"] = True
        else:
            self.tags["success"] = False
            if exc_info[0] is not None and issubclass(ServerTimeout, exc_info[0]):
                self.tags["timed_out"] = True
        filtered_tags = {k: v for (k, v) in self.tags.items() if k in self.whitelist}
        self.tags = filtered_tags
        for key, delta in self.counters.items():
            self.batch.counter(key, self.tags).increment(delta, sample_rate=self.sample_rate)
        self.timer.update_tags(self.tags)
        self.timer.stop()
        self.batch.flush()


class TaggedMetricsLocalSpanObserver(SpanObserver):
    def __init__(
        self, batch: metrics.Batch, span: Span, whitelist: Set[str], sample_rate: float = 1.0
    ):
        self.batch = batch
        self.span = span
        self.tags: Dict[str, Any] = {}
        self.timer = batch.timer("baseplate.local")
        self.whitelist = whitelist
        self.counters: Dict[str, float] = {}
        self.sample_rate = sample_rate

    def on_start(self) -> None:
        self.timer.start(self.sample_rate)
        self.tags["endpoint"] = self.span.name

    def on_incr_tag(self, key: str, delta: float) -> None:
        self.counters[key] = delta

    def on_set_tag(self, key: str, value: Any) -> None:
        if key == "error":
            if isinstance(value, Errors):
                self.tags["error"] = value.value
        else:
            self.tags[key] = value

    def on_child_span_created(self, span: Span) -> None:
        observer: SpanObserver
        if isinstance(span, LocalSpan):
            observer = TaggedMetricsLocalSpanObserver(
                self.batch, span, self.whitelist, self.sample_rate
            )
        else:
            observer = TaggedMetricsClientSpanObserver(
                self.batch, span, self.whitelist, self.sample_rate
            )
        span.register(observer)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        filtered_tags = {k: v for (k, v) in self.tags.items() if k in self.whitelist}
        for key, delta in self.counters.items():
            self.batch.counter(key, self.tags).increment(delta, sample_rate=self.sample_rate)
        self.tags = filtered_tags
        self.timer.update_tags(self.tags)
        self.timer.stop()
        self.batch.flush()


class TaggedMetricsClientSpanObserver(SpanObserver):
    def __init__(
        self, batch: metrics.Batch, span: Span, whitelist: Set[str], sample_rate: float = 1.0
    ):
        self.batch = batch
        self.span = span
        self.base_name = "baseplate.client"
        self.tags: Dict[str, Any] = {}
        self.timer = batch.timer(self.base_name)
        self.whitelist = whitelist
        self.counters: Dict[str, float] = {}
        self.sample_rate = sample_rate

    def on_start(self) -> None:
        self.timer.start(self.sample_rate)
        self.tags["client"] = self.span.name.split(".")[0]
        self.tags["endpoint"] = self.span.name.split(".")[1]

    def on_incr_tag(self, key: str, delta: float) -> None:
        self.counters[key] = delta

    def on_set_tag(self, key: str, value: Any) -> None:
        if key == "error":
            if isinstance(value, Errors):
                self.tags["error"] = value.value
        else:
            self.tags[key] = value

    def on_log(self, name: str, payload: Any) -> None:
        if name == "error.object":
            self.tags["error"] = Errors.EXCEPTION.value

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.tags["success"] = not exc_info
        filtered_tags = {k: v for (k, v) in self.tags.items() if k in self.whitelist}
        self.tags = filtered_tags
        for key, delta in self.counters.items():
            self.batch.counter(key, self.tags).increment(delta, sample_rate=self.sample_rate)
        self.timer.update_tags(self.tags)
        self.timer.stop()
        self.batch.flush()
