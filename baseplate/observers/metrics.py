from random import random
from typing import Any
from typing import Optional

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import LocalSpan
from baseplate import RequestContext
from baseplate import Span
from baseplate import SpanObserver
from baseplate.lib import config
from baseplate.lib import metrics
from baseplate.observers.timeout import ServerTimeout


class MetricsBaseplateObserver(BaseplateObserver):
    """Metrics collecting observer.

    This observer reports metrics to statsd. It does two important things:

    * it tracks the time taken in serving each request.
    * it batches all metrics generated during a request into as few packets
      as possible.

    The batch is accessible to your application during requests as the
    ``metrics`` attribute on the :py:class:`~baseplate.RequestContext`.

    :param client: The client where metrics will be sent.

    """

    def __init__(self, client: metrics.Client, sample_rate: float = 1.0):
        self.client = client
        self.sample_rate = sample_rate

    @classmethod
    def from_config_and_client(
        cls, raw_config: config.RawConfig, client: metrics.Client
    ) -> "MetricsBaseplateObserver":
        cfg = config.parse_config(
            raw_config,
            {"metrics_observer": {"sample_rate": config.Optional(config.Percent, default=1.0)}},
        )
        return cls(client, sample_rate=cfg.metrics_observer.sample_rate)

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        batch = self.client.batch()
        context.metrics = batch
        if self.sample_rate == 1.0 or random() < self.sample_rate:
            observer: SpanObserver = MetricsServerSpanObserver(batch, server_span, self.sample_rate)
        else:
            observer = MetricsServerSpanDummyObserver(batch)
        server_span.register(observer)


class MetricsServerSpanDummyObserver(SpanObserver):
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


class MetricsServerSpanObserver(SpanObserver):
    def __init__(self, batch: metrics.Batch, server_span: Span, sample_rate: float = 1.0):
        self.batch = batch
        self.base_name = "server." + server_span.name
        self.timer: Optional[metrics.Timer] = None
        self.sample_rate = sample_rate

    def on_start(self) -> None:
        self.timer = self.batch.timer(self.base_name)
        self.timer.start(self.sample_rate)

    def on_incr_tag(self, key: str, delta: float) -> None:
        self.batch.counter(key).increment(delta, sample_rate=self.sample_rate)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        # the timer might not exist if another observer threw an exception
        # before we got our on_start() called
        if self.timer:
            self.timer.stop()

        if not exc_info:
            self.batch.counter(f"{self.base_name}.success").increment(sample_rate=self.sample_rate)
        else:
            self.batch.counter(f"{self.base_name}.failure").increment(sample_rate=self.sample_rate)

            if exc_info[0] is not None and issubclass(ServerTimeout, exc_info[0]):
                self.batch.counter(f"{self.base_name}.timed_out").increment(
                    sample_rate=self.sample_rate
                )

        self.batch.flush()

    def on_child_span_created(self, span: Span) -> None:
        observer: SpanObserver
        if isinstance(span, LocalSpan):
            observer = MetricsLocalSpanObserver(self.batch, span, self.sample_rate)
        else:
            observer = MetricsClientSpanObserver(self.batch, span, self.sample_rate)
        span.register(observer)


class MetricsLocalSpanObserver(SpanObserver):
    def __init__(self, batch: metrics.Batch, span: Span, sample_rate: float = 1.0):
        self.batch = batch
        self.timer = batch.timer(f"{span.component_name}.{span.name}")
        self.sample_rate = sample_rate

    def on_start(self) -> None:
        self.timer.start(self.sample_rate)

    def on_incr_tag(self, key: str, delta: float) -> None:
        self.batch.counter(key).increment(delta, sample_rate=self.sample_rate)

    def on_child_span_created(self, span: Span) -> None:
        observer: SpanObserver
        if isinstance(span, LocalSpan):
            observer = MetricsLocalSpanObserver(self.batch, span, self.sample_rate)
        else:
            observer = MetricsClientSpanObserver(self.batch, span, self.sample_rate)
        span.register(observer)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.timer.stop()


class MetricsClientSpanObserver(SpanObserver):
    def __init__(self, batch: metrics.Batch, span: Span, sample_rate: float = 1.0):
        self.batch = batch
        self.base_name = f"clients.{span.name}"
        self.timer = batch.timer(self.base_name)
        self.sample_rate = sample_rate

    def on_start(self) -> None:
        self.timer.start(self.sample_rate)

    def on_incr_tag(self, key: str, delta: float) -> None:
        self.batch.counter(key).increment(delta, sample_rate=self.sample_rate)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.timer.stop()
        suffix = "success" if not exc_info else "failure"
        self.batch.counter(self.base_name + "." + suffix).increment(sample_rate=self.sample_rate)

    def on_log(self, name: str, payload: Any) -> None:
        if name == "error.object":
            self.batch.counter(f"errors.{payload.__class__.__name__}").increment(
                sample_rate=self.sample_rate
            )
