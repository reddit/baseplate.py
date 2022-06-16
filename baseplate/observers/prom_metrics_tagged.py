from random import random
from time import time
from typing import Any
from typing import Dict
from typing import Optional
from typing import List

from prometheus_client import Counter
from prometheus_client import Histogram

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import LocalSpan
from baseplate import RequestContext
from baseplate import Span
from baseplate import SpanObserver
from baseplate.lib import config


class _PrometheusMetrics:
    def __init__(self, allowlist):
        print(allowlist)
        self.server_rate = Counter(
            "baseplate_server_span_rate", "Baseplate Server Span Counter", allowlist
        )
        self.server_latency = Histogram(
            "baseplate_server_span_latency_seconds", "Baseplate Server Span Latency", allowlist
        )
        self.client_rate = Counter(
            "baseplate_client_span_rate", "Baseplate Client Span Counter", allowlist
        )
        self.client_latency = Histogram(
            "baseplate_client_span_latency_seconds", "Baseplate Client Span Latency", allowlist
        )
        self.local_rate = Counter(
            "baseplate_local_span_rate", "Baseplate Local Span Counter", allowlist
        )
        self.local_latency = Histogram(
            "baseplate_local_span_latency_seconds", "Baseplate Local Span Latency", allowlist
        )


class PromTaggedMetricsBaseplateObserver(BaseplateObserver):
    """Metrics collecting observer.

    This observer reports metrics on Spans to Prometheus.

    * it tracks the time taken in serving each request.
    * it adds tags to the metric if they are in the config tag allowlist

    :param cfg: the parsed application config with the tag allowlist
    """

    def __init__(
        self, prom_metrics: _PrometheusMetrics, allowlist: List[str], sample_rate: float = 1.0
    ):
        self.prom_metrics = prom_metrics
        self.allowlist = allowlist
        self.sample_rate = sample_rate

    @classmethod
    def from_config(cls, raw_config: config.RawConfig) -> "PromTaggedMetricsBaseplateObserver":
        cfg = config.parse_config(
            raw_config,
            {
                "metrics": {
                    "allowlist": config.Optional(config.TupleOf(config.String), default=[]),
                },
                "metrics_observer": {"sample_rate": config.Optional(config.Percent, default=1.0)},
                # TODO: make latency buckets configurable?
            },
        )
        allowlist = [*cfg.metrics.allowlist, "client", "endpoint", "success"]
        return cls(
            _PrometheusMetrics(allowlist),
            allowlist=allowlist,
            sample_rate=cfg.metrics_observer.sample_rate,
        )

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        if self.sample_rate == 1.0 or random() < self.sample_rate:
            observer: SpanObserver = PromTaggedMetricsServerSpanObserver(
                self.prom_metrics, server_span, self.allowlist, self.sample_rate
            )
        else:
            observer = _PromTaggedMetricsServerSpanDummyObserver()
        server_span.register(observer)


class _PromSpanObserver(SpanObserver):
    def __init__(
        self,
        prom_metrics: _PrometheusMetrics,
        server_span: Span,
        allowlist: List[str],
        sample_rate: float = 1.0,
    ):
        self.prom_metrics = prom_metrics
        self.span = server_span
        self.allowlist = allowlist
        self.tags: Dict[str, Any] = {}
        self.counters: Dict[str, float] = {}
        self.sample_rate = sample_rate
        self.time_started: Optional[float] = None

    def on_start(self) -> None:
        self.time_started = time()
        self.tags["endpoint"] = self.span.name

    def on_incr_tag(self, key: str, delta: float) -> None:
        self.counters[key] = delta

    def on_set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    def default_tags(self):
        return {k: "" for k in self.allowlist}

    def filtered_tags(self):
        filtered_tags = {k: v for (k, v) in self.tags.items() if k in self.allowlist}
        return {**self.default_tags(), **filtered_tags}

    def filtered_counters(self):
        return {k: v for (k, v) in self.counters.items() if k in self.allowlist}

    def runtime(self):
        if self.time_started:
            return time() - self.time_started
        return 0


class _PromTaggedMetricsServerSpanDummyObserver(SpanObserver):
    # for requests that aren't sampled
    def __init__(self):
        pass

    def on_start(self) -> None:
        pass

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        pass

    def on_child_span_created(self, span: Span) -> None:
        pass


class PromTaggedMetricsServerSpanObserver(_PromSpanObserver):
    def on_child_span_created(self, span: Span) -> None:
        observer: SpanObserver
        if isinstance(span, LocalSpan):
            observer = PromTaggedMetricsLocalSpanObserver(
                self.prom_metrics, span, self.allowlist, self.sample_rate
            )
        else:
            observer = PromTaggedMetricsClientSpanObserver(
                self.prom_metrics, span, self.allowlist, self.sample_rate
            )
        span.register(observer)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.prom_metrics.server_latency.labels(**self.filtered_tags()).observe(self.runtime())
        self.prom_metrics.server_rate.labels(
            **{**self.filtered_tags(), **self.filtered_counters(), "success": not exc_info}
        ).inc()


class PromTaggedMetricsLocalSpanObserver(_PromSpanObserver):
    def on_child_span_created(self, span: Span) -> None:
        observer: SpanObserver
        if isinstance(span, LocalSpan):
            observer = PromTaggedMetricsLocalSpanObserver(
                self.prom_metrics, span, self.allowlist, self.sample_rate
            )
        else:
            observer = PromTaggedMetricsClientSpanObserver(
                self.prom_metrics, span, self.allowlist, self.sample_rate
            )
        span.register(observer)

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.prom_metrics.local_latency.labels(**self.filtered_tags()).observe(self.runtime())
        self.prom_metrics.local_rate.labels(
            **{**self.filtered_tags(), **self.filtered_counters(), "success": not exc_info}
        ).inc()


class PromTaggedMetricsClientSpanObserver(_PromSpanObserver):
    def on_start(self) -> None:
        self.time_started = time()
        self.tags["client"], _, self.tags["endpoint"] = self.span.name.rpartition(".")

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.prom_metrics.client_latency.labels(**self.filtered_tags()).observe(self.runtime())
        self.prom_metrics.client_rate.labels(
            **{**self.filtered_tags(), **self.filtered_counters(), "success": not exc_info}
        ).inc()
