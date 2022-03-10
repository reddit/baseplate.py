import time
import logging

from typing import Any
from typing import Optional

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import LocalSpan
from baseplate import RequestContext
from baseplate import Span
from baseplate import SpanObserver
from baseplate.lib.prometheus_metrics import PrometheusHTTPServerMetrics
from baseplate.lib.prometheus_metrics import PrometheusHTTPClientMetrics
from baseplate.lib.prometheus_metrics import PrometheusHTTPLocalMetrics
from baseplate.lib.prometheus_metrics import PrometheusThriftServerMetrics
from baseplate.lib.prometheus_metrics import PrometheusThriftClientMetrics
from baseplate.lib.prometheus_metrics import PrometheusThriftLocalMetrics


NANOSECONDS_PER_SECOND = 1e9

logger = logging.getLogger(__name__)


class PrometheusBaseplateObserver(BaseplateObserver):
    """Metrics collecting observer.

    This observer reports Prometheus metrics for HTTP or Thrift client and servers.

    There must be a "protocol" tag set on the server_span that indictes the protocol, either "http" or "thrift",
    i.e. `span.set_tag("protocol", "http")` or `span.set_tag("protocol", "thrift")`.

    """
    def __init__(self):
        pass

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        observer = PrometheusServerSpanObserver()
        server_span.register(observer)


class PrometheusServerSpanObserver(SpanObserver):
    def __init__(self):
        self.tags = {}
        self.start_time = None
        self.metrics = None

    def on_set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    def get_prefix(self) -> str:
        return f"{self.protocol}_server"

    def get_labels(self) -> dict[str, str]:
        return self.tags

    @property
    def protocol(self):
        return self.tags.get("protocol", "unknown")

    def set_metrics_by_protocol(self):
        if self.protocol == "http":
            self.metrics = PrometheusHTTPServerMetrics()
        elif self.protocol == "thrift":
            self.metrics = PrometheusThriftServerMetrics()
        else:
            logger.warning("No valid protocol set for Prometheus metric collection, metrics won't be collected. Expected 'http' or 'thrift' protocol. Actual protocol: %s", self.protocol)

    def on_start(self) -> None:
        self.set_metrics_by_protocol()
        if self.metrics is None:
            logger.warning("No metrics set for Prometheus metric collection. Metrics will not be exported correctly.")
            return
        self.start_time = time.perf_counter_ns()
        self.metrics.active_requests_metric(self.tags).inc()

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        if self.metrics is None:
            logger.warning("No metrics set for Prometheus metric collection. Metrics will not be exported correctly.")
            return

        if exc_info is not None:
            self.tags["exception_type"] = exc_info[0]

        self.metrics.active_requests_metric(self.tags).dec()
        self.metrics.requests_total_metric(self.tags).inc()
        if self.start_time is not None:
            elapsed_ns = time.perf_counter_ns() - self.start_time
            self.metrics.latency_seconds_metric(self.tags).observe(elapsed_ns / NANOSECONDS_PER_SECOND)

    def on_child_span_created(self, span: Span) -> None:
        if isinstance(span, LocalSpan):
            observer = PrometheusLocalSpanObserver()
        else:
            observer = PrometheusClientSpanObserver()

        span.register(observer)


class PrometheusClientSpanObserver(SpanObserver):
    def __init__(self):
        self.tags = {}
        self.start_time = None
        self.metrics = None

    def on_set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    def get_prefix(self) -> str:
        return f"{self.protocol}_client"

    def get_labels(self) -> dict[str, str]:
        return self.tags

    @property
    def protocol(self):
        return self.tags.get("protocol", "unknown")

    def set_metrics_by_protocol(self):
        if self.protocol == "http":
            self.metrics = PrometheusHTTPClientMetrics()
        elif self.protocol == "thrift":
            self.metrics = PrometheusThriftClientMetrics()
        else:
            logger.warning("No valid protocol set for Prometheus metric collection, metrics won't be collected. Expected 'http' or 'thrift' protocol. Actual protocol: %s", self.protocol)
        return

    def on_start(self) -> None:
        self.set_metrics_by_protocol()
        if self.metrics is None:
            logger.warning("No metrics set for Prometheus metric collection. Metrics will not be exported correctly.")
            return
        self.start_time = time.perf_counter_ns()
        self.metrics.active_requests_metric(self.tags).inc()

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        if self.metrics is None:
            logger.warning("No metrics set for Prometheus metric collection. Metrics will not be exported correctly.")
            return

        if exc_info is not None:
            self.tags["exception_type"] = exc_info[0]

        self.metrics.active_requests_metric(self.tags).dec()
        self.metrics.requests_total_metric(self.tags).inc()
        if self.start_time is not None:
            elapsed_ns = time.perf_counter_ns() - self.start_time
            self.metrics.latency_seconds_metric(self.tags).observe(elapsed_ns / NANOSECONDS_PER_SECOND)

    def on_child_span_created(self, span: Span) -> None:
        if isinstance(span, LocalSpan):
            observer = PrometheusLocalSpanObserver()
        else:
            observer = PrometheusClientSpanObserver()

        span.register(observer)


class PrometheusLocalSpanObserver(SpanObserver):
    def __init__(self):
        self.tags = {}
        self.start_time = None
        self.metrics = None

    def on_set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    def get_prefix(self) -> str:
        return f"{self.protocol}_local"

    def get_labels(self) -> dict[str, str]:
        return self.tags

    @property
    def protocol(self):
        return self.tags.get("protocol", "unknown")

    def set_metrics_by_protocol(self):
        if self.protocol == "http":
            self.metrics = PrometheusHTTPLocalMetrics()
        elif self.protocol == "thrift":
            self.metrics = PrometheusThriftLocalMetrics()
        else:
            logger.warning("No valid protocol set for Prometheus metric collection, metrics won't be collected. Expected 'http' or 'thrift' protocol. Actual protocol: %s", self.protocol)
        return

    def on_start(self) -> None:
        self.set_metrics_by_protocol()
        if self.metrics is None:
            logger.warning("No metrics set for Prometheus metric collection. Metrics will not be exported correctly.")
            return
        self.start_time = time.perf_counter_ns()
        self.metrics.active_requests_metric(self.tags).inc()

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        if self.metrics is None:
            logger.warning("No metrics set for Prometheus metric collection. Metrics will not be exported correctly.")
            return

        if exc_info is not None:
            self.tags["exception_type"] = exc_info[0]

        self.metrics.active_requests_metric(self.tags).dec()
        self.metrics.requests_total_metric(self.tags).inc()
        if self.start_time is not None:
            elapsed_ns = time.perf_counter_ns() - self.start_time
            self.metrics.latency_seconds_metric(self.tags).observe(elapsed_ns / NANOSECONDS_PER_SECOND)

    def on_child_span_created(self, span: Span) -> None:
        if isinstance(span, LocalSpan):
            observer = PrometheusLocalSpanObserver()
        else:
            observer = PrometheusClientSpanObserver()

        span.register(observer)
