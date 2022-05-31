import logging
import time

from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import LocalSpan
from baseplate import RequestContext
from baseplate import Span
from baseplate import SpanObserver
from baseplate.lib.prometheus_metrics import get_metrics_for_prefix
from baseplate.lib.prometheus_metrics import PrometheusGenericSpanMetrics
from baseplate.lib.prometheus_metrics import PrometheusHTTPClientMetrics
from baseplate.lib.prometheus_metrics import PrometheusHTTPServerMetrics
from baseplate.lib.prometheus_metrics import PrometheusThriftClientMetrics
from baseplate.lib.prometheus_metrics import PrometheusThriftServerMetrics
from baseplate.thrift.ttypes import Error
from baseplate.thrift.ttypes import ErrorCode


NANOSECONDS_PER_SECOND = 1e9

logger = logging.getLogger(__name__)


class PrometheusBaseplateObserver(BaseplateObserver):
    """Metrics collecting observer.

    This observer reports Prometheus metrics for HTTP or Thrift client and servers.

    There must be a "protocol" tag set on the server_span that indictes the protocol, either "http" or "thrift",
    i.e. `span.set_tag("protocol", "http")` or `span.set_tag("protocol", "thrift")`.

    """

    def __init__(self) -> None:
        pass

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        observer = PrometheusServerSpanObserver(server_span)
        server_span.register(observer)


class PrometheusServerSpanObserver(SpanObserver):
    def __init__(self, span: Optional[Span] = None) -> None:
        self.tags: Dict[str, Any] = {}
        self.start_time: Optional[int] = None
        self.metrics: Optional[
            Union[
                PrometheusHTTPServerMetrics,
                PrometheusThriftServerMetrics,
                PrometheusGenericSpanMetrics,
            ]
        ] = None
        self.span = span

    def on_set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    def get_labels(self) -> Dict[str, str]:
        return self.tags

    @property
    def protocol(self) -> str:
        return self.tags.get("protocol", "unknown")

    def set_metrics_by_protocol(self) -> None:
        if self.protocol == "thrift":
            self.metrics = PrometheusThriftServerMetrics()
        elif self.protocol == "http":
            self.metrics = PrometheusHTTPServerMetrics()
        else:
            logger.debug(
                "Unknown protocol %s. 'http' and 'thrift' are supported.",
                self.protocol,
            )
            self.metrics = get_metrics_for_prefix(self.span.name if self.span else "generic_server")

    def on_start(self) -> None:
        self.set_metrics_by_protocol()
        if self.metrics is None:
            logger.warning(
                "No metrics set for Prometheus metric collection. Metrics will not be exported correctly."
            )
            return
        self.start_time = time.perf_counter_ns()
        self.metrics.active_requests_metric(self.tags).inc()

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        if self.metrics is None:
            logger.warning(
                "No metrics set for Prometheus metric collection. Metrics will not be exported correctly."
            )
            return

        if exc_info is not None:
            self.tags["exception_type"] = exc_info[1].__class__.__name__
            self.tags["success"] = "false"

        if self.tags.get("exception_type", "") == "":
            self.tags["success"] = "true"

        self.metrics.active_requests_metric(self.tags).dec()
        self.metrics.requests_total_metric(self.tags).inc()

        if self.start_time is not None:
            elapsed_ns = time.perf_counter_ns() - self.start_time
            self.metrics.latency_seconds_metric(self.tags).observe(
                elapsed_ns / NANOSECONDS_PER_SECOND
            )

        if isinstance(self.metrics, PrometheusHTTPServerMetrics):
            self.metrics.response_size_bytes_metric(self.tags).observe(
                self.tags.get("http.response_length") or 0
            )

            self.metrics.request_size_bytes_metric(self.tags).observe(
                self.tags.get("http.request_length") or 0
            )

    def on_child_span_created(self, span: Span) -> None:
        observer: Any = None
        if isinstance(span, LocalSpan):
            observer = PrometheusLocalSpanObserver(span.name)
        else:
            observer = PrometheusClientSpanObserver(prefix=span.component_name)

        span.register(observer)


class PrometheusClientSpanObserver(SpanObserver):
    prefix = None

    def __init__(self, prefix: Optional[str] = "generic_client") -> None:
        self.tags: Dict[str, Any] = {}
        self.start_time: Optional[int] = None
        self.metrics: Optional[
            Union[
                PrometheusHTTPClientMetrics,
                PrometheusThriftClientMetrics,
                PrometheusGenericSpanMetrics,
            ]
        ] = None
        self.prefix = prefix or "generic_client"

    def on_set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    def get_labels(self) -> Dict[str, str]:
        return self.tags

    @property
    def protocol(self) -> str:
        return self.tags.get("protocol", "unknown")

    def set_metrics_by_protocol(self) -> None:
        if self.protocol == "http":
            self.metrics = PrometheusHTTPClientMetrics()
        elif self.protocol == "thrift":
            self.metrics = PrometheusThriftClientMetrics()
        else:
            logger.debug(
                "Unknown protocol %s. 'http' and 'thrift' are supported.",
                self.protocol,
            )
            self.metrics = get_metrics_for_prefix(self.prefix if self.prefix else "generic_client")

    def on_start(self) -> None:
        self.set_metrics_by_protocol()
        if self.metrics is None:
            logger.warning(
                "No metrics set for Prometheus metric collection. Metrics will not be exported correctly."
            )
            return
        self.start_time = time.perf_counter_ns()
        self.metrics.active_requests_metric(self.tags).inc()

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        if self.metrics is None:
            logger.warning(
                "No metrics set for Prometheus metric collection. Metrics will not be exported correctly."
            )
            return

        self.tags["success"] = "true"
        if exc_info is not None:
            exc = exc_info[1]
            self.tags["exception_type"] = exc.__class__.__name__
            self.tags["success"] = "false"
            if self.protocol == "thrift" and isinstance(exc, Error):
                self.tags["thrift_status_code"] = exc.code
                self.tags["thrift_status"] = ErrorCode()._VALUES_TO_NAMES.get(exc.code, "")

        self.metrics.active_requests_metric(self.tags).dec()
        self.metrics.requests_total_metric(self.tags).inc()
        if self.start_time is not None:
            elapsed_ns = time.perf_counter_ns() - self.start_time
            self.metrics.latency_seconds_metric(self.tags).observe(
                elapsed_ns / NANOSECONDS_PER_SECOND
            )

    def on_child_span_created(self, span: Span) -> None:
        observer: Optional[SpanObserver] = None
        if isinstance(span, LocalSpan):
            observer = PrometheusLocalSpanObserver(span.name)
        else:
            observer = PrometheusClientSpanObserver(prefix=span.component_name)

        span.register(observer)


class PrometheusLocalSpanObserver(SpanObserver):
    prefix = "local"

    def __init__(self, span_name: Optional[str] = None) -> None:
        self.tags: Dict[str, Any] = {"span_name": span_name if span_name is not None else ""}
        self.start_time: Optional[int] = None

        self.metrics: PrometheusGenericSpanMetrics = get_metrics_for_prefix(self.prefix)

    def on_set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    @property
    def protocol(self) -> str:
        return self.prefix

    def get_labels(self) -> Dict[str, Any]:
        return self.tags

    def on_start(self) -> None:
        self.start_time = time.perf_counter_ns()
        self.metrics.active_requests_metric(self.tags).inc()

    def on_incr_tag(self, key: str, delta: float) -> None:
        pass

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.tags["success"] = "true"
        if exc_info is not None:
            self.tags["exception_type"] = exc_info[1].__class__.__name__
            self.tags["success"] = "false"

        self.metrics.active_requests_metric(self.tags).dec()
        self.metrics.requests_total_metric(self.tags).inc()
        if self.start_time is not None:
            elapsed_ns = time.perf_counter_ns() - self.start_time
            self.metrics.latency_seconds_metric(self.tags).observe(
                elapsed_ns / NANOSECONDS_PER_SECOND
            )

    def on_child_span_created(self, span: Span) -> None:
        observer: Optional[SpanObserver] = None
        if isinstance(span, LocalSpan):
            observer = PrometheusLocalSpanObserver(span_name=span.name)
        else:
            observer = PrometheusClientSpanObserver(prefix=span.component_name)

        span.register(observer)
