import atexit
import os
import time

from http.server import ThreadingHTTPServer
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
from baseplate.observers.timeout import ServerTimeout
from baseplate.server import make_listener
from gevent.pywsgi import LoggingLogAdapter
from gevent.pywsgi import WSGIServer

from prometheus_client import start_http_server
from prometheus_client.multiprocess import MultiProcessorCollector
from prometheus_client import generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST


NANOSECONDS_PER_SECOND = 1e9


class PrometheusBaseplateObserver(BaseplateObserver):
    def __init__(self):
        pass

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        observer = PrometheusServerSpanObserver()
        server_span.register(observer)


LABELS_BY_PROTOCOL = {
    "http": {
        "http.method": "http_method",
    },
}


class PrometheusSpanObserver(SpanObserver):
    def __init__(self):
        self.tags = {}
        self.start_time = None

    def on_set_tag(self, key: str, value: Any) -> None:
        self.tags[key] = value

    @property
    def protocol(self):
        return self.tags.get("protocol", "unknown")

    def on_start(self) -> None:
        self.start_time = time.perf_counter_ns()

    def on_incr_tag(self, key: str, delta: float) -> None:
        # TODO: tags
        Counter(key).inc()

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        # if another observer threw an exception before we got our on_start()
        # called, start_time may not be set
        if self.start_time is not None:
            prefix = self.get_prefix()
            counter = Counter(f"{prefix}_requests_total")
            counter.labels(**self.tags).inc()

            elapsed_ns = time.perf_counter_ns() - self.start_time
            histogram = Histogram(f"{prefix}_latency_seconds")
            histogram.observe(elapsed_ns / NANOSECONDS_PER_SECOND, self.tags)

    def on_child_span_created(self, span: Span) -> None:
        if isinstance(span, LocalSpan):
            observer = PrometheusLocalSpanObserver()
        else:
            observer = PrometheusClientSpanObserver()

        span.register(observer)


class PrometheusServerSpanObserver(SpanObserver):
    def get_prefix(self) -> str:
        return f"{self.protocol}_server"

    def get_labels(self) -> dict[str, str]:
        pass


class PrometheusClientSpanObserver(SpanObserver):
    def get_prefix(self) -> str:
        return f"{self.protocol}_client"

    def get_labels(self) -> dict[str, str]:
        pass


class PrometheusLocalSpanObserver(SpanObserver):
    def get_prefix(self) -> str:
        return "local"

    def get_labels(self) -> dict[str, str]:
        pass
