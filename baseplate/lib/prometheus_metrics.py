import logging
import re

from typing import Any
from typing import Dict

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram

logger = logging.getLogger(__name__)

# default_latency_buckets creates the default bucket values for time based histogram metrics.
# we want this to match the baseplate.go default_buckets, ref: https://github.com/reddit/baseplate.go/blob/master/prometheusbp/metrics.go.
# start is the value of the lowest bucket.
# factor is amount to multiply the previous bucket by to get the value for the next bucket.
# count is the number of buckets created.
start = 0.0001
factor = 2.5
count = 14
# creates 14 buckets from 100us ~ 14.9s.
default_latency_buckets = [start * factor ** i for i in range(count)]

# Default buckets for size base histograms, from <=8 bytes to 4mB in 20
# increments (8*2^i).  Larger requests go in the +Inf bucket.
default_size_start = 8
default_size_factor = 2
default_size_count = 20
default_size_buckets = [
    default_size_start * default_size_factor ** i for i in range(default_size_count)
]

generic_metrics = {}


class PrometheusHTTPClientMetrics:
    prefix = "http_client"

    # http client labels and metrics
    latency_labels = [
        "http_method",
        "http_success",
        "http_slug",
    ]
    requests_total_labels = [
        "http_method",
        "http_success",
        "http_response_code",
        "http_slug",
    ]
    active_requests_labels = [
        "http_method",
        "http_slug",
    ]

    # Latency histogram of HTTP calls made by clients
    # buckets are defined above (from 100µs to ~14.9s)
    latency_seconds = Histogram(
        f"{prefix}_latency_seconds",
        "Latency histogram of HTTP calls made by clients",
        latency_labels,
        buckets=default_latency_buckets,
    )
    # Counter counting total HTTP requests started by a given client
    requests_total = Counter(
        f"{prefix}_requests_total",
        "Total number of HTTP requests started by a given client",
        requests_total_labels,
    )
    # Gauge showing current number of active requests by a given client
    active_requests = Gauge(
        f"{prefix}_active_requests",
        "Number of active requests for a given client",
        active_requests_labels,
    )

    def __init__(self) -> None:
        pass

    @classmethod
    def latency_seconds_metric(cls, tags: Dict[str, Any]) -> Histogram:
        return cls.latency_seconds.labels(
            http_method=tags.get("http.method", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
            http_slug=tags.get("http.slug", ""),
        )

    @classmethod
    def requests_total_metric(cls, tags: Dict[str, Any]) -> Counter:
        return cls.requests_total.labels(
            http_method=tags.get("http.method", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
            http_response_code=tags.get("http.status_code", ""),
            http_slug=tags.get("http.slug", ""),
        )

    @classmethod
    def active_requests_metric(cls, tags: Dict[str, Any]) -> Gauge:
        return cls.active_requests.labels(
            http_method=tags.get("http.method", ""),
            http_slug=tags.get("http.slug", ""),
        )

    @classmethod
    def get_latency_seconds_metric(cls) -> Histogram:
        return cls.latency_seconds

    @classmethod
    def get_requests_total_metric(cls) -> Counter:
        return cls.requests_total

    @classmethod
    def get_active_requests_metric(cls) -> Gauge:
        return cls.active_requests


class PrometheusHTTPServerMetrics:
    prefix = "http_server"

    # http server labels and metrics
    histogram_labels = [
        "http_method",
        "http_endpoint",
        "http_success",
    ]
    requests_total_labels = [
        "http_method",
        "http_endpoint",
        "http_success",
        "http_response_code",
    ]
    active_requests_labels = [
        "http_method",
        "http_endpoint",
    ]

    latency_seconds = Histogram(
        f"{prefix}_latency_seconds",
        "Time spent processing requests",
        histogram_labels,
        buckets=default_latency_buckets,
    )
    request_size_bytes = Histogram(
        f"{prefix}_request_size_bytes",
        "Size of incoming requests in bytes",
        histogram_labels,
        buckets=default_size_buckets,
    )
    response_size_bytes = Histogram(
        f"{prefix}_response_size_bytes",
        "Size of outgoing responses in bytes",
        histogram_labels,
        buckets=default_size_buckets,
    )
    requests_total = Counter(
        f"{prefix}_requests_total",
        "Total number of request handled",
        requests_total_labels,
    )
    active_requests = Gauge(
        f"{prefix}_active_requests",
        "Current requests in flight",
        active_requests_labels,
    )

    def __init__(self) -> None:
        pass

    @classmethod
    def latency_seconds_metric(cls, tags: Dict[str, Any]) -> Histogram:
        return cls.latency_seconds.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
        )

    @classmethod
    def requests_total_metric(cls, tags: Dict[str, Any]) -> Counter:
        return cls.requests_total.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
            http_response_code=tags.get("http.status_code", ""),
        )

    @classmethod
    def active_requests_metric(cls, tags: Dict[str, Any]) -> Gauge:
        return cls.active_requests.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
        )

    @classmethod
    def request_size_bytes_metric(cls, tags: Dict[str, Any]) -> Histogram:
        return cls.request_size_bytes.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
        )

    @classmethod
    def response_size_bytes_metric(cls, tags: Dict[str, Any]) -> Histogram:
        return cls.response_size_bytes.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
        )

    @classmethod
    def get_latency_seconds_metric(cls) -> Any:
        return cls.latency_seconds

    @classmethod
    def get_requests_total_metric(cls) -> Any:
        return cls.requests_total

    @classmethod
    def get_active_requests_metric(cls) -> Any:
        return cls.active_requests


class PrometheusThriftClientMetrics:
    prefix = "thrift_client"

    latency_labels = [
        "thrift_slug",
        "thrift_success",
    ]
    requests_total_labels = [
        "thrift_slug",
        "thrift_success",
        "thrift_exception_type",
        "thrift_baseplate_status",
        "thrift_baseplate_status_code",
    ]
    active_requests_labels = [
        "thrift_slug",
        "thrift_method",
    ]

    latency_seconds = Histogram(
        f"{prefix}_latency_seconds",
        "Latency of thrift client requests",
        latency_labels,
        buckets=default_latency_buckets,
    )
    requests_total = Counter(
        f"{prefix}_requests_total",
        "Total number of outgoing requests",
        requests_total_labels,
    )
    active_requests = Gauge(
        f"{prefix}_active_requests",
        "Current in-flight requests",
        active_requests_labels,
    )

    def __init__(self) -> None:
        pass

    @classmethod
    def active_requests_metric(cls, tags: Dict) -> Gauge:
        return cls.active_requests.labels(
            thrift_slug=tags.get("slug", ""), thrift_method=tags.get("method", "")
        )

    @classmethod
    def requests_total_metric(cls, tags: Dict) -> Counter:
        return cls.requests_total.labels(
            thrift_slug=tags.get("slug", ""),
            thrift_success=tags.get("success", ""),
            thrift_exception_type=tags.get("exception_type", ""),
            thrift_baseplate_status=tags.get("thrift_status", ""),
            thrift_baseplate_status_code=tags.get("thrift_status_code", ""),
        )

    @classmethod
    def latency_seconds_metric(cls, tags: Dict) -> Histogram:
        return cls.latency_seconds.labels(
            thrift_slug=tags.get("slug", ""), thrift_success=tags.get("success", "")
        )

    @classmethod
    def get_latency_seconds_metric(cls) -> Histogram:
        """Return the latency_seconds metrics"""
        return cls.latency_seconds

    @classmethod
    def get_requests_total_metric(cls) -> Counter:
        """Return the requests_total metrics"""
        return cls.requests_total

    @classmethod
    def get_active_requests_metric(cls) -> Gauge:
        """Return the active_requests metrics"""
        return cls.active_requests


class PrometheusThriftServerMetrics:
    prefix = "thrift_server"

    # thrift server labels
    latency_labels = [
        "thrift_method",
        "thrift_success",
    ]
    requests_total_labels = [
        "thrift_method",
        "thrift_success",
        "thrift_exception_type",
        "thrift_baseplate_status",
        "thrift_baseplate_status_code",
    ]
    active_requests_labels = ["thrift_method"]

    # thrift server metrics
    latency_seconds = Histogram(
        f"{prefix}_latency_seconds",
        "RPC latencies",
        latency_labels,
        buckets=default_latency_buckets,
    )
    requests_total = Counter(
        f"{prefix}_requests_total",
        "Total RPC request count",
        requests_total_labels,
    )
    active_requests = Gauge(
        f"{prefix}_active_requests",
        "The number of in-flight requests being handled by the service",
        active_requests_labels,
    )

    def __init__(self) -> None:
        pass

    @classmethod
    def latency_seconds_metric(cls, tags: Dict[str, str]) -> Any:
        """Return the latency_seconds metrics with labels set"""
        return cls.latency_seconds.labels(
            thrift_method=tags.get("thrift.method", ""),
            thrift_success=tags.get("success", ""),
        )

    @classmethod
    def requests_total_metric(cls, tags: Dict[str, str]) -> Any:
        """Return the requests_total metrics with labels set"""
        return cls.requests_total.labels(
            thrift_method=tags.get("thrift.method", ""),
            thrift_success=tags.get("success", ""),
            thrift_exception_type=tags.get("exception_type", ""),
            thrift_baseplate_status=tags.get("thrift.status", ""),
            thrift_baseplate_status_code=tags.get("thrift.status_code", ""),
        )

    @classmethod
    def active_requests_metric(cls, tags: Dict[str, str]) -> Any:
        """Return the active_requests metrics with labels set"""
        return cls.active_requests.labels(
            thrift_method=tags.get("thrift.method", ""),
        )

    @classmethod
    def get_latency_seconds_metric(cls) -> Histogram:
        """Return the latency_seconds metrics"""
        return cls.latency_seconds

    @classmethod
    def get_requests_total_metric(cls) -> Counter:
        """Return the requests_total metrics"""
        return cls.requests_total

    @classmethod
    def get_active_requests_metric(cls) -> Gauge:
        """Return the active_requests metrics"""
        return cls.active_requests


def getHTTPSuccessLabel(httpStatusCode: int) -> str:
    """
    The HTTP success label is "true" if the status code is 2xx or 3xx, "false" otherwise.
    """
    return str(200 <= httpStatusCode < 400).lower()


class PrometheusGenericSpanMetrics:
    prefix = "generic"

    # local labels and metrics
    labels = [
        "span",
    ]

    # Latency histogram of local span
    # buckets are defined above (from 100µs to ~14.9s)
    latency_seconds = Histogram(
        f"{prefix}_latency_seconds",
        "Latency histogram of local span",
        labels,
        buckets=default_latency_buckets,
    )
    # Counter counting total local spans started
    requests_total = Counter(
        f"{prefix}_requests_total",
        "Total number of local spans started",
        labels,
    )
    # Gauge showing current number of local spans
    active_requests = Gauge(
        f"{prefix}_active_requests",
        "Number of active local spans",
        labels,
    )

    def __init__(self) -> None:
        pass

    @classmethod
    def latency_seconds_metric(cls, tags: Dict) -> Histogram:
        return cls.latency_seconds.labels(
            span=tags.get("span_name", ""),
        )

    @classmethod
    def requests_total_metric(cls, tags: Dict) -> Counter:
        return cls.requests_total.labels(
            span=tags.get("span_name", ""),
        )

    @classmethod
    def active_requests_metric(cls, tags: Dict) -> Gauge:
        return cls.active_requests.labels(
            span=tags.get("span_name", ""),
        )

    @classmethod
    def get_latency_seconds_metric(cls) -> Histogram:
        return cls.latency_seconds

    @classmethod
    def get_requests_total_metric(cls) -> Counter:
        return cls.requests_total

    @classmethod
    def get_active_requests_metric(cls) -> Gauge:
        return cls.active_requests


def get_metrics_for_prefix(prefix: str) -> PrometheusGenericSpanMetrics:
    # make sure metric names don't include disallowed chars
    # https://github.com/prometheus/client_python/blob/748ffb00600dc25fbd22d37d549578e8e370d996/prometheus_client/metrics_core.py#L10
    prefix = prefix.replace(".", "_")
    prefix = re.sub("[^0-9a-zA-Z_:]+", "", prefix)

    if prefix not in generic_metrics:
        # local labels and metrics
        labels = [
            "span",
        ]
        generic_metrics[prefix] = type(
            f"PrometheusGenericSpanMetrics<{prefix}>",
            (PrometheusGenericSpanMetrics,),
            {
                "prefix": prefix,
                # Reset the class attributes to avoid having the same prefix in the metric names
                "labels": labels,
                "latency_seconds": Histogram(
                    f"{prefix}_latency_seconds",
                    f"Latency histogram of {prefix} span",
                    labels,
                    buckets=default_latency_buckets,
                ),
                "requests_total": Counter(
                    f"{prefix}_requests_total",
                    f"Total number of {prefix} spans started",
                    labels,
                ),
                "active_requests": Gauge(
                    f"{prefix}_active_requests",
                    f"Number of active {prefix} spans",
                    labels,
                ),
            },
        )
        logger.debug("Created new metrics class for prefix %s", prefix)
    return generic_metrics[prefix]  # type: ignore
