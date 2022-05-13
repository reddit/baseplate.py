from typing import Any
from typing import Dict

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram


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

# http client labels and metrics
http_client_latency_labels = [
    "http_method",
    "http_success",
    "http_slug",
]
http_client_requests_total_labels = [
    "http_method",
    "http_success",
    "http_response_code",
    "http_slug",
]
http_client_active_requests_labels = [
    "http_method",
    "http_slug",
]

# Latency histogram of HTTP calls made by clients
# buckets are defined above (from 100µs to ~14.9s)
http_client_latency_seconds = Histogram(
    "http_client_latency_seconds",
    "Latency histogram of HTTP calls made by clients",
    http_client_latency_labels,
    buckets=default_latency_buckets,
)

# Counter counting total HTTP requests started by a given client
http_client_requests_total = Counter(
    "http_client_requests_total",
    "Total number of HTTP requests started by a given client",
    http_client_requests_total_labels,
)

# Gauge showing current number of active requests by a given client
http_client_active_requests = Gauge(
    "http_client_active_requests",
    "Number of active requests for a given client",
    http_client_active_requests_labels,
)


class PrometheusHTTPClientMetrics:
    def __init__(self) -> None:
        pass

    def latency_seconds_metric(self, tags: Dict[str, Any]) -> Histogram:
        return http_client_latency_seconds.labels(
            http_method=tags.get("http.method", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
            http_slug=tags.get("http.slug", ""),
        )

    def requests_total_metric(self, tags: Dict[str, Any]) -> Counter:
        return http_client_requests_total.labels(
            http_method=tags.get("http.method", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
            http_response_code=tags.get("http.status_code", ""),
            http_slug=tags.get("http.slug", ""),
        )

    def active_requests_metric(self, tags: Dict[str, Any]) -> Gauge:
        return http_client_active_requests.labels(
            http_method=tags.get("http.method", ""),
            http_slug=tags.get("http.slug", ""),
        )

    def get_latency_seconds_metric(self) -> Histogram:
        return http_client_latency_seconds

    def get_requests_total_metric(self) -> Counter:
        return http_client_requests_total

    def get_active_requests_metric(self) -> Gauge:
        return http_client_active_requests


# thrift server labels
thrift_server_latency_labels = [
    "thrift_method",
    "thrift_success",
]
thrift_server_requests_total_labels = [
    "thrift_method",
    "thrift_success",
    "thrift_exception_type",
    "thrift_baseplate_status",
    "thrift_baseplate_status_code",
]
thrift_server_active_requests_labels = ["thrift_method"]

# thrift server metrics
thrift_server_latency_seconds = Histogram(
    "thrift_server_latency_seconds",
    "RPC latencies",
    thrift_server_latency_labels,
    buckets=default_latency_buckets,
)

thrift_server_requests_total = Counter(
    "thrift_server_requests_total",
    "Total RPC request count",
    thrift_server_requests_total_labels,
)

thrift_server_active_requests = Gauge(
    "thrift_server_active_requests",
    "The number of in-flight requests being handled by the service",
    thrift_server_active_requests_labels,
)


class PrometheusThriftServerMetrics:
    def __init__(self) -> None:
        pass

    def latency_seconds_metric(self, tags: Dict[str, str]) -> Any:
        """Return the latency_seconds metrics with labels set"""
        return thrift_server_latency_seconds.labels(
            thrift_method=tags.get("thrift.method", ""),
            thrift_success=tags.get("success", ""),
        )

    def requests_total_metric(self, tags: Dict[str, str]) -> Any:
        """Return the requests_total metrics with labels set"""
        return thrift_server_requests_total.labels(
            thrift_method=tags.get("thrift.method", ""),
            thrift_success=tags.get("success", ""),
            thrift_exception_type=tags.get("exception_type", ""),
            thrift_baseplate_status=tags.get("thrift.status", ""),
            thrift_baseplate_status_code=tags.get("thrift.status_code", ""),
        )

    def active_requests_metric(self, tags: Dict[str, str]) -> Any:
        """Return the active_requests metrics with labels set"""
        return thrift_server_active_requests.labels(
            thrift_method=tags.get("thrift.method", ""),
        )

    def get_latency_seconds_metric(self) -> Histogram:
        """Return the latency_seconds metrics"""
        return thrift_server_latency_seconds

    def get_requests_total_metric(self) -> Counter:
        """Return the requests_total metrics"""
        return thrift_server_requests_total

    def get_active_requests_metric(self) -> Gauge:
        """Return the active_requests metrics"""
        return thrift_server_active_requests


thrift_client_active_gauge = Gauge(
    "thrift_client_active_requests", "Current in-flight requests", ["thrift_slug", "thrift_method"]
)

thrift_client_latency_histogram = Histogram(
    "thrift_client_latency_seconds",
    "Latency of thrift client requests",
    ["thrift_slug", "thrift_success"],
    buckets=default_latency_buckets,
)

thrift_client_requests_counter = Counter(
    "thrift_client_requests_total",
    "Total number of outgoing requests",
    [
        "thrift_slug",
        "thrift_success",
        "thrift_exception_type",
        "thrift_baseplate_status",
        "thrift_baseplate_status_code",
    ],
)


class PrometheusThriftClientMetrics:
    def __init__(self) -> None:
        pass

    def active_requests_metric(self, tags: Dict) -> Gauge:
        return thrift_client_active_gauge.labels(
            thrift_slug=tags.get("slug", ""), thrift_method=tags.get("method", "")
        )

    def requests_total_metric(self, tags: Dict) -> Counter:
        return thrift_client_requests_counter.labels(
            thrift_slug=tags.get("slug", ""),
            thrift_success=tags.get("success", ""),
            thrift_exception_type=tags.get("exception_type", ""),
            thrift_baseplate_status=tags.get("thrift_status", ""),
            thrift_baseplate_status_code=tags.get("thrift_status_code", ""),
        )

    def latency_seconds_metric(self, tags: Dict) -> Histogram:
        return thrift_client_latency_histogram.labels(
            thrift_slug=tags.get("slug", ""), thrift_success=tags.get("success", "")
        )

    def get_latency_seconds_metric(self) -> Histogram:
        """Return the latency_seconds metrics"""
        return thrift_client_latency_histogram

    def get_requests_total_metric(self) -> Counter:
        """Return the requests_total metrics"""
        return thrift_client_requests_counter

    def get_active_requests_metric(self) -> Gauge:
        """Return the active_requests metrics"""
        return thrift_client_active_gauge


# http server labels and metrics
http_server_histogram_labels = [
    "http_method",
    "http_endpoint",
    "http_success",
]

http_server_requests_total_labels = [
    "http_method",
    "http_endpoint",
    "http_success",
    "http_response_code",
]

http_server_active_requests_labels = [
    "http_method",
    "http_endpoint",
]

http_server_latency_seconds = Histogram(
    "http_server_latency_seconds",
    "Time spent processing requests",
    http_server_histogram_labels,
    buckets=default_latency_buckets,
)

http_server_request_size_bytes = Histogram(
    "http_server_request_size_bytes",
    "Size of incoming requests in bytes",
    http_server_histogram_labels,
    buckets=default_size_buckets,
)

http_server_response_size_bytes = Histogram(
    "http_server_response_size_bytes",
    "Size of outgoing responses in bytes",
    http_server_histogram_labels,
    buckets=default_size_buckets,
)

http_server_requests_total = Counter(
    "http_server_requests_total",
    "Total number of request handled",
    http_server_requests_total_labels,
)
http_server_active_requests = Gauge(
    "http_server_active_requests",
    "Current requests in flight",
    http_server_active_requests_labels,
)


class PrometheusHTTPServerMetrics:
    def __init__(self) -> None:
        pass

    def latency_seconds_metric(self, tags: Dict[str, Any]) -> Histogram:
        return http_server_latency_seconds.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
        )

    def requests_total_metric(self, tags: Dict[str, Any]) -> Counter:
        return http_server_requests_total.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
            http_response_code=tags.get("http.status_code", ""),
        )

    def active_requests_metric(self, tags: Dict[str, Any]) -> Gauge:
        return http_server_active_requests.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
        )

    def request_size_bytes_metric(self, tags: Dict[str, Any]) -> Histogram:
        return http_server_request_size_bytes.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
        )

    def response_size_bytes_metric(self, tags: Dict[str, Any]) -> Histogram:
        return http_server_response_size_bytes.labels(
            http_method=tags.get("http.method", ""),
            http_endpoint=tags.get("http.route", ""),
            http_success=getHTTPSuccessLabel(int(tags.get("http.status_code", "0"))),
        )

    def get_latency_seconds_metric(self) -> Any:
        return http_server_latency_seconds

    def get_requests_total_metric(self) -> Any:
        return http_server_requests_total

    def get_active_requests_metric(self) -> Any:
        return http_server_active_requests


def getHTTPSuccessLabel(httpStatusCode: int) -> str:
    """
    The HTTP success label is "true" if the status code is 2xx or 3xx, "false" otherwise.
    """
    return str(200 <= httpStatusCode < 400).lower()
