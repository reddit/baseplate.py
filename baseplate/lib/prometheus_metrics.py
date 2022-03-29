from typing import Any

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram


# default_buckets creates the default bucket values for histogram metrics.
# we want this to match the baseplate.go default_buckets, ref: https://github.com/reddit/baseplate.go/blob/master/prometheusbp/metrics.go.
# start is the value of the lowest bucket.
# factor is amount to multiply the previous bucket by to get the value for the next bucket.
# count is the number of buckets created.
start = 0.0001
factor = 2.5
count = 14
# creates 14 buckets from 100us ~ 14.9s.
default_buckets = [start * factor ** i for i in range(count)]


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
    buckets=default_buckets,
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

    def latency_seconds_metric(self, tags: dict) -> Any:
        """Return the latency_seconds metrics with labels set"""
        return thrift_server_latency_seconds.labels(
            thrift_method=tags.get("thrift.method", ""),
            thrift_success=tags.get("success", ""),
        )

    def requests_total_metric(self, tags: dict) -> Any:
        """Return the requests_total metrics with labels set"""
        return thrift_server_requests_total.labels(
            thrift_method=tags.get("thrift.method", ""),
            thrift_success=tags.get("success", ""),
            thrift_exception_type=tags.get("exception_type", ""),
            thrift_baseplate_status=tags.get("thrift.status", ""),
            thrift_baseplate_status_code=tags.get("thrift.status_code", ""),
        )

    def active_requests_metric(self, tags: dict) -> Any:
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
