from prometheus_client import Counter, Histogram, Gauge


# default_buckets creates the default bucket values for histogram metrics.
# we want this to match the baseplate.go default_buckets, ref: https://github.com/reddit/baseplate.go/blob/master/prometheusbp/metrics.go.
# start is the value of the lowest bucket.
# factor is amount to multiply the previous bucket by to get the value for the next bucket.
# count is the number of buckets created.
start = 0.0001
factor = 2.5
count = 14
# creates 14 buckets from 100us ~ 14.9s.
default_buckets = [start*factor**i for i in range(count)]


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
    "Description of histogram",
    http_server_histogram_labels,
    buckets=default_buckets,
)
http_server_request_size_bytes = Histogram(
    "http_server_request_size_bytes",
    "Description of histogram",
    http_server_histogram_labels,
    buckets=default_buckets,
)
http_server_response_size_bytes = Histogram(
    "http_server_response_size_bytes",
    "Description of histogram",
    http_server_histogram_labels,
    buckets=default_buckets,
)
http_server_requests_total = Counter(
    "http_server_requests_total",
    "Description of counter",
    http_server_requests_total_labels,
)
http_server_active_requests = Gauge(
    "http_server_active_requests",
    "Description of gauge",
    http_server_active_requests_labels,
)

class PrometheusHTTPServerMetrics():
    def __init__(self):
        pass

    def latency_seconds_metric(self, tags):
        return http_server_latency_seconds.labels(
            http_method = tags.get("http.method", ""),
            http_endpoint = tags.get("http.url", ""),
            http_success = getHTTPSuccessLabel(int(tags.get("http.status_code", ""))),
        )

    def request_size_bytes_metric(self, tags):
        return http_server_request_size_bytes.labels(
            http_method = tags.get("http.method", ""),
            http_endpoint = tags.get("http.url", ""),
            http_success = getHTTPSuccessLabel(int(tags.get("http.status_code", ""))),
        )

    def response_size_bytes_metric(self, tags):
        return http_server_response_size_bytes.labels(
            http_method = tags.get("http.method", ""),
            http_endpoint = tags.get("http.url", ""),
            http_success = getHTTPSuccessLabel(int(tags.get("http.status_code", ""))),
        )

    def requests_total_metric(self, tags):
        return http_server_requests_total.labels(
            http_method = tags.get("http.method", ""),
            http_endpoint = tags.get("http.url", ""),
            http_success = getHTTPSuccessLabel(int(tags.get("http.status_code", ""))),
            http_response_code = tags.get("http.status_code", ""),
        )

    def active_requests_metric(self, tags):
        return http_server_active_requests.labels(
            http_method = tags.get("http.method", ""),
            http_endpoint = tags.get("http.url", ""),
        )


# http client labels and metrics
http_client_latency_labels = [
    "http_method",
    "http_success",
    "http_endpoint",
    "http_slug",
]
http_client_requests_total_labels = [
    "http_method",
    "http_success",
    "http_endpoint",
    "http_response_code",
    "http_slug",
]
http_client_active_requests_labels = [
    "http_method",
    "http_endpoint",
    "http_slug",
]

http_client_latency_seconds = Histogram(
    "http_client_latency_seconds",
    "Description of histogram",
    http_client_latency_labels,
    buckets=default_buckets,
)
http_client_requests_total = Counter(
    "http_client_requests_total",
    "Description of counter",
    http_client_requests_total_labels,
)
http_client_active_requests = Gauge(
    "http_client_active_requests",
    "Description of gauge",
    http_client_active_requests_labels,
)


class PrometheusHTTPClientMetrics():
    def __init__(self):
        pass

    def latency_seconds_metric(self, tags):
        return http_client_latency_seconds.labels(
            http_method = tags.get("http.method", ""),
            http_endpoint = tags.get("http.url", ""),
            http_success = getHTTPSuccessLabel(int(tags.get("http.status_code", ""))),
            http_slug = tags.get("http.slug", ""),
        )

    def requests_total_metric(self, tags):
        return http_client_requests_total.labels(
            http_method = tags.get("http.method", ""),
            http_endpoint = tags.get("http.url", ""),
            http_success = getHTTPSuccessLabel(int(tags.get("http.status_code", ""))),
            http_response_code = tags.get("http.status_code", ""),
            http_slug = tags.get("http.slug", ""),
        )

    def active_requests_metric(self, tags):
        return http_client_active_requests.labels(
            http_method = tags.get("http.method", ""),
            http_endpoint = tags.get("http.url", ""),
            http_slug = tags.get("http.slug", ""),
        )


# thrift server labels and metrics
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

thrift_server_latency_seconds = Histogram(
    "thrift_server_latency_seconds",
    "Description of histogram",
    thrift_server_latency_labels,
    buckets=default_buckets,
)
thrift_server_requests_total = Counter(
    "thrift_server_requests_total",
    "Description of counter",
    thrift_server_requests_total_labels,
)
thrift_server_active_requests = Gauge(
    "thrift_server_active_requests",
    "Description of gauge",
    thrift_server_active_requests_labels,
)

class PrometheusThriftServerMetrics():
    def __init__(self):
        pass

    def latency_seconds_metric(self, tags):
        return thrift_server_latency_seconds.labels(
            thrift_method = tags.get("thrift.method", ""),
            thrift_success = tags.get("success", ""),
        )

    def requests_total_metric(self, tags):
        return thrift_server_requests_total.labels(
            thrift_method = tags.get("thrift.method", ""),
            thrift_success = tags.get("success", ""),
            thrift_exception_type = tags.get("thrift_exception_type", ""),
            thrift_baseplate_status = tags.get("thrift_baseplate_status", ""),
            thrift_baseplate_status_code = tags.get("thrift_baseplate_status_code", ""),
        )

    def active_requests_metric(self, tags):
        return thrift_server_active_requests.labels(
            thrift_method = tags.get("thrift.method", ""),
        )


# thrift client labels and metrics
thrift_client_latency_labels = [
    "thrift_method",
    "thrift_success",
    "thrift_slug",
]
thrift_client_requests_total_labels = [
    "thrift_method",
    "thrift_success",
    "thrift_exception_type",
    "thrift_baseplate_status",
    "thrift_baseplate_status_code",
    "thrift_slug",
]
thrift_client_active_requests_labels = [
    "thrift_method",
    "thrift_slug",
]

thrift_client_latency_seconds = Histogram(
    "thrift_client_latency_seconds",
    "Description of histogram",
    thrift_client_latency_labels,
    buckets=default_buckets,
)
thrift_client_requests_total = Counter(
    "thrift_client_requests_total",
    "Description of counter",
    thrift_client_requests_total_labels,
)
thrift_client_active_requests = Gauge(
    "thrift_client_active_requests",
    "Description of gauge",
    thrift_client_active_requests_labels,
)

class PrometheusThriftClientMetrics():
    def __init__(self):
        pass

    def latency_seconds_metric(self, tags):
        return thrift_client_latency_seconds.labels(
            thrift_method = tags.get("thrift.method", ""),
            thrift_success = tags.get("success", ""),
            thrift_slug = tags.get("thrift.slug", ""),
        )

    def requests_total_metric(self, tags):
        return thrift_client_requests_total.labels(
            thrift_method = tags.get("thrift.method", ""),
            thrift_success = tags.get("success", ""),
            thrift_exception_type = tags.get("thrift_exception_type", ""),
            thrift_baseplate_status = tags.get("thrift_baseplate_status", ""),
            thrift_baseplate_status_code = tags.get("thrift_baseplate_status_code", ""),
            thrift_slug = tags.get("thrift.slug", ""),
        )

    def active_requests_metric(self, tags):
        return thrift_client_active_requests.labels(
            thrift_method = tags.get("thrift.method", ""),
            thrift_slug = tags.get("thrift.slug", ""),
        )

def getHTTPSuccessLabel(httpStatusCode: int) -> bool:
    """
    The HTTP success label is "true" if the status code is 2xx or 3xx, "false" otherwise.
    """
    return httpStatusCode >= 200 and httpStatusCode < 400
