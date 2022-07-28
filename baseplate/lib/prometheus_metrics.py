# default_latency_buckets creates the default bucket values for time based histogram metrics.
# we want this to match the baseplate.go default_buckets
# bp.go v0 ref: https://github.com/reddit/baseplate.go/blob/master/prometheusbp/metrics.go.
# bp.go v2 ref: https://XXXXXXXXX/baseplate/blob/main/metricsbp/metricsbp.go

default_latency_buckets = [
    0.000100,  # 100us
    0.000500,  # 500us
    0.001000,  # 1ms
    0.002500,  # 2.5ms
    0.005000,  # 5ms
    0.010000,  # 10ms
    0.025000,  # 25ms
    0.050000,  # 50ms
    0.100000,  # 100ms
    0.250000,  # 250ms
    0.500000,  # 500ms
    1.000000,  # 1s
    5.000000,  # 5s
    15.000000,  # 15s (fastly timeout)
    30.000000,  # 30s
]

# Default buckets for size base histograms, from <=8 bytes to 4mB in 20
# increments (8*2^i).  Larger requests go in the +Inf bucket.
default_size_start = 8
default_size_factor = 2
default_size_count = 20
default_size_buckets = [
    default_size_start * default_size_factor ** i for i in range(default_size_count)
]


def getHTTPSuccessLabel(httpStatusCode: int) -> str:
    """
    The HTTP success label is "true" if the status code is 2xx or 3xx, "false" otherwise.
    """
    return str(200 <= httpStatusCode < 400).lower()
