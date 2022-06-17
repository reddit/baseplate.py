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


def getHTTPSuccessLabel(httpStatusCode: int) -> str:
    """
    The HTTP success label is "true" if the status code is 2xx or 3xx, "false" otherwise.
    """
    return str(200 <= httpStatusCode < 400).lower()
