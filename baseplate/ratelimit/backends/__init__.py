from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import time


class RateLimitBackend(object):
    """An interface for rate limit backends to implement.

    :param str key: The name of the rate limit bucket to consume from.
    :param int amount: The amount to consume from the rate limit bucket.
    :param int allowance: The maximum allowance for the rate limit bucket.
    :param int interval: The interval to reset the allowance.

    """
    def consume(self, key, amount, max, bucket_size):
        raise NotImplementedError


def _get_current_bucket(bucket_size):
    current_timestamp_seconds = int(time.time())
    return str(current_timestamp_seconds // bucket_size)
