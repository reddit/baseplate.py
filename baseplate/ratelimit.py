from __future__ import division

import time


class RateLimitExceededException(Exception):
    """This exception gets raised whenever a rate limit is exceeded.
    """
    pass


class RateLimiter:
    """A class for rate limiting actions.

    :param `RateLimitCache` cache: The backend
        to use for storing rate limit counters.
    :param int allowance: The maximum allowance allowed per key.
    :param int interval: The interval (in seconds) to reset allowances.
    :param str key_prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, cache, allowance=None, interval=None, key_prefix=''):
        assert allowance >= 1, 'minimum allowance is 1'
        assert interval >= 1, 'minimum interval is 1'

        self.cache = cache
        self.key_prefix = key_prefix
        self.allowance = allowance
        self.interval = interval

    def consume(self, key, amount=1):
        """Consume the given `amount` from the allowance for the given `key`.

        This will rate
        :py:class:`baseplate.ratelimit.RateLimitExceededException` if the
        allowance for `key` is exhausted.

        :param str key: The name of the rate limit bucket to consume from.
        :param int amount: The amount to consume from the rate limit bucket.

        """
        key = self.key_prefix + key
        if not self.cache.consume(key, amount, self.allowance, self.interval):
            raise RateLimitExceededException('Rate limit exceeded.')


class RateLimitCache:
    """An interface for rate limit backends to implement.

    :param str key: The name of the rate limit bucket to consume from.
    :param int amount: The amount to consume from the rate limit bucket.
    :param int allowance: The maximum allowance for the rate limit bucket.
    :param int interval: The interval to reset the allowance.

    """
    def consume(self, key, amount, max, bucket_size):
        raise NotImplementedError


class RedisRateLimitCache(RateLimitCache):
    """A Redis-backed cache for rate limiting.

    :param redis_client: An instance of
        :py:class:`baseplate.context.redis.MonitoredRedisConnection`.

    """

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def consume(self, key, amount, allowance, interval):
        """Consume the given `amount` from the allowance for the given `key`.

        This will return true if the `key` remains below the `allowance`
        after consuming the given `amount`.

        :param str key: The name of the rate limit bucket to consume from.
        :param int amount: The amount to consume from the rate limit bucket.
        :param int allowance: The maximum allowance for the rate limit bucket.
        :param int interval: The interval to reset the allowance.

        """
        current_bucket = _get_current_bucket(interval)
        key = key + current_bucket
        ttl = interval * 2
        with self.redis_client.pipeline('ratelimit') as pipe:
            pipe.incr(key, amount)
            pipe.expire(key, time=ttl)
            responses = pipe.execute()
        count = responses[0]
        return count <= allowance


def _get_current_bucket(bucket_size):
    current_timestamp_seconds = int(time.time())
    return str(current_timestamp_seconds // bucket_size)
