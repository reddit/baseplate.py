from .base import RateLimitBackend
from .base import _get_current_bucket
from ...context import ContextFactory
from ...context.redis import MonitoredRedisConnection


class RedisRateLimitBackendContextFactory(ContextFactory):
    """RedisRateLimitBackend context factory

    :param redis_pool: An instance of :py:class:`redis.ConnectionPool`

    """

    def __init__(self, redis_pool):
        self.redis_pool = redis_pool

    def make_object_for_context(self, name, server_span):
        redis = MonitoredRedisConnection(name, server_span, self.redis_pool)
        return RedisRateLimitBackend(redis)


class RedisRateLimitBackend(RateLimitBackend):
    """A Redis backend for rate limiting.

    :param redis: An instance of
        :py:class:`baseplate.context.redis.MonitoredRedisConnection`.

    """

    def __init__(self, redis):
        self.redis = redis

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
        with self.redis.pipeline('ratelimit') as pipe:
            pipe.incr(key, amount)
            pipe.expire(key, time=ttl)
            responses = pipe.execute()
        count = responses[0]
        return count <= allowance
