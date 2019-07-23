from redis import ConnectionPool

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.clients.redis import MonitoredRedisConnection
from baseplate.clients.redis import RedisContextFactory
from baseplate.lib.ratelimit.backends import _get_current_bucket
from baseplate.lib.ratelimit.backends import RateLimitBackend


class RedisRateLimitBackendContextFactory(ContextFactory):
    """RedisRateLimitBackend context factory.

    :param redis_pool: The redis pool to back this ratelimit.
    :param prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, redis_pool: ConnectionPool, prefix: str = "rl:"):
        self.redis_context_factory = RedisContextFactory(redis_pool)
        self.prefix = prefix

    def make_object_for_context(self, name: str, span: Span) -> "RedisRateLimitBackend":
        redis = self.redis_context_factory.make_object_for_context(name, span)
        return RedisRateLimitBackend(redis, prefix=self.prefix)


class RedisRateLimitBackend(RateLimitBackend):
    """A Redis backend for rate limiting.

    :param redis: An instance of
        :py:class:`baseplate.clients.redis.MonitoredRedisConnection`.
    :param prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, redis: MonitoredRedisConnection, prefix: str = "rl:"):
        self.redis = redis
        self.prefix = prefix

    def consume(self, key: str, amount: int, allowance: int, interval: int) -> bool:
        """Consume the given `amount` from the allowance for the given `key`.

        This will return true if the `key` remains below the `allowance`
        after consuming the given `amount`.

        :param key: The name of the rate limit bucket to consume from.
        :param amount: The amount to consume from the rate limit bucket.
        :param allowance: The maximum allowance for the rate limit bucket.
        :param interval: The interval to reset the allowance.

        """
        current_bucket = _get_current_bucket(interval)
        key = self.prefix + key + current_bucket
        ttl = interval * 2
        with self.redis.pipeline("ratelimit") as pipe:
            pipe.incr(key, amount)
            pipe.expire(key, time=ttl)
            responses = pipe.execute()
        count = responses[0]
        return count <= allowance
