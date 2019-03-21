from __future__ import division

import time

from .context import ContextFactory

try:
    from .context.memcache import MonitoredMemcacheConnection
except ImportError as exc:
    pass

try:
    from .context.redis import MonitoredRedisConnection
except ImportError:
    pass


class RateLimitExceededException(Exception):
    """This exception gets raised whenever a rate limit is exceeded.
    """
    pass


class RateLimiterContextFactory(ContextFactory):
    """RateLimiter context factory

    :param backend_factory: An instance of
        :py:class:`baseplate.context.ContextFactory`. The context factory must
        return an instance of :py:class:`baseplate.ratelimit.RateLimitBackend`
    :param int allowance: The maximum allowance allowed per key.
    :param int interval: The interval (in seconds) to reset allowances.
    :param str key_prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, backend_factory, allowance, interval, key_prefix=''):
        self.backend_factory = backend_factory
        self.allowance = allowance
        self.interval = interval
        self.key_prefix = key_prefix

    def make_object_for_context(self, name, server_span):
        backend = self.backend_factory.make_object_for_context(name, server_span)
        return RateLimiter(backend, self.allowance, self.interval,
                           key_prefix=self.key_prefix)


class RateLimiter(object):
    """A class for rate limiting actions.

    :param `RateLimitBackend` backend: The backend to use for storing rate
        limit counters.
    :param int allowance: The maximum allowance allowed per key.
    :param int interval: The interval (in seconds) to reset allowances.
    :param str key_prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, backend, allowance, interval, key_prefix='rl:'):
        if allowance < 1:
            raise ValueError('minimum allowance is 1')
        if interval < 1:
            raise ValueError('minimum interval is 1')
        if not isinstance(backend, RateLimitBackend):
            raise TypeError('backend must be an instance of RateLimitBackend')

        self.backend = backend
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
        if not self.backend.consume(key, amount, self.allowance, self.interval):
            raise RateLimitExceededException('Rate limit exceeded.')


class RedisRateLimitBackendContextFactory(ContextFactory):
    """RedisRateLimitBackend context factory

    :param redis_pool: An instance of :py:class:`redis.ConnectionPool`

    """

    def __init__(self, redis_pool):
        self.redis_pool = redis_pool

    def make_object_for_context(self, name, server_span):
        redis = MonitoredRedisConnection(name, server_span, self.redis_pool)
        return RedisRateLimitBackend(redis)


class MemcacheRateLimitBackendContextFactory(ContextFactory):
    """MemcacheRateLimitBackend context factory

    :param memcache_pool: An instance of
        :py:class:`~pymemcache.client.base.PooledClient`

    """

    def __init__(self, memcache_pool):
        self.memcache_pool = memcache_pool

    def make_object_for_context(self, name, server_span):
        memcache = MonitoredMemcacheConnection(name, server_span, self.memcache_pool)
        return MemcacheRateLimitBackend(memcache)


class RateLimitBackend(object):
    """An interface for rate limit backends to implement.

    :param str key: The name of the rate limit bucket to consume from.
    :param int amount: The amount to consume from the rate limit bucket.
    :param int allowance: The maximum allowance for the rate limit bucket.
    :param int interval: The interval to reset the allowance.

    """
    def consume(self, key, amount, max, bucket_size):
        raise NotImplementedError


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


class MemcacheRateLimitBackend(RateLimitBackend):
    """A Memcache backend for rate limiting.

    :param memcache: An instance of
        :py:class:`baseplate.context.memcache.MonitoredMemcacheConnection`.

    """

    def __init__(self, memcache):
        self.memcache = memcache

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
        self.memcache.add(key, 0, expire=ttl)
        count = self.memcache.incr(key, 1) or 0
        return count <= allowance


def _get_current_bucket(bucket_size):
    current_timestamp_seconds = int(time.time())
    return str(current_timestamp_seconds // bucket_size)
