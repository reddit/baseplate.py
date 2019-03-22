from .base import RateLimitBackend
from .base import _get_current_bucket
from ...context import ContextFactory
from ...context.memcache import MonitoredMemcacheConnection


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
        count = self.memcache.incr(key, amount) or amount
        return count <= allowance
