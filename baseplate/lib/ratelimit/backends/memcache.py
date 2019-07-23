from pymemcache.client import PooledClient

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.clients.memcache import MemcacheContextFactory
from baseplate.clients.memcache import MonitoredMemcacheConnection
from baseplate.lib.ratelimit.backends import _get_current_bucket
from baseplate.lib.ratelimit.backends import RateLimitBackend


class MemcacheRateLimitBackendContextFactory(ContextFactory):
    """MemcacheRateLimitBackend context factory.

    :param memcache_pool: The memcache pool to back this ratelimit.
    :param prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, memcache_pool: PooledClient, prefix: str = "rl:"):
        self.memcache_context_factory = MemcacheContextFactory(memcache_pool)
        self.prefix = prefix

    def make_object_for_context(self, name: str, span: Span) -> "MemcacheRateLimitBackend":
        memcache = self.memcache_context_factory.make_object_for_context(name, span)
        return MemcacheRateLimitBackend(memcache, prefix=self.prefix)


class MemcacheRateLimitBackend(RateLimitBackend):
    """A Memcache backend for rate limiting.

    :param memcache: A memcached connection.
    :param prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, memcache: MonitoredMemcacheConnection, prefix: str = "rl:"):
        self.memcache = memcache
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
        self.memcache.add(key, 0, expire=ttl)
        # `incr` will return None if we experience a delay after the prior
        # `add` call that causes the ttl to expire. We default to `amount` in
        # this case.
        count = self.memcache.incr(key, amount) or amount
        return count <= allowance
