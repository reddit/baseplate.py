import unittest

from time import sleep
from uuid import uuid4

from baseplate import Baseplate
from baseplate.lib.ratelimit import RateLimiterContextFactory
from baseplate.lib.ratelimit import RateLimitExceededException

try:
    from pymemcache.client.base import PooledClient
    from baseplate.lib.ratelimit.backends.memcache import MemcacheRateLimitBackendContextFactory
except ImportError:
    raise unittest.SkipTest("pymemcache is not installed")
try:
    from redis import ConnectionPool
    from baseplate.lib.ratelimit.backends.redis import RedisRateLimitBackendContextFactory
except ImportError:
    raise unittest.SkipTest("redis-py is not installed")

from . import TestBaseplateObserver, get_endpoint_or_skip_container


redis_endpoint = get_endpoint_or_skip_container("redis", 6379)
memcached_endpoint = get_endpoint_or_skip_container("memcached", 11211)


class RateLimiterBackendTests:
    def setUp(self):
        self.allowance = 10
        self.interval = 1
        ratelimiter_factory = RateLimiterContextFactory(
            self.backend_factory, self.allowance, self.interval
        )

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("ratelimiter", ratelimiter_factory)

        self.context = baseplate.make_context_object()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_ratelimiter_consume(self):
        user_id = str(uuid4())
        with self.server_span:
            self.context.ratelimiter.consume(user_id, amount=self.allowance)

    def test_ratelimiter_exceeded(self):
        user_id = str(uuid4())
        with self.server_span:
            with self.assertRaises(RateLimitExceededException):
                self.context.ratelimiter.consume(user_id, amount=self.allowance + 1)

    def test_ratelimiter_resets(self):
        user_id = str(uuid4())
        with self.server_span:
            self.context.ratelimiter.consume(user_id, amount=self.allowance)
            sleep(self.interval)
            self.context.ratelimiter.consume(user_id, amount=self.allowance)


class RedisRateLimitBackendTests(RateLimiterBackendTests, unittest.TestCase):
    def setUp(self):
        pool = ConnectionPool(host=redis_endpoint.address.host, port=redis_endpoint.address.port)
        self.backend_factory = RedisRateLimitBackendContextFactory(pool)
        super().setUp()


class MemcacheRateLimitBackendTests(RateLimiterBackendTests, unittest.TestCase):
    def setUp(self):
        pool = PooledClient(server=memcached_endpoint.address)
        self.backend_factory = MemcacheRateLimitBackendContextFactory(pool)
        super().setUp()
