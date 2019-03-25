from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.core import Baseplate
from baseplate.ratelimit import RateLimiterContextFactory, RateLimitExceededException
try:
    from pymemcache.client.base import PooledClient
    from baseplate.ratelimit.backends.memcache import MemcacheRateLimitBackendContextFactory
except ImportError:
    raise unittest.SkipTest("pymemcache is not installed")
try:
    from redis import ConnectionPool
    from baseplate.ratelimit.backends.redis import RedisRateLimitBackendContextFactory
except ImportError:
    raise unittest.SkipTest("redis-py is not installed")

from . import TestBaseplateObserver, skip_if_server_unavailable
from .. import mock


skip_if_server_unavailable("redis", 6379)
skip_if_server_unavailable("memcached", 11211)


# TODO: Mock _get_current_bucket to avoid flakey tests
# TODO: Test bucket resets


class RedisRateLimitBackendTests(unittest.TestCase):
    def setUp(self):
        pool = ConnectionPool(host="localhost")
        backend_factory = RedisRateLimitBackendContextFactory(pool)
        self.allowance = 10
        interval = 60
        ratelimiter_factory = RateLimiterContextFactory(
            backend_factory, allowance, interval)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("ratelimiter", ratelimiter_factory)

        self.context = mock.Mock()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_redis_ratelimiter(self):
        with self.server_span:
            self.context.ratelimiter.consume('user_foo', amount=self.allowance)
            with self.assertRaises(RateLimitExceededException):
                self.context.ratelimiter.consume('user_foo')


class MemcacheRateLimitBackendTests(unittest.TestCase):
    def setUp(self):
        pool = PooledClient(server=("localhost", 11211))
        backend_factory = MemcacheRateLimitBackendContextFactory(pool)
        self.allowance = 10
        interval = 60
        ratelimiter_factory = RateLimiterContextFactory(
            backend_factory, allowance, interval)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("ratelimiter", ratelimiter_factory)

        self.context = mock.Mock()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_memcache_ratelimiter(self):
        with self.server_span:
            self.context.ratelimiter.consume('user_foo', amount=self.allowance)
            with self.assertRaises(RateLimitExceededException):
                self.context.ratelimiter.consume('user_foo')
