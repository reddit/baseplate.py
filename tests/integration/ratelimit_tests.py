from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from time import sleep
import unittest
from uuid import uuid4

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

from . import TestBaseplateObserver, get_endpoint_or_skip_container
from .. import mock


redis_endpoint = get_endpoint_or_skip_container("redis", 6379)
memcached_endpoint = get_endpoint_or_skip_container("memcached", 11211)


class RateLimiterBackendTests(object):
    def setUp(self):
        self.allowance = 10
        self.interval = 1
        ratelimiter_factory = RateLimiterContextFactory(
            self.backend_factory, self.allowance, self.interval)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("ratelimiter", ratelimiter_factory)

        self.context = mock.Mock()
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
        pool = ConnectionPool(host=redis_endpoint.address.host,
                              port=redis_endpoint.address.port)
        self.backend_factory = RedisRateLimitBackendContextFactory(pool)
        super(RedisRateLimitBackendTests, self).setUp()


class MemcacheRateLimitBackendTests(RateLimiterBackendTests, unittest.TestCase):
    def setUp(self):
        pool = PooledClient(server=memcached_endpoint.address)
        self.backend_factory = MemcacheRateLimitBackendContextFactory(pool)
        super(MemcacheRateLimitBackendTests, self).setUp()
