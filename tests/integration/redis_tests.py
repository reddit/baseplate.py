from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    import redis
except ImportError:
    raise unittest.SkipTest("redis-py is not installed")

from baseplate.context.redis import RedisContextFactory
from baseplate.core import Baseplate

from . import TestBaseplateObserver, skip_if_server_unavailable
from .. import mock


skip_if_server_unavailable("redis", 6379)


class RedisIntegrationTests(unittest.TestCase):
    def setUp(self):
        pool = redis.ConnectionPool(host="localhost")
        factory = RedisContextFactory(pool)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("redis", factory)

        self.context = mock.Mock()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple_command(self):
        with self.server_span:
            self.context.redis.ping()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "redis.PING")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)

    def test_error(self):
        with self.server_span:
            with self.assertRaises(redis.ResponseError):
                self.context.redis.execute_command("crazycommand")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)

    def test_pipeline(self):
        with self.server_span:
            with self.context.redis.pipeline("foo") as pipeline:
                pipeline.ping()
                pipeline.execute()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "redis.pipeline_foo")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
