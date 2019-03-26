from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import contextlib
import time
import unittest

try:
    import redis
except ImportError:
    raise unittest.SkipTest("redis-py is not installed")

from baseplate.context.redis import RedisContextFactory
from baseplate.core import Baseplate

from . import TestBaseplateObserver, skip_if_server_unavailable
from .. import mock

from baseplate.context.redis import MessageQueue
from baseplate.message_queue import TimedOutError
from baseplate.integration.thrift import RequestContext


skip_if_server_unavailable("redis", 6379)


class RedisIntegrationTests(unittest.TestCase):
    def setUp(self):
        pool = redis.ConnectionPool(host="localhost")
        factory = RedisContextFactory(pool)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("redis", factory)

        self.context = RequestContext()
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


class RedisMessageQueueTests(unittest.TestCase):
    qname = "redisTestQueue"

    def setUp(self):
        self.pool = redis.ConnectionPool(host="localhost")

    def test_put_get(self):
        message_queue = MessageQueue(self.qname, self.pool)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_get_zero_timeout(self):
        message_queue = MessageQueue(self.qname, self.pool)

        message_queue.put(b"y")
        message = message_queue.get(timeout=0)
        self.assertEqual(message, b"y")

        with self.assertRaises(TimedOutError):
            message_queue.get(timeout=0)

    def test_put_zero_timeout(self):
        message_queue = MessageQueue(self.qname, self.pool)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x", timeout=0)
            message = mq.get()
            self.assertEqual(message, b"x")

    def tearDown(self):
        redis.Redis(connection_pool=self.pool).delete(self.qname)
