from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    from pymemcache.client.base import PooledClient
    from pymemcache.exceptions import MemcacheClientError
except ImportError:
    raise unittest.SkipTest("pymemcache is not installed")

from baseplate.context.memcache import MemcacheContextFactory
from baseplate.core import Baseplate

from . import TestBaseplateObserver, skip_if_server_unavailable
from .. import mock


skip_if_server_unavailable("memcached", 11211)


class MemcacheIntegrationTests(unittest.TestCase):
    def setUp(self):
        pool = PooledClient(server=("localhost", 11211))
        factory = MemcacheContextFactory(pool)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("memcache", factory)

        self.context = mock.Mock()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple(self):
        with self.server_span:
            self.context.memcache.get("whatever")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "memcache.get")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)

    def test_error(self):
        with self.server_span:
            with self.assertRaises(MemcacheClientError):
                self.context.memcache.cas("key", b"value", b"whatever")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "memcache.cas")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)
