from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import time
import unittest

try:
    from cassandra import InvalidRequest
    from cassandra.cluster import Cluster
    from cassandra.concurrent import execute_concurrent_with_args
    from cassandra.query import dict_factory, named_tuple_factory
except ImportError:
    raise unittest.SkipTest("cassandra-driver is not installed")

from baseplate.context.cassandra import CassandraContextFactory
from baseplate.core import Baseplate

from . import TestBaseplateObserver, get_endpoint_or_skip_container
from .. import mock


cassandra_endpoint = get_endpoint_or_skip_container("cassandra", 9042)


class CassandraTests(unittest.TestCase):
    def setUp(self):
        cluster = Cluster([cassandra_endpoint.address.host], port=cassandra_endpoint.address.port)
        session = cluster.connect("system")
        factory = CassandraContextFactory(session)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("cassandra", factory)

        self.context = mock.Mock()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple_query(self):
        with self.server_span:
            self.context.cassandra.execute("SELECT * FROM system.local;")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
        span_observer.assert_tag("statement", "SELECT * FROM system.local;")

    def test_error_in_query(self):
        with self.server_span:
            with self.assertRaises(InvalidRequest):
                self.context.cassandra.execute("SELECT * FROM does_not_exist;")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)

    def test_async(self):
        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
        span_observer.assert_tag("statement", "SELECT * FROM system.local;")

    def test_properties(self):
        with self.server_span:
            self.assertIsNotNone(self.context.cassandra.cluster)
            self.assertIsNotNone(self.context.cassandra.encoder)
            self.assertEqual(self.context.cassandra.keyspace, "system")

            self.assertEqual(self.context.cassandra.row_factory, named_tuple_factory)
            self.context.cassandra.row_factory = dict_factory
            self.assertEqual(self.context.cassandra.row_factory, dict_factory)

    def test_prepared_statements(self):
        with self.server_span:
            statement = self.context.cassandra.prepare("SELECT * FROM system.local;")
            self.context.cassandra.execute(statement)

    def test_async_callback_fail(self):
        # mock threading.Event so that Event.wait() returns immediately
        event = mock.patch('baseplate.context.cassandra.Event')
        event.start()
        self.addCleanup(event.stop)

        # mock the on_execute_complete callback to be slow
        def on_execute_complete(result, span, event):
            import time
            time.sleep(0.005)
            span.finish()
            event.set()

        on_execute_complete = mock.patch(
            'baseplate.context.cassandra._on_execute_complete', side_effect=on_execute_complete)
        on_execute_complete.start()
        self.addCleanup(on_execute_complete.stop)

        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertFalse(span_observer.on_finish_called)

    def test_async_callback_pass(self):
        # mock the on_execute_complete callback to be slow
        def on_execute_complete(result, span, event):
            import time
            time.sleep(0.005)
            span.finish()
            event.set()

        on_execute_complete = mock.patch(
            'baseplate.context.cassandra._on_execute_complete', side_effect=on_execute_complete)
        on_execute_complete.start()
        self.addCleanup(on_execute_complete.stop)

        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_finish_called)

    def test_async_callback_too_slow(self):
        # mock the on_execute_complete callback to be slow
        def on_execute_complete(result, span, event):
            import time
            time.sleep(0.02)
            span.finish()
            event.set()

        on_execute_complete = mock.patch(
            'baseplate.context.cassandra._on_execute_complete', side_effect=on_execute_complete)
        on_execute_complete.start()
        self.addCleanup(on_execute_complete.stop)

        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertFalse(span_observer.on_finish_called)


class CassandraConcurrentTests(unittest.TestCase):
    def setUp(self):
        cluster = Cluster([cassandra_endpoint.address.host], port=cassandra_endpoint.address.port)
        session = cluster.connect("system")
        factory = CassandraContextFactory(session)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("cassandra", factory)

        self.context = mock.Mock()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_execute_concurrent_with_args(self):
        with self.server_span:
            statement = self.context.cassandra.prepare('SELECT * FROM system.local WHERE "key"=?')
            params = [(_key,) for _key in ['local', 'other']]
            results = execute_concurrent_with_args(self.context.cassandra, statement, params)

        server_span_observer = self.baseplate_observer.get_only_child()
        self.assertEqual(len(server_span_observer.children), 3)
        for span_observer in server_span_observer.children:
            self.assertTrue(span_observer.on_start_called)
            self.assertTrue(span_observer.on_finish_called)
            self.assertIsNone(span_observer.on_finish_exc_info)
            span_observer.assert_tag("statement", 'SELECT * FROM system.local WHERE "key"=?')

        self.assertEqual(len(results), 2)
        self.assertTrue(results[0].success)
        self.assertTrue(results[1].success)
