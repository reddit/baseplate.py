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
    from cassandra.query import dict_factory, named_tuple_factory
except ImportError:
    raise unittest.SkipTest("cassandra-driver is not installed")

from baseplate.context.cassandra import CassandraContextFactory
from baseplate.core import Baseplate
from baseplate.integration.thrift import RequestContext

from . import TestBaseplateObserver, skip_if_server_unavailable
from .. import mock


skip_if_server_unavailable("cassandra", 9042)


def _wait_for_callbacks(span_observer):
    # until it's fixed, paper over a race condition in span reporting to
    # prevent intermittent test failures. see:
    # https://github.com/reddit/baseplate/issues/100
    for _ in range(10):
        if span_observer.on_finish_called:
            return
        logging.info("sleeping to dodge race condition...")
        time.sleep(0.01)
    else:
        logging.warning("gave up sleeping")


class CassandraTests(unittest.TestCase):
    def setUp(self):
        cluster = Cluster(["localhost"])
        session = cluster.connect("system")
        factory = CassandraContextFactory(session)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("cassandra", factory)

        self.context = RequestContext()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple_query(self):
        with self.server_span:
            self.context.cassandra.execute("SELECT * FROM system.local;")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        _wait_for_callbacks(span_observer)
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
        _wait_for_callbacks(span_observer)
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)

    def test_async(self):
        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        _wait_for_callbacks(span_observer)
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
