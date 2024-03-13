import time
import unittest

from unittest import mock

from opentelemetry import trace
from opentelemetry.sdk.trace import Span
from opentelemetry.test.test_base import TestBase

try:
    from cassandra import InvalidRequest, ConsistencyLevel
    from cassandra.cluster import ExecutionProfile
    from cassandra.concurrent import execute_concurrent_with_args
    from cassandra.query import dict_factory, named_tuple_factory
except ImportError:
    raise unittest.SkipTest("cassandra-driver is not installed")

from baseplate.clients.cassandra import CassandraClient
from baseplate import Baseplate

from . import TestBaseplateObserver, get_endpoint_or_skip_container


cassandra_endpoint = get_endpoint_or_skip_container("cassandra", 9042)

tracer = trace.get_tracer(__name__)


class CassandraTests(TestBase):
    def setUp(self):
        super().setUp()
        self.baseplate_observer = TestBaseplateObserver()

        profiles = {"foo": ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM)}

        baseplate = Baseplate(
            {
                "cassandra.contact_points": cassandra_endpoint.address.host,
                "cassandra_no_prof.contact_points": cassandra_endpoint.address.host,
            }
        )
        baseplate.register(self.baseplate_observer)
        baseplate.configure_context(
            {
                "cassandra_no_prof": CassandraClient(keyspace="system"),
                "cassandra": CassandraClient(keyspace="system", execution_profiles=profiles),
            }
        )

        self.context = baseplate.make_context_object()
        self.server_span = tracer.start_span("test")
        self.context.span = self.server_span

    def test_simple_query(self):
        with self.server_span:
            self.context.cassandra.execute("SELECT * FROM system.local;")

        finished_spans = self.get_finished_spans()
        first_span: Span = finished_spans[0]
        self.assertGreater(len(finished_spans), 0)
        # TODO test span status
        self.assertSpanHasAttributes(
            finished_spans[0], {"statement": "SELECT * FROM system.local;"}
        )
        self.assertEqual(
            first_span.get_span_context().trace_id, self.server_span.get_span_context().trace_id
        )
        self.assertEqual(first_span.parent.span_id, self.server_span.get_span_context().span_id)
        self.assertTrue(first_span.status.is_ok)

    def test_error_in_query(self):
        with self.server_span:
            with self.assertRaises(InvalidRequest):
                self.context.cassandra.execute("SELECT * FROM does_not_exist;")

        finished_spans = self.get_finished_spans()
        first_span: Span = finished_spans[0]
        self.assertGreater(len(finished_spans), 0)
        self.assertSpanHasAttributes(
            finished_spans[0], {"statement": "SELECT * FROM does_not_exist;"}
        )
        self.assertEqual(
            first_span.get_span_context().trace_id, self.server_span.get_span_context().trace_id
        )
        self.assertEqual(first_span.parent.span_id, self.server_span.get_span_context().span_id)
        self.assertFalse(first_span.status.is_ok)

    def test_async(self):
        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        finished_spans = self.get_finished_spans()

        self.assertGreater(len(finished_spans), 0)
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertSpanHasAttributes(
            finished_spans[0], {"statement": "SELECT * FROM system.local;"}
        )

    def test_properties(self):
        with self.server_span:
            self.assertIsNotNone(self.context.cassandra_no_prof.cluster)
            self.assertIsNotNone(self.context.cassandra_no_prof.encoder)
            self.assertEqual(self.context.cassandra_no_prof.keyspace, "system")

            self.assertEqual(self.context.cassandra_no_prof.row_factory, named_tuple_factory)
            self.context.cassandra_no_prof.row_factory = dict_factory
            self.assertEqual(self.context.cassandra_no_prof.row_factory, dict_factory)

    def test_prepared_statements(self):
        with self.server_span:
            statement = self.context.cassandra.prepare("SELECT * FROM system.local;")
            self.context.cassandra.execute(statement)

    def test_async_callback_fail(self):
        # mock threading.Event so that Event.wait() returns immediately
        event = mock.patch("baseplate.clients.cassandra.Event")
        event.start()
        self.addCleanup(event.stop)

        # mock the on_execute_complete callback to be slow
        def on_execute_complete(result, args, event):
            time.sleep(0.005)
            args.span.end()
            event.set()

        on_execute_complete = mock.patch(
            "baseplate.clients.cassandra._on_execute_complete", side_effect=on_execute_complete
        )
        on_execute_complete.start()
        self.addCleanup(on_execute_complete.stop)

        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 0)

    def test_async_callback_pass(self):
        # mock the on_execute_complete callback to be slow
        def on_execute_complete(result, span, event):
            time.sleep(0.005)
            span.finish()
            event.set()

        on_execute_complete = mock.patch(
            "baseplate.clients.cassandra._on_execute_complete", side_effect=on_execute_complete
        )
        on_execute_complete.start()
        self.addCleanup(on_execute_complete.stop)

        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)

    def test_async_callback_too_slow(self):
        # mock the on_execute_complete callback to be slow
        def on_execute_complete(result, args, event):
            time.sleep(0.02)
            args.span.end()
            event.set()

        on_execute_complete = mock.patch(
            "baseplate.clients.cassandra._on_execute_complete", side_effect=on_execute_complete
        )
        on_execute_complete.start()
        self.addCleanup(on_execute_complete.stop)

        with self.server_span:
            future = self.context.cassandra.execute_async("SELECT * FROM system.local;")
            future.result()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 0)

    def test_cluster_name_from_metadata(self):
        with self.server_span:
            name = self.context.cassandra.prometheus_cluster_name
            self.assertEqual(name, "Test Cluster")


class CassandraConcurrentTests(TestBase):
    def setUp(self):
        super().setUp()
        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate({"cassandra.contact_points": cassandra_endpoint.address.host})
        baseplate.register(self.baseplate_observer)
        baseplate.configure_context({"cassandra": CassandraClient(keyspace="system")})

        self.context = baseplate.make_context_object()
        self.server_span = tracer.start_span("test")
        self.context.span = self.server_span

    def test_execute_concurrent_with_args(self):
        with self.server_span:
            statement = self.context.cassandra.prepare('SELECT * FROM system.local WHERE "key"=?')
            params = [(_key,) for _key in ["local", "other"]]
            results = execute_concurrent_with_args(self.context.cassandra, statement, params)

        finished_spans = self.get_finished_spans()
        first_span: Span = finished_spans[0]
        self.assertEqual(len(finished_spans), 3)
        self.assertSpanHasAttributes(
            finished_spans[0], {"statement": 'SELECT * FROM system.local WHERE "key"=?'}
        )
        self.assertEqual(
            first_span.get_span_context().trace_id, self.server_span.get_span_context().trace_id
        )
        self.assertEqual(first_span.parent.span_id, self.server_span.get_span_context().span_id)

        self.assertEqual(len(results), 2)
        self.assertTrue(results[0].success)
        self.assertTrue(results[1].success)
