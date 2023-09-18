import contextlib
import logging
import unittest

from importlib import reload
from unittest import mock

import gevent.monkey
import pytest

from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate import ServerSpanObserver
from baseplate import SpanObserver
from baseplate import TraceInfo
from baseplate.clients.thrift import ThriftClient
from baseplate.frameworks.thrift import baseplateify_processor
from baseplate.lib import config
from baseplate.lib.thrift_pool import ThriftConnectionPool
from baseplate.observers.timeout import ServerTimeout
from baseplate.observers.timeout import TimeoutBaseplateObserver
from baseplate.server import make_listener
from baseplate.server.thrift import make_server
from baseplate.thrift import BaseplateService
from baseplate.thrift import BaseplateServiceV2
from baseplate.thrift.ttypes import Error
from baseplate.thrift.ttypes import ErrorCode
from baseplate.thrift.ttypes import IsHealthyProbe
from baseplate.thrift.ttypes import IsHealthyRequest

from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from . import FakeEdgeContextFactory
from .test_thrift import TestService


@contextlib.contextmanager
def serve_thrift(handler, server_spec, server_span_observer=None, baseplate_observer=None):
    # create baseplate root
    baseplate = Baseplate()

    if server_span_observer:

        class TestBaseplateObserver(BaseplateObserver):
            def on_server_span_created(self, context, server_span):
                server_span.register(server_span_observer)

        baseplate.register(TestBaseplateObserver())

    if baseplate_observer:
        baseplate.register(baseplate_observer)

    # set up the server's processor
    logger = mock.Mock(spec=logging.Logger)
    edge_context_factory = FakeEdgeContextFactory()
    processor = server_spec.Processor(handler)
    processor = baseplateify_processor(
        processor, logger, baseplate, edge_context_factory, convert_to_baseplate_error=True
    )

    # bind a server socket on an available port
    server_bind_endpoint = config.Endpoint("127.0.0.1:0")
    listener = make_listener(server_bind_endpoint)
    server = make_server({"stop_timeout": "1 millisecond"}, listener, processor)

    # figure out what port the server ended up on
    server_address = listener.getsockname()
    server.endpoint = config.Endpoint(f"{server_address[0]}:{server_address[1]}")

    # run the server until our caller is done with it
    server_greenlet = gevent.spawn(server.serve_forever)
    try:
        yield server
    finally:
        server_greenlet.kill()


@contextlib.contextmanager
def raw_thrift_client(endpoint, client_spec):
    pool = ThriftConnectionPool(endpoint)
    with pool.connection() as client_protocol:
        yield client_spec.Client(client_protocol)


@contextlib.contextmanager
def baseplate_thrift_client(
    endpoint,
    client_spec,
    client_span_observer=None,
    timeout=None,
):
    app_config = {
        "baseplate.service_name": "fancy test client",
        "example_service.endpoint": str(endpoint),
    }
    if timeout:
        app_config["example_service.timeout"] = timeout
    baseplate = Baseplate(app_config=app_config)

    if client_span_observer:

        class TestServerSpanObserver(ServerSpanObserver):
            def on_child_span_created(self, span):
                span.register(client_span_observer)

        observer = TestServerSpanObserver()

        class TestBaseplateObserver(BaseplateObserver):
            def on_server_span_created(self, context, span):
                span.register(observer)

        baseplate.register(TestBaseplateObserver())

    context = baseplate.make_context_object()
    trace_info = TraceInfo.from_upstream(
        trace_id="1234", parent_id="2345", span_id="3456", flags=4567, sampled=True
    )

    baseplate.configure_context({"example_service": ThriftClient(client_spec.Client)})

    baseplate.make_server_span(context, "example_service.example", trace_info)

    context.raw_edge_context = FakeEdgeContextFactory.RAW_BYTES

    yield context


class GeventPatchedTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        gevent.monkey.patch_socket()

    def tearDown(self):
        super().tearDown()
        import socket

        reload(socket)
        gevent.monkey.saved.clear()


class ThriftTraceHeaderTests(GeventPatchedTestCase, TestBase):
    def test_user_agent(self):
        """We should accept user-agent headers and apply them to the server span tags."""

        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(server.endpoint, TestService) as context:
                context.example_service.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertSpanHasAttributes(finished_spans[0], {'user.agent': 'fancy test client'})


    def test_no_headers(self):
        """We should accept requests without headers and generate a trace."""

        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                client_result = client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertIsNotNone(finished_spans[0].context.span_id)
        self.assertIsNone(finished_spans[0].parent)
        self.assertTrue(client_result)

    def test_header_propagation(self):
        """If the client sends headers, we should set the trace up accordingly."""
        trace_id = 0x4bf92f3577b34da6a3ce929d0e0e4736
        parent_id = 0x00f067aa0ba902b7
        sampled = 0x01
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                transport = client._oprot.trans
                transport.set_header(b"traceparent", traceparent.encode())
                client_result = client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(finished_spans[0].context.trace_id, trace_id)
        self.assertEqual(finished_spans[0].context.trace_flags, sampled)
        self.assertEqual(finished_spans[0].parent.span_id, parent_id)
        self.assertIsNotNone(finished_spans[0].context.span_id)
        self.assertTrue(client_result)

    def test_not_sampled_flag(self):
        """Test that we do not sample the span if we receive a false sampled flag."""
        trace_id = 0x4bf92f3577b34da6a3ce929d0e0e4736
        parent_id = 0x00f067aa0ba902b7
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"

        class Handler(TestService.Iface):
            def __init__(self):
                self.server_span = None

            def example(self, context):
                self.server_span = context.span
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                transport = client._oprot.trans
                transport.set_header(b"traceparent", traceparent.encode())
                client_result = client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 0)
        self.assertTrue(client_result)

    def test_sampled_flag(self):
        """Test that we do not sample the span if we receive a false sampled flag."""
        trace_id = 0x4bf92f3577b34da6a3ce929d0e0e4736
        parent_id = 0x00f067aa0ba902b7
        sampled = 0x01
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

        class Handler(TestService.Iface):
            def __init__(self):
                self.server_span = None

            def example(self, context):
                self.server_span = context.span
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                transport = client._oprot.trans
                transport.set_header(b"traceparent", traceparent.encode())
                client_result = client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(finished_spans[0].context.trace_id, trace_id)
        self.assertEqual(finished_spans[0].context.trace_flags, sampled)
        self.assertEqual(finished_spans[0].parent.span_id, parent_id)
        self.assertIsNotNone(finished_spans[0].context.span_id)
        self.assertTrue(client_result)

    def test_budget_header(self):
        """Test that the budget header is set in the headers if the client sets it."""
        budget = "1234"

        class Handler(TestService.Iface):
            def example(self, context):
                self.context = context
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                transport = client._oprot.trans
                transport.set_header(b"Deadline-Budget", budget.encode())
                client_result = client.example()

        self.assertEqual(handler.context.headers.get(b"Deadline-Budget").decode(), budget)
        self.assertTrue(client_result)


class ThriftEdgeRequestHeaderTests(GeventPatchedTestCase):
    def _test(self, header_name=None):
        class Handler(TestService.Iface):
            def __init__(self):
                self.edge_context = None

            def example(self, context):
                self.edge_context = context.edge_context
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                if header_name:
                    transport = client._oprot.trans
                    transport.set_header(header_name, FakeEdgeContextFactory.RAW_BYTES)
                client_result = client.example()

        assert client_result is True
        return handler.edge_context

    def test_edge_request_context(self):
        """If the client sends an edge-request header we should parse it."""
        edge_context = self._test(b"Edge-Request")
        assert edge_context == FakeEdgeContextFactory.DECODED_CONTEXT

    def test_edge_request_context_case_insensitive(self):
        edge_context = self._test(b"edge-request")
        assert edge_context == FakeEdgeContextFactory.DECODED_CONTEXT

    def test_no_edge_context(self):
        edge_context = self._test()
        assert edge_context is None


class ThriftServerSpanTests(GeventPatchedTestCase, TestBase):
    def test_server_span_starts_and_stops(self):
        """The server span should start/stop appropriately."""

        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertLess(finished_spans[0].start_time, finished_spans[0].end_time)

    def test_expected_exception_not_passed_to_server_span_finish(self):
        """If the server returns an expected exception, don't count it as failure."""

        class Handler(TestService.Iface):
            def example(self, context):
                raise TestService.ExpectedException()

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                with self.assertRaises(TestService.ExpectedException):
                    client.example()


        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertFalse(finished_spans[0].status.is_ok)
        self.assertEqual(len(finished_spans[0].events), 0)

    def test_unexpected_exception_passed_to_server_span_finish(self):
        """If the server returns an unexpected exception, mark a failure."""

        class UnexpectedException(Exception):
            pass

        class Handler(TestService.Iface):
            def example(self, context):
                raise UnexpectedException

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                with self.assertRaises(Error):
                    client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertFalse(finished_spans[0].status.is_ok)
        self.assertEqual(len(finished_spans[0].events), 1)
        self.assertEqual(finished_spans[0].events[0].attributes['exception.type'], 'UnexpectedException')


class ThriftClientSpanTests(GeventPatchedTestCase):
    def test_client_span_starts_and_stops(self):
        """The client span should start/stop appropriately."""

        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        client_span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, client_span_observer
            ) as context:
                context.example_service.example()

        client_span_observer.on_start.assert_called_once_with()
        client_span_observer.on_finish.assert_called_once_with(None)

    def test_expected_exception_not_passed_to_client_span_finish(self):
        """If the server returns an expected exception, don't count it as failure."""

        class Handler(TestService.Iface):
            def example(self, context):
                raise TestService.ExpectedException()

        handler = Handler()

        client_span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, client_span_observer
            ) as context:
                with self.assertRaises(TestService.ExpectedException):
                    context.example_service.example()

        client_span_observer.on_start.assert_called_once_with()
        client_span_observer.on_finish.assert_called_once_with(None)

    def test_unexpected_exception_passed_to_client_span_finish(self):
        """If the server returns an unexpected exception, mark a failure."""

        class UnexpectedException(Exception):
            pass

        class Handler(TestService.Iface):
            def example(self, context):
                raise UnexpectedException

        handler = Handler()

        client_span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, client_span_observer
            ) as context:
                with self.assertRaises(Error):
                    context.example_service.example()

        client_span_observer.on_start.assert_called_once_with()
        self.assertEqual(client_span_observer.on_finish.call_count, 1)
        _, captured_exc, _ = client_span_observer.on_finish.call_args[0][0]
        self.assertIsInstance(captured_exc, Error)


class ThriftEndToEndTests(GeventPatchedTestCase, TestBase):
    def test_end_to_end(self):
        class Handler(TestService.Iface):
            def __init__(self):
                self.edge_context = None

            def example(self, context):
                self.edge_context = context.edge_context
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(server.endpoint, TestService) as context:
                context.example_service.example()

        assert handler.edge_context == FakeEdgeContextFactory.DECODED_CONTEXT

    def test_budget_header_pool_timeout(self):
        """Test that the budget header is set in the headers with the pool timeout."""
        retry_timeout_seconds = 100.0

        class Handler(TestService.Iface):
            def example(self, context):
                self.context = context
                return True

        handler = Handler()

        span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, span_observer, timeout="1 second"
            ) as context:
                with context.example_service.retrying(
                    attempts=3, budget=retry_timeout_seconds
                ) as svc:
                    svc.example()

        # this should be 1 second (1000 ms) for the pool timeout
        self.assertAlmostEqual(handler.context.deadline_budget, 1.0)

    def test_budget_header_retry_timeout(self):
        """Test that the budget header is set in the headers with the retry timeout."""
        retry_timeout_seconds = 0.1

        class Handler(TestService.Iface):
            def example(self, context):
                self.context = context
                return True

        handler = Handler()

        span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, span_observer, timeout="1000 seconds"
            ) as context:
                with context.example_service.retrying(
                    attempts=3, budget=retry_timeout_seconds
                ) as svc:
                    svc.example()

        self.assertAlmostEqual(handler.context.deadline_budget, retry_timeout_seconds)

    def test_budget_header_budget_and_backoff(self):
        """Test that the budget header is set in the headers with the backoff timeout."""
        retry_timeout_seconds = 1.0
        backoff = 1.0

        class Handler(TestService.Iface):
            def example(self, context):
                self.context = context
                return True

        handler = Handler()

        span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, span_observer, timeout="1000 seconds"
            ) as context:
                with context.example_service.retrying(
                    attempts=3, budget=retry_timeout_seconds, backoff=backoff
                ) as svc:
                    svc.example()

        self.assertAlmostEqual(handler.context.deadline_budget, retry_timeout_seconds)

    def test_budget_timeout_from_client(self):
        """Test that the server times out when passed a short timeout from the client."""
        retry_timeout_seconds = 0.25

        class Handler(TestService.Iface):
            def example(self, context):
                self.context = context
                with pytest.raises(ServerTimeout):
                    gevent.sleep(1)
                return True

        handler = Handler()

        span_observer = mock.Mock(spec=SpanObserver)
        timeout_observer = TimeoutBaseplateObserver.from_config(
            {"server_timeout.default": "1 hour"}
        )
        with serve_thrift(handler, TestService, baseplate_observer=timeout_observer) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, span_observer, timeout="1000 seconds"
            ) as context:
                with context.example_service.retrying(
                    attempts=3, budget=retry_timeout_seconds
                ) as svc:
                    svc.example()

        self.assertAlmostEqual(handler.context.deadline_budget, retry_timeout_seconds)


class ThriftHealthcheck(GeventPatchedTestCase):
    def test_v2_client_v1_server(self):
        class Handler(BaseplateService.Iface):
            def is_healthy(self, context):
                return True

        handler = Handler()

        span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, BaseplateService) as server:
            with baseplate_thrift_client(
                server.endpoint, BaseplateServiceV2, span_observer
            ) as context:
                healthy = context.example_service.is_healthy(
                    request=IsHealthyRequest(probe=IsHealthyProbe.READINESS),
                )
                self.assertTrue(healthy)

    def test_v2_client_v2_server(self):
        class Handler(BaseplateServiceV2.Iface):
            def __init__(self):
                self.probe = None

            def is_healthy(self, context, req=None):
                self.probe = req.probe
                return True

        handler = Handler()

        span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, BaseplateServiceV2) as server:
            with baseplate_thrift_client(
                server.endpoint, BaseplateServiceV2, span_observer
            ) as context:
                healthy = context.example_service.is_healthy(
                    request=IsHealthyRequest(probe=IsHealthyProbe.LIVENESS),
                )
                self.assertTrue(healthy)
                self.assertEqual(handler.probe, IsHealthyProbe.LIVENESS)


class ThriftErrorReplacementTests(GeventPatchedTestCase):
    def test_server_replaces_unhandled_errors(self):
        """The server span should start/stop appropriately."""

        class Handler(TestService.Iface):
            def example(self, context):
                raise Exception("foo")

        handler = Handler()

        server_span_observer = mock.Mock(spec=ServerSpanObserver)
        with serve_thrift(handler, TestService, server_span_observer) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                with self.assertRaises(Error) as exc_info:
                    client.example()
        self.assertEqual(exc_info.exception.code, ErrorCode.INTERNAL_SERVER_ERROR)


class ThriftPrometheusMetricsTests(GeventPatchedTestCase):
    def reset_metrics(self, metrics):
        if not metrics:
            return

        try:
            metrics.get_active_requests_metric().clear()
            metrics.get_latency_seconds_metric().clear()
            metrics.get_requests_total_metric().clear()
        except Exception:
            pass

    def assert_correct_metric(
        self, metric, want_count, want_sample, want_name, want_labels, want_value
    ):
        m = metric.collect()
        self.assertEqual(len(m), 1)
        self.assertEqual(len(m[0].samples), want_count)
        sample = m[0].samples[want_sample]
        got_name = sample[0]
        self.assertEqual(got_name, want_name)
        got_labels = sample[1]
        self.assertEqual(got_labels, want_labels)
        got_value = sample[2]
        self.assertEqual(got_value, want_value)
