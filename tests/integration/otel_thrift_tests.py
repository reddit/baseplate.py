import contextlib
import logging
import unittest

from importlib import reload
from unittest import mock

import gevent.monkey
import pytest

from opentelemetry import trace, propagate
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from parameterized import parameterized
from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
from thrift.transport.TTransport import TTransportException

from baseplate import Baseplate
from baseplate import TraceInfo
from baseplate.clients.thrift import ThriftClient
from baseplate.frameworks.thrift import baseplateify_processor
from baseplate.lib import config
from baseplate.lib.propagator_redditb3 import RedditB3Format
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

from . import FakeEdgeContextFactory
from .test_thrift import TestService

logger = logging.getLogger(__name__)


THRIFT_CLIENT_NAME = "example_service"

propagate.set_global_textmap(CompositePropagator([RedditB3Format(), TraceContextTextMapPropagator()]))

@contextlib.contextmanager
def serve_thrift(handler, server_spec, baseplate_observer=None, convert_to_baseplate_error=True):
    # create baseplate root
    baseplate = Baseplate()

    if baseplate_observer:
        baseplate.register(baseplate_observer)

    # set up the server's processor
    logger = mock.Mock(spec=logging.Logger)
    edge_context_factory = FakeEdgeContextFactory()
    processor = server_spec.Processor(handler)
    processor = baseplateify_processor(
        processor, logger, baseplate, edge_context_factory, convert_to_baseplate_error
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
    timeout=None,
):
    app_config = {
        "baseplate.service_name": "fancy test client",
        "example_service.endpoint": str(endpoint),
    }
    if timeout:
        app_config["example_service.timeout"] = timeout
    baseplate = Baseplate(app_config=app_config)

    context = baseplate.make_context_object()

    baseplate.configure_context({THRIFT_CLIENT_NAME: ThriftClient(client_spec.Client)})

    context = baseplate.make_context_object()
    # our client currently requires the legacy headers to be configured.
    trace_info = TraceInfo.from_upstream(
        trace_id=0x4BF92F3577B34DA6A3CE929D0E0E4736,
        parent_id=0x00F067AA0BA902B7,
        span_id=0x00F067AA0BA902B9,
        flags=4567,
        sampled=True,
    )

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
        self.assertGreater(len(finished_spans), 0)
        self.assertSpanHasAttributes(finished_spans[0], {"user.agent": "fancy test client"})

    def test_client_span_attributes(self):
        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(server.endpoint, TestService) as context:
                context.example_service.example()

                net_peer_addr = server.endpoint.address.host
                net_peer_port = server.endpoint.address.port

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 2)

        thrift_client_span = finished_spans[-1]
        self.assertEqual(thrift_client_span.kind, trace.SpanKind.CLIENT)
        self.assertSpanHasAttributes(
            thrift_client_span,
            {
                SpanAttributes.RPC_SYSTEM: "thrift",
                SpanAttributes.RPC_SERVICE: THRIFT_CLIENT_NAME,
                SpanAttributes.RPC_METHOD: "example",
                SpanAttributes.NET_PEER_NAME: net_peer_addr,
                SpanAttributes.NET_PEER_PORT: net_peer_port,
            },
        )

    def test_client_to_server_span_parenthood(self):
        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(server.endpoint, TestService) as context:
                context.example_service.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 2)

        thrift_server_span = finished_spans[0]
        thrift_client_span = finished_spans[1]

        self.assertEqual(thrift_server_span.kind, trace.SpanKind.SERVER)
        self.assertEqual(thrift_client_span.kind, trace.SpanKind.CLIENT)

        # trace id should be the same everywhere
        self.assertEqual(
            thrift_server_span.get_span_context().trace_id,
            thrift_client_span.get_span_context().trace_id,
        )
        self.assertEqual(
            thrift_server_span.get_span_context().trace_id, thrift_server_span.parent.trace_id
        )

        # Ensure client span ID is == server parent span ID
        # but other span IDs are distinct
        self.assertEqual(
            thrift_server_span.parent.span_id, thrift_client_span.get_span_context().span_id
        )
        self.assertNotEqual(
            thrift_server_span.parent.span_id, thrift_server_span.get_span_context().span_id
        )
        self.assertNotEqual(
            thrift_server_span.get_span_context().span_id,
            thrift_client_span.get_span_context().span_id,
        )

    def test_server_span_defaults_to_old_headers(self):
        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                transport = client._oprot.trans
                transport.set_header('X-Trace'.encode('utf-8'), '20d294c28becf34d'.encode('utf-8'))
                transport.set_header('X-Span'.encode('utf-8'), 'a1bf4d567fc497a4'.encode('utf-8'))
                transport.set_header('X-Sampled'.encode('utf-8'), '1'.encode('utf-8'))
                client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        thrift_server_span = finished_spans[0]
        self.assertEqual(thrift_server_span.kind, trace.SpanKind.SERVER)

        self.assertIsNotNone(thrift_server_span.parent)

        # span_id should be the value we manually set above in the mock for random
        self.assertEqual(thrift_server_span.parent.span_id, 0xa1bf4d567fc497a4)
        # this should be the trace_id set originally in baseplate_thrift_client
        self.assertEqual(thrift_server_span.parent.trace_id, 0x000000000000000020d294c28becf34d)

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
        trace_id = 0x4BF92F3577B34DA6A3CE929D0E0E4736
        parent_id = 0x00F067AA0BA902B7
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
        trace_id = 0x4BF92F3577B34DA6A3CE929D0E0E4736
        parent_id = 0x00F067AA0BA902B7
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

    def test_expected_exception_not_marked_as_error(self):
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
        self.assertTrue(finished_spans[0].status.is_ok)

    def test_unexpected_exception_is_marked_as_error(self):
        """If the server returns an unexpected exception, mark a failure."""

        class UnexpectedException(Exception):
            pass

        class Handler(TestService.Iface):
            def example(self, context):
                raise UnexpectedException

        handler = Handler()

        with serve_thrift(handler, TestService, convert_to_baseplate_error=False) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                # although we set `convert_to_baseplate_error` to `False`, this still gets "converted"
                # in the ``TestService`` interface. But the point is it's not just ``Error``
                with self.assertRaises(TApplicationException):
                    client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertFalse(finished_spans[0].status.is_ok)
        self.assertEqual(len(finished_spans[0].events), 1)
        self.assertEqual(
            finished_spans[0].events[0].attributes["exception.type"], "UnexpectedException"
        )

    def test_unexpected_exception_is_marked_as_error_convert(self):
        """If the server returns an unexpected exception, mark a failure."""

        class UnexpectedException(Exception):
            pass

        class Handler(TestService.Iface):
            def example(self, context):
                raise UnexpectedException

        handler = Handler()

        with serve_thrift(handler, TestService, convert_to_baseplate_error=True) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                with self.assertRaises(Error):
                    client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertFalse(finished_spans[0].status.is_ok)
        self.assertEqual(len(finished_spans[0].events), 1)
        self.assertEqual(finished_spans[0].events[0].attributes["exception.type"], "Error")

    @parameterized.expand(
        [
            (
                TApplicationException(TApplicationException.UNKNOWN_METHOD, "unknown method"),
                True,
                pytest.raises(TApplicationException),
                "TApplicationException",
                trace.status.StatusCode.ERROR,
            ),
            (
                TProtocolException(message="Required field is unset!"),
                True,
                pytest.raises(TApplicationException),  # because of TestService
                "TProtocolException",
                trace.status.StatusCode.ERROR,
            ),
            (
                TTransportException(message="Something is wrong with the transport"),
                True,
                pytest.raises(TTransportException),
                "TTransportException",
                trace.status.StatusCode.ERROR,
            ),
            (
                Error(ErrorCode.NOT_FOUND, "404 not found"),
                True,
                pytest.raises(Error),
                "Error",
                trace.status.StatusCode.OK,
            ),
            (
                Error(ErrorCode.SERVICE_UNAVAILABLE, "503 unavailable"),
                True,
                pytest.raises(Error),
                "Error",
                trace.status.StatusCode.ERROR,
            ),
            (
                TException(message="Some other generic thrift exception"),
                True,
                pytest.raises(TException),
                "TException",
                trace.status.StatusCode.OK,
            ),
            (
                Exception("Some very generic exception"),
                False,
                pytest.raises(Exception),
                "Exception",
                trace.status.StatusCode.ERROR,
            ),
            (
                Exception("Some very generic exception"),
                True,
                pytest.raises(Exception),
                "Error",
                trace.status.StatusCode.ERROR,
            ),
        ],
    )
    def test_exception_handling_trace_status(
        self, exc, convert, expectation, otel_exception, otel_status
    ):
        """
        This test ensures that we do record (i.e. add exceptions as events on the otel spans) every
        exception that is happening, but only set the error status when we want it to (i.e. only on
        some exceptions, or when the status on baseplate Error is a 5xx).
        """
        logger.debug(f"{exc=}, {convert=}, {expectation=}, {otel_exception=}, {otel_status=}")

        class Handler(TestService.Iface):
            def example(self, context):
                if exc is None:
                    pass
                else:
                    raise exc

        handler = Handler()

        with serve_thrift(handler, TestService, convert_to_baseplate_error=convert) as server:
            with raw_thrift_client(server.endpoint, TestService) as client:
                with expectation:
                    client.example()

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(finished_spans[0].status.status_code, otel_status)
        self.assertEqual(len(finished_spans[0].events), 1)
        self.assertEqual(finished_spans[0].events[0].attributes["exception.type"], otel_exception)


class ThriftClientSpanTests(GeventPatchedTestCase, TestBase):
    def test_client_span_starts_and_stops(self):
        """The client span should start/stop appropriately."""

        class Handler(TestService.Iface):
            def example(self, context):
                return True

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(server.endpoint, TestService) as context:
                with trace.get_tracer("TestWrapper").start_as_current_span("test"):
                    context.example_service.example()

        finished_spans = self.get_finished_spans()
        self.assertGreaterEqual(len(finished_spans), 2)
        client_span = finished_spans[1]
        server_span = finished_spans[0]
        self.assertGreater(client_span.end_time, client_span.start_time)
        self.assertEqual(client_span.context.span_id, server_span.parent.span_id)

    def test_expected_exception_not_passed_to_client_span_finish(self):
        """If the server returns an expected exception, don't count it as failure."""

        class Handler(TestService.Iface):
            def example(self, context):
                raise TestService.ExpectedException()

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(server.endpoint, TestService) as context:
                with self.assertRaises(TestService.ExpectedException):
                    with trace.get_tracer("TestWrapper").start_as_current_span("test"):
                        context.example_service.example()

        finished_spans = self.get_finished_spans()
        self.assertGreaterEqual(len(finished_spans), 2)
        client_span = finished_spans[1]
        self.assertTrue(client_span.status.is_ok)

    def test_unexpected_exception_passed_to_client_span_finish(self):
        """If the server returns an unexpected exception, mark a failure."""

        class UnexpectedException(Exception):
            pass

        class Handler(TestService.Iface):
            def example(self, context):
                raise UnexpectedException

        handler = Handler()

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(server.endpoint, TestService) as context:
                with self.assertRaises(Error):
                    with trace.get_tracer("TestWrapper").start_as_current_span("test"):
                        context.example_service.example()

        finished_spans = self.get_finished_spans()
        self.assertGreaterEqual(len(finished_spans), 2)
        client_span = finished_spans[1]
        self.assertGreater(len(client_span.events), 0)
        self.assertEqual(client_span.events[0].name, "exception")


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

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, timeout="1 second"
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

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, timeout="1000 seconds"
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

        with serve_thrift(handler, TestService) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, timeout="1000 seconds"
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

        timeout_observer = TimeoutBaseplateObserver.from_config(
            {"server_timeout.default": "1 hour"}
        )
        with serve_thrift(handler, TestService, baseplate_observer=timeout_observer) as server:
            with baseplate_thrift_client(
                server.endpoint, TestService, timeout="1000 seconds"
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

        with serve_thrift(handler, BaseplateService) as server:
            with baseplate_thrift_client(server.endpoint, BaseplateServiceV2) as context:
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

        with serve_thrift(handler, BaseplateServiceV2) as server:
            with baseplate_thrift_client(server.endpoint, BaseplateServiceV2) as context:
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

        with serve_thrift(handler, TestService) as server:
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
