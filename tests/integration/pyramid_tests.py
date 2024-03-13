import base64
import unittest

from unittest import mock

from opentelemetry import propagate
from opentelemetry import trace
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pyramid.response import Response

from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate.lib.propagator_redditb3 import RedditB3Format
from baseplate.lib.propagator_redditb3_thrift import RedditB3ThriftFormat

from . import FakeEdgeContextFactory


try:
    import webtest

    from baseplate.frameworks.pyramid import BaseplateConfigurator
    from baseplate.frameworks.pyramid import ServerSpanInitialized
    from baseplate.frameworks.pyramid import StaticTrustHandler
    from pyramid.config import Configurator
    from pyramid.httpexceptions import HTTPInternalServerError
except ImportError:
    raise unittest.SkipTest("pyramid/webtest is not installed")


class TestException(Exception):
    pass


class ControlFlowException(Exception):
    pass


class ControlFlowException2(Exception):
    pass


class ExceptionViewException(Exception):
    pass


def example_application(request):
    if "error" in request.params:
        raise TestException("this is a test")

    if "control_flow_exception" in request.params:
        raise ControlFlowException()

    if "exception_view_exception" in request.params:
        raise ControlFlowException2()

    if "stream" in request.params:

        def make_iter():
            yield b"foo"
            yield b"bar"

        return Response(status_code=200, app_iter=make_iter())

    return {"test": "success"}


def render_exception_view(request):
    return HTTPInternalServerError(title="a fancy title", body="a fancy explanation")


def render_bad_exception_view(request):
    raise ExceptionViewException()


def local_tracing_within_context(request):
    ctx = trace.set_span_in_context(request.span)
    tracer = trace.get_tracer(__name__)
    with tracer.start_span("local-req", ctx, kind=trace.SpanKind.INTERNAL):
        pass
    return {"trace": "success"}


class ConfiguratorTests(TestBase):
    def setUp(self):
        super().setUp()

        propagate.set_global_textmap(
            CompositePropagator(
                [RedditB3ThriftFormat(), RedditB3Format(), TraceContextTextMapPropagator()]
            )
        )

        configurator = Configurator()
        configurator.add_route("example", "/example", request_method="GET")
        configurator.add_route("route", "/route/{hello}/world", request_method="GET")
        configurator.add_route("trace_context", "/trace_context", request_method="GET")

        configurator.add_view(example_application, route_name="example", renderer="json")
        configurator.add_view(example_application, route_name="route", renderer="json")

        configurator.add_view(
            local_tracing_within_context, route_name="trace_context", renderer="json"
        )

        configurator.add_view(render_exception_view, context=ControlFlowException, renderer="json")

        configurator.add_view(
            render_bad_exception_view, context=ControlFlowException2, renderer="json"
        )

        self.observer = mock.Mock(spec=BaseplateObserver)

        self.baseplate = Baseplate()
        self.baseplate.register(self.observer)
        self.baseplate_configurator = BaseplateConfigurator(
            self.baseplate,
            edge_context_factory=FakeEdgeContextFactory(),
            header_trust_handler=StaticTrustHandler(trust_headers=True),
        )
        configurator.include(self.baseplate_configurator.includeme)
        self.context_init_event_subscriber = mock.Mock()
        configurator.add_subscriber(self.context_init_event_subscriber, ServerSpanInitialized)
        app = configurator.make_wsgi_app()
        self.test_app = webtest.TestApp(app)

    def test_no_trace_headers(self):
        self.test_app.get("/example")

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertIsNone(finished_spans[0].parent)

    def test_redditb3_trace_headers(self):
        self.test_app.get(
            "/example",
            headers={
                "X-Trace": "1234",
                "X-Edge-Request": base64.b64encode(FakeEdgeContextFactory.RAW_BYTES).decode(),
                "X-Parent": "2345",
                "X-Span": "3456",
                "X-Sampled": "1",
                "X-Flags": "1",
            },
        )

        finished_spans = self.get_finished_spans()

        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(finished_spans[0].get_span_context().trace_id, 1234)
        self.assertEqual(finished_spans[0].parent.span_id, 3456)
        self.assertTrue(finished_spans[0].get_span_context().trace_flags & 1)

    def test_w3c_trace_headers(self):
        self.test_app.get(
            "/example",
            headers={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
        )

        finished_spans = self.get_finished_spans()

        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(
            trace.format_trace_id(finished_spans[0].get_span_context().trace_id),
            "4bf92f3577b34da6a3ce929d0e0e4736",
        )
        self.assertEqual(trace.format_span_id(finished_spans[0].parent.span_id), "00f067aa0ba902b7")
        self.assertTrue(finished_spans[0].get_span_context().trace_flags & 1)

    def test_edge_request_headers(self):
        self.test_app.get(
            "/example",
            headers={
                "X-Trace": "1234",
                "X-Edge-Request": base64.b64encode(FakeEdgeContextFactory.RAW_BYTES).decode(),
                "X-Parent": "2345",
                "X-Span": "3456",
                "X-Sampled": "1",
                "X-Flags": "1",
            },
        )
        context, _ = self.observer.on_server_span_created.call_args[0]
        # we've been abusing the server span as a way to pass this context...
        assert context.edge_context == FakeEdgeContextFactory.DECODED_CONTEXT

    def test_empty_edge_request_headers(self):
        self.test_app.get(
            "/example",
            headers={
                "X-Trace": "1234",
                "X-Edge-Request": "",
                "X-Parent": "2345",
                "X-Span": "3456",
                "X-Sampled": "1",
                "X-Flags": "1",
            },
        )
        context, _ = self.observer.on_server_span_created.call_args[0]
        # we've been abusing the server span as a way to pass this context...
        self.assertEqual(context.raw_edge_context, b"")

    def test_not_found(self):
        resp = self.test_app.get("/nope", status=404)

        finished_spans = self.get_finished_spans()

        self.assertEqual(len(finished_spans), 1)
        self.assertSpanHasAttributes(finished_spans[0], {"http.status_code": 404})

    def test_not_found_echo_path(self):
        # confirm that issue #800 isn't reintroduced. This is an issue where we
        # echo the path to the 404 in the response which probably isn't a
        # problem, but does show up in automated vuln scans which can cause some
        # extra work hunting down false positives
        resp = self.test_app.get("/doesnt_exist", status=404)
        self.assertNotIn(b"doesnt_exist", resp.body)

    def test_exception_caught(self):
        with self.assertRaises(TestException):
            self.test_app.get("/example?error")
        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(len(finished_spans[0].events), 1)
        self.assertEqual(finished_spans[0].events[0].name, "exception")

    def test_control_flow_exception_not_caught(self):
        response = self.test_app.get("/example?control_flow_exception", status=500)

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(len(finished_spans[0].events), 0)

    def test_exception_in_exception_view_caught(self):
        with self.assertRaises(ExceptionViewException):
            self.test_app.get("/example?exception_view_exception")

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(len(finished_spans[0].events), 1)
        self.assertEqual(finished_spans[0].events[0].name, "exception")

    def test_local_trace_in_context(self):
        self.test_app.get("/trace_context")

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 2)
        self.assertEqual(
            finished_spans[0].parent.span_id, finished_spans[1].get_span_context().span_id
        )
        self.assertEqual(finished_spans[0].kind, trace.SpanKind.INTERNAL)
        self.assertEqual(finished_spans[1].kind, trace.SpanKind.SERVER)

    def test_streaming_response(self):
        class StreamingTestResponse(webtest.TestResponse):
            def decode_content(self):
                # keep your grubby hands off the app_iter, webtest!!!!
                pass

            @property
            def body(self):
                # seriously
                pass

        class StreamingTestRequest(webtest.TestRequest):
            ResponseClass = StreamingTestResponse

        self.test_app.RequestClass = StreamingTestRequest

        response = self.test_app.get("/example?stream")

        # ok, we've returned from the wsgi app but the iterator's not done
        # so... we should have started the span but not finished it yet
        self.assertEqual(len(self.get_finished_spans()), 0)

        self.assertEqual(b"foo", next(response.app_iter))
        self.assertEqual(len(self.get_finished_spans()), 0)

        self.assertEqual(b"bar", next(response.app_iter))
        self.assertEqual(len(self.get_finished_spans()), 0)

        with self.assertRaises(StopIteration):
            next(response.app_iter)
        self.assertEqual(len(self.get_finished_spans()), 1)

        response.app_iter.close()
