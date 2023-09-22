import unittest

from unittest import mock

from opentelemetry import trace
from opentelemetry.test.test_base import TestBase
from pyramid.response import Response

from baseplate import Baseplate

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
    tracer = trace.get_tracer("in-context")
    with tracer.start_as_current_span("local-req"):
        pass
    return {"trace": "success"}


class ConfiguratorTests(TestBase):
    def setUp(self):
        super().setUp()
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

        self.baseplate = Baseplate()
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

    @mock.patch("random.getrandbits")
    def test_no_trace_headers(self, getrandbits):
        getrandbits.return_value = 1234
        self.test_app.get("/example")

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertIsNone(finished_spans[0].parent)

    def test_trace_headers(self):
        self.test_app.get(
            "/example",
            headers={
                "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            },
        )

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertEqual(finished_spans[0].parent.span_id, 0x00F067AA0BA902B7)
        self.assertEqual(finished_spans[0].context.trace_id, 0x4BF92F3577B34DA6A3CE929D0E0E4736)

    def test_not_found(self):
        self.test_app.get("/nope", status=404)

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertIsNone(finished_spans[0].parent)

    def test_exception_caught(self):
        with self.assertRaises(TestException):
            self.test_app.get("/example?error")

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertFalse(finished_spans[0].status.is_ok)
        self.assertGreater(len(finished_spans[0].events), 0)
        self.assertEqual(finished_spans[0].events[0].name, "exception")

    def test_control_flow_exception_not_caught(self):
        self.test_app.get("/example?control_flow_exception", status=500)

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertTrue(finished_spans[0].status.is_ok)

    def test_exception_in_exception_view_caught(self):
        with self.assertRaises(ExceptionViewException):
            self.test_app.get("/example?exception_view_exception")

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertFalse(finished_spans[0].status.is_ok)

    def test_distrust_headers(self):
        self.baseplate_configurator.header_trust_handler.trust_headers = False
        # We need to get this into the settings so that we can load it in our tween.
        # The above method should be load at app initialisation still.
        self.test_app.app.registry.settings["reddit.tracing.trust_headers"] = False

        self.test_app.get(
            "/example",
            headers={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
        )

        finished_spans = self.get_finished_spans()
        self.assertEqual(len(finished_spans), 1)
        self.assertIsNone(finished_spans[0].parent)

    def test_local_trace_in_context(self):
        self.test_app.get("/trace_context")

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 1)
        self.assertEqual(finished_spans[0].kind, trace.SpanKind.INTERNAL)

        # self.assertEqual(self.server_observer.on_child_span_created.call_count, 1)
        # child_span = self.server_observer.on_child_span_created.call_args[0][0]
        # context, server_span = self.observer.on_server_span_created.call_args[0]
        # self.assertNotEqual(child_span.context, context)