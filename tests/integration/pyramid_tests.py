import base64
import unittest

from unittest import mock

from pyramid.response import Response

from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate import ServerSpanObserver

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
    with request.span.make_child("local-req", local=True, component_name="in-context"):
        pass
    return {"trace": "success"}


class ConfiguratorTests(unittest.TestCase):
    def setUp(self):
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
        self.server_observer = mock.Mock(spec=ServerSpanObserver)

        def _register_mock(context, server_span):
            server_span.register(self.server_observer)

        self.observer.on_server_span_created.side_effect = _register_mock

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

    @mock.patch("random.getrandbits")
    def test_no_trace_headers(self, getrandbits):
        getrandbits.return_value = 1234
        self.test_app.get("/example")

        self.assertEqual(self.observer.on_server_span_created.call_count, 1)

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, "1234")
        self.assertEqual(server_span.parent_id, None)
        self.assertEqual(server_span.id, "1234")

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        self.assertTrue(self.context_init_event_subscriber.called)

    def test_trace_headers(self):
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

        self.assertEqual(self.observer.on_server_span_created.call_count, 1)

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, "1234")
        self.assertEqual(server_span.parent_id, "2345")
        self.assertEqual(server_span.id, "3456")
        self.assertEqual(server_span.sampled, True)
        self.assertEqual(server_span.flags, 1)

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        self.assertTrue(self.context_init_event_subscriber.called)

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
        self.assertEqual(context.raw_edge_context, b"")

    def test_not_found(self):
        self.test_app.get("/nope", status=404)

        self.assertFalse(self.observer.on_server_span_created.called)
        self.assertFalse(self.context_init_event_subscriber.called)

    def test_exception_caught(self):
        with self.assertRaises(TestException):
            self.test_app.get("/example?error")

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        self.assertTrue(self.context_init_event_subscriber.called)
        _, captured_exc, _ = self.server_observer.on_finish.call_args[0][0]
        self.assertIsInstance(captured_exc, TestException)

    def test_control_flow_exception_not_caught(self):
        response = self.test_app.get("/example?control_flow_exception", status=500)

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        self.assertTrue(self.context_init_event_subscriber.called)
        self.assertTrue(b"a fancy explanation", response.body)
        args, _ = self.server_observer.on_finish.call_args
        self.assertEqual(args[0], None)

    def test_exception_in_exception_view_caught(self):
        with self.assertRaises(ExceptionViewException):
            self.test_app.get("/example?exception_view_exception")

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        self.assertTrue(self.context_init_event_subscriber.called)
        _, captured_exc, _ = self.server_observer.on_finish.call_args[0][0]
        self.assertIsInstance(captured_exc, ExceptionViewException)

    @mock.patch("random.getrandbits")
    def test_distrust_headers(self, getrandbits):
        getrandbits.return_value = 9999
        self.baseplate_configurator.header_trust_handler.trust_headers = False

        self.test_app.get(
            "/example", headers={"X-Trace": "1234", "X-Parent": "2345", "X-Span": "3456"}
        )

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, str(getrandbits.return_value))
        self.assertEqual(server_span.parent_id, None)
        self.assertEqual(server_span.id, str(getrandbits.return_value))

    def test_local_trace_in_context(self):
        self.test_app.get("/trace_context")
        self.assertEqual(self.server_observer.on_child_span_created.call_count, 1)
        child_span = self.server_observer.on_child_span_created.call_args[0][0]
        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertNotEqual(child_span.context, context)

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
        self.assertTrue(self.server_observer.on_start.called)
        self.assertFalse(self.server_observer.on_finish.called)

        self.assertEqual(b"foo", next(response.app_iter))
        self.assertFalse(self.server_observer.on_finish.called)

        self.assertEqual(b"bar", next(response.app_iter))
        self.assertFalse(self.server_observer.on_finish.called)

        with self.assertRaises(StopIteration):
            next(response.app_iter)
        self.assertTrue(self.server_observer.on_finish.called)

        response.app_iter.close()
