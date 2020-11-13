import base64
import unittest

from unittest import mock

import jwt

from pyramid.response import Response

from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate import ServerSpanObserver
from baseplate.lib.edge_context import EdgeRequestContextFactory
from baseplate.lib.edge_context import NoAuthenticationError
from baseplate.testing.lib.secrets import FakeSecretsStore

try:
    import webtest

    from baseplate.frameworks.pyramid import BaseplateConfigurator, ServerSpanInitialized
    from pyramid.config import Configurator
except ImportError:
    raise unittest.SkipTest("pyramid/webtest is not installed")

from .. import AUTH_TOKEN_PUBLIC_KEY
from .. import SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH
from .. import SERIALIZED_EDGECONTEXT_WITH_NO_AUTH


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
    return


def render_bad_exception_view(request):
    raise ExceptionViewException()


def local_tracing_within_context(request):
    with request.trace.make_child("local-req", local=True, component_name="in-context"):
        pass
    return {"trace": "success"}


class ConfiguratorTests(unittest.TestCase):
    def setUp(self):
        configurator = Configurator()
        configurator.add_route("example", "/example", request_method="GET")
        configurator.add_route("trace_context", "/trace_context", request_method="GET")

        configurator.add_view(example_application, route_name="example", renderer="json")

        configurator.add_view(
            local_tracing_within_context, route_name="trace_context", renderer="json"
        )

        configurator.add_view(render_exception_view, context=ControlFlowException, renderer="json")

        configurator.add_view(
            render_bad_exception_view, context=ControlFlowException2, renderer="json"
        )

        secrets = FakeSecretsStore(
            {
                "secrets": {
                    "secret/authentication/public-key": {
                        "type": "versioned",
                        "current": AUTH_TOKEN_PUBLIC_KEY,
                    }
                },
            }
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
            trust_trace_headers=True,
            edge_context_factory=EdgeRequestContextFactory(secrets),
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
        self.assertEqual(server_span.trace_id, 1234)
        self.assertEqual(server_span.parent_id, None)
        self.assertEqual(server_span.id, 1234)

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        self.assertTrue(self.context_init_event_subscriber.called)

    def test_trace_headers(self):
        self.test_app.get(
            "/example",
            headers={
                "X-Trace": "1234",
                "X-Edge-Request": base64.b64encode(SERIALIZED_EDGECONTEXT_WITH_NO_AUTH).decode(),
                "X-Parent": "2345",
                "X-Span": "3456",
                "X-Sampled": "1",
                "X-Flags": "1",
            },
        )

        self.assertEqual(self.observer.on_server_span_created.call_count, 1)

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, 1234)
        self.assertEqual(server_span.parent_id, 2345)
        self.assertEqual(server_span.id, 3456)
        self.assertEqual(server_span.sampled, True)
        self.assertEqual(server_span.flags, 1)

        with self.assertRaises(NoAuthenticationError):
            context.request_context.user.id

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        self.assertTrue(self.context_init_event_subscriber.called)

    def test_edge_request_headers(self):
        self.test_app.get(
            "/example",
            headers={
                "X-Trace": "1234",
                "X-Edge-Request": base64.b64encode(SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH).decode(),
                "X-Parent": "2345",
                "X-Span": "3456",
                "X-Sampled": "1",
                "X-Flags": "1",
            },
        )
        context, _ = self.observer.on_server_span_created.call_args[0]
        try:
            self.assertEqual(context.request_context.user.id, "t2_example")
            self.assertEqual(context.request_context.user.roles, set())
            self.assertEqual(context.request_context.user.is_logged_in, True)
            self.assertEqual(context.request_context.user.loid, "t2_deadbeef")
            self.assertEqual(context.request_context.user.cookie_created_ms, 100000)
            self.assertEqual(context.request_context.oauth_client.id, None)
            self.assertFalse(context.request_context.oauth_client.is_type("third_party"))
            self.assertEqual(context.request_context.session.id, "beefdead")
        except jwt.exceptions.InvalidAlgorithmError:
            raise unittest.SkipTest("cryptography is not installed")

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
        self.assertEqual(context.raw_request_context, b"")

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
        self.test_app.get("/example?control_flow_exception")

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        self.assertTrue(self.context_init_event_subscriber.called)
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
        getrandbits.return_value = 1234
        self.baseplate_configurator.header_trust_handler.trust_headers = False

        self.test_app.get(
            "/example", headers={"X-Trace": "1234", "X-Parent": "2345", "X-Span": "3456"}
        )

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, getrandbits.return_value)
        self.assertEqual(server_span.parent_id, None)
        self.assertEqual(server_span.id, getrandbits.return_value)

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
