from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# webtest doesn't play well with unicode literals for headers on py2 :(
#from __future__ import unicode_literals

import unittest
import jwt

from baseplate import Baseplate
from baseplate.core import (
    BaseplateObserver,
    EdgeRequestContextFactory,
    NoAuthenticationError,
    ServerSpanObserver,
)
from baseplate.file_watcher import FileWatcher
from baseplate.secrets import store

try:
    import webtest

    from baseplate.integration.pyramid import (
        BaseplateConfigurator,
        ServerSpanInitialized,
    )
    from pyramid.config import Configurator
except ImportError:
    raise unittest.SkipTest("pyramid/webtest is not installed")

from .. import (
    mock,
    AUTH_TOKEN_PUBLIC_KEY,
    SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH,
)


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

    return {"test": "success"}


def render_exception_view(request):
    return


def render_bad_exception_view(request):
    raise ExceptionViewException()


def local_tracing_within_context(request):
    with request.trace.make_child('local-req',
                                  local=True,
                                  component_name='in-context'):
        pass
    return {'trace': 'success'}

class ConfiguratorTests(unittest.TestCase):
    def setUp(self):
        configurator = Configurator()
        configurator.add_route("example", "/example", request_method="GET")
        configurator.add_route("trace_context", "/trace_context", request_method="GET")

        configurator.add_view(
            example_application, route_name="example", renderer="json")

        configurator.add_view(
            local_tracing_within_context, route_name="trace_context", renderer="json")

        configurator.add_view(
            render_exception_view,
            context=ControlFlowException,
            renderer="json",
        )

        configurator.add_view(
            render_bad_exception_view,
            context=ControlFlowException2,
            renderer="json",
        )

        mock_filewatcher = mock.Mock(spec=FileWatcher)
        mock_filewatcher.get_data.return_value = {
            "secrets": {
                "secret/authentication/public-key": {
                    "type": "versioned",
                    "current": AUTH_TOKEN_PUBLIC_KEY,
                },
            },
            "vault": {
                "token": "test",
                "url": "http://vault.example.com:8200/",
            }
        }
        secrets = store.SecretsStore("/secrets")
        secrets._filewatcher = mock_filewatcher

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
        self.test_app.get("/example", headers={
            "X-Trace": "1234",
            "X-Parent": "2345",
            "X-Span": "3456",
            "X-Sampled": "1",
            "X-Flags": "1",
        })

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

    def test_b3_trace_headers(self):
        self.test_app.get("/example", headers={
            "X-B3-TraceId": "1234",
            "X-B3-ParentSpanId": "2345",
            "X-B3-SpanId": "3456",
            "X-B3-Sampled": "1",
            "X-B3-Flags": "1",
        })

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
        self.test_app.get("/example", headers={
            "X-Trace": "1234",
            "X-Edge-Request": SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH,
            "X-Parent": "2345",
            "X-Span": "3456",
            "X-Sampled": "1",
            "X-Flags": "1",
        })
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

        self.test_app.get("/example", headers={
            "X-Trace": "1234",
            "X-Parent": "2345",
            "X-Span": "3456",
        })

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, getrandbits.return_value)
        self.assertEqual(server_span.parent_id, None)
        self.assertEqual(server_span.id, getrandbits.return_value)

    def test_local_trace_in_context(self):
        self.test_app.get('/trace_context')
        self.assertEqual(self.server_observer.on_child_span_created.call_count, 1)
        child_span = self.server_observer.on_child_span_created.call_args[0][0]
        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertNotEqual(child_span.context, context)
