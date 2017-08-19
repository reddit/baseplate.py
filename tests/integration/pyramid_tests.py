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
    ServerSpanObserver,
    AuthenticationContextFactory,
)
from baseplate.file_watcher import FileWatcher
from baseplate.secrets import store

try:
    import webtest

    from baseplate.integration.pyramid import BaseplateConfigurator
    from pyramid.config import Configurator
    from pyramid.request import Request
except ImportError:
    raise unittest.SkipTest("pyramid/webtest is not installed")

from .. import mock


class TestException(Exception):
    pass


def example_application(request):
    if "error" in request.params:
        raise TestException("this is a test")
    return {"test": "success"}


def local_tracing_within_context(request):
    with request.trace.make_child('local-req',
                                  local=True,
                                  component_name='in-context'):
        pass
    return {'trace': 'success'}

class ConfiguratorTests(unittest.TestCase):
    VALID_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X3VzZXJfaWQiLCJleHAiOjQ2NTY1OTM0NTV9.Q8bz2qccFOHLTQ6H3MPdjSh7wDkRQtbBuBwGMzNRKjDFSkCoVF5kiwhBUdwbW8UXO5iZn4Bh7oKdj69lIEOATUxFBblU8Do05EfjECXLYGdbr6ClNmldrB8SsdAtQYQ4Ud-70Z8_75QvkqX_TY5OA4asGJZwH9MC7oHey47-38I"
    TOKEN_SECRET = "-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC0Kd3qYtc6zI5tj3iKBux70BhE\nZLLJ7fAKNBUO7h9FCwUcYku+SFigzNOu3AAYt3seNgxl+cvMR2+SNwsa605J9D1v\n9eGmpcITQi85SeJnfR7LJUMu7RieY5wEl0RyuwnSkX3Gkv0+hZISC/XYcWEYolIi\n8725u7u/8HRtUeHoLwIDAQAB\n-----END PUBLIC KEY-----"

    def setUp(self):
        configurator = Configurator()
        configurator.add_route("example", "/example", request_method="GET")
        configurator.add_route("trace_context", "/trace_context", request_method="GET")

        configurator.add_view(
            example_application, route_name="example", renderer="json")

        configurator.add_view(
            local_tracing_within_context, route_name="trace_context", renderer="json")

        mock_filewatcher = mock.Mock(spec=FileWatcher)
        mock_filewatcher.get_data.return_value = {
            "secrets": {
                "jwt/authentication/secret": {
                    "type": "simple",
                    "value": self.TOKEN_SECRET,
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
            auth_factory=AuthenticationContextFactory(secrets),
        )
        configurator.include(self.baseplate_configurator.includeme)
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

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)

    def test_auth_headers(self):
        self.test_app.get("/example", headers={
            "X-Trace": "1234",
            "X-Authentication": self.VALID_TOKEN,
            "X-Parent": "2345",
            "X-Span": "3456",
            "X-Sampled": "1",
            "X-Flags": "1",
        })
        context, _ = self.observer.on_server_span_created.call_args[0]
        try:
            self.assertTrue(context.authentication.valid)
            self.assertEqual(context.authentication.account_id, "test_user_id")
        except jwt.exceptions.InvalidAlgorithmError:
            raise unittest.SkipTest("cryptography is not installed")

    def test_blind_passthrough(self):
        self.test_app.get("/example", headers={
            "X-Trace": "1234",
            "X-Authentication": "invalid_but_doesnt_matter",
            "X-Parent": "2345",
            "X-Span": "3456",
            "X-Sampled": "1",
            "X-Flags": "1",
        })
        context, _ = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(context.authentication.token, "invalid_but_doesnt_matter")


    def test_not_found(self):
        self.test_app.get("/nope", status=404)

        self.assertFalse(self.observer.on_server_span_created.called)

    def test_exception_caught(self):
        with self.assertRaises(TestException):
            self.test_app.get("/example?error")

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        _, captured_exc, _ = self.server_observer.on_finish.call_args[0][0]
        self.assertIsInstance(captured_exc, TestException)

    @mock.patch("random.getrandbits")
    def test_distrust_headers(self, getrandbits):
        getrandbits.return_value = 1234
        self.baseplate_configurator.trust_trace_headers = False

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
