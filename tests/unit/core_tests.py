from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import jwt

from baseplate.core import (
    Baseplate,
    BaseplateObserver,
    LocalSpan,
    ServerSpan,
    ServerSpanObserver,
    Span,
    SpanObserver,
    TraceInfo,
    AuthenticationContext,
    UndefinedSecretsException,
    UndefinedAuthenticationError,
)
from baseplate.integration import WrappedRequestContext
from baseplate.file_watcher import FileWatcher
from baseplate.secrets import store

from .. import mock

cryptography_installed = True
try:
    import cryptography
except:
    cryptography_installed = False

def make_test_server_span(context=None):
    if not context:
        context = mock.Mock()
    return ServerSpan(1, 2, 3, None, 0, "name", context)


def make_test_span(context=None, local=False):
    if not context:
        context = mock.Mock()
    span = Span(1, 2, 3, None, 0, "name", context)
    if local:
        span = LocalSpan(1, 2, 3, None, 0, "name", context)
    return span


class BaseplateTests(unittest.TestCase):
    def test_server_observer_made(self):
        mock_context = mock.Mock()
        mock_observer = mock.Mock(spec=BaseplateObserver)

        baseplate = Baseplate()
        baseplate.register(mock_observer)
        server_span = baseplate.make_server_span(mock_context, "name", TraceInfo(1, 2, 3, None, 0))

        self.assertEqual(baseplate.observers, [mock_observer])
        self.assertEqual(mock_observer.on_server_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_server_span_created.call_args,
            mock.call(mock_context, server_span))

    def test_null_server_observer(self):
        mock_context = mock.Mock()
        mock_observer = mock.Mock(spec=BaseplateObserver)
        mock_observer.on_server_span_created.return_value = None

        baseplate = Baseplate()
        baseplate.register(mock_observer)
        server_span = baseplate.make_server_span(mock_context, "name", TraceInfo(1, 2, 3, None, 0))

        self.assertEqual(server_span.observers, [])


class SpanTests(unittest.TestCase):
    def test_events(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = make_test_span()
        span.register(mock_observer)

        span.start()
        self.assertEqual(mock_observer.on_start.call_count, 1)

        span.set_tag("key", "value")
        mock_observer.on_set_tag("key", "value")

        span.log("name", "payload")
        mock_observer.on_log("name", "payload")

        span.finish()
        mock_observer.on_finish(exc_info=None)

    def test_context(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = make_test_span()
        span.register(mock_observer)

        with span:
            self.assertEqual(mock_observer.on_start.call_count, 1)
        self.assertEqual(mock_observer.on_finish.call_count, 1)

    def test_context_with_exception(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        #span = Span(1, 2, 3, None, 0, "name")
        span = make_test_span()
        span.register(mock_observer)

        class TestException(Exception):
            pass

        exc = TestException()
        with self.assertRaises(TestException):
            with span:
                raise exc
        self.assertEqual(mock_observer.on_finish.call_count, 1)
        _, captured_exc, _ = mock_observer.on_finish.call_args[0][0]
        self.assertEqual(captured_exc, exc)


class ServerSpanTests(unittest.TestCase):
    @mock.patch("random.getrandbits", autospec=True)
    def test_make_child(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE

        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_context = mock.Mock()

        server_span = ServerSpan("trace", "parent", "id", None, 0, "name", mock_context)
        server_span.register(mock_observer)
        child_span = server_span.make_child("child_name")

        self.assertEqual(child_span.name, "child_name")
        self.assertEqual(child_span.id, 0xCAFE)
        self.assertEqual(child_span.trace_id, "trace")
        self.assertEqual(child_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args,
            mock.call(child_span))

    def test_null_child(self):
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_observer.on_child_span_created.return_value = None

        server_span = make_test_server_span()
        #server_span = ServerSpan("trace", "parent", "id", None, 0, "name")
        server_span.register(mock_observer)
        child_span = server_span.make_child("child_name")

        self.assertEqual(child_span.observers, [])

    @mock.patch("random.getrandbits", autospec=True)
    def test_make_local_span(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_context = mock.Mock()
        mock_cloned_context = mock.Mock(spec=WrappedRequestContext)
        mock_context.clone.return_value = mock_cloned_context

        server_span = ServerSpan("trace", "parent", "id", None, 0, "name", mock_context)
        server_span.register(mock_observer)
        local_span = server_span.make_child("test_op", local=True,
                                            component_name="test_component")

        self.assertEqual(local_span.name, "test_op")
        self.assertEqual(local_span.id, 0xCAFE)
        self.assertEqual(local_span.trace_id, "trace")
        self.assertEqual(local_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args,
                         mock.call(local_span))

    @mock.patch("random.getrandbits", autospec=True)
    def test_make_local_span_copies_context(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_context = mock.Mock()
        mock_cloned_context = mock.Mock(spec=WrappedRequestContext)
        mock_context.clone.return_value = mock_cloned_context

        server_span = ServerSpan("trace", "parent", "id", None, 0, "name", mock_context)
        server_span.register(mock_observer)
        local_span = server_span.make_child("test_op", local=True,
                                            component_name="test_component")
        self.assertNotEqual(local_span.context, mock_context)


class LocalSpanTests(unittest.TestCase):
    def test_events(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = make_test_span(local=True)
        span.register(mock_observer)

        span.start()
        self.assertEqual(mock_observer.on_start.call_count, 1)

        span.set_tag("key", "value")
        mock_observer.on_set_tag("key", "value")

        span.log("name", "payload")
        mock_observer.on_log("name", "payload")

        span.make_child('local_child')
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        span.finish()
        mock_observer.on_finish(exc_info=None)


class TraceInfoTests(unittest.TestCase):

    def test_new_does_not_have_parent_id(self):
        new_trace_info = TraceInfo.new()
        self.assertIsNone(new_trace_info.parent_id)

    def test_new_does_not_set_flags(self):
        new_trace_info = TraceInfo.new()
        self.assertIsNone(new_trace_info.flags)

    def test_new_does_not_set_sampled(self):
        new_trace_info = TraceInfo.new()
        self.assertIsNone(new_trace_info.sampled)

    def test_from_upstream_fails_on_invalid_sampled(self):
        with self.assertRaises(ValueError) as e:
            TraceInfo.from_upstream(1, 2, 3, 'True', None)
        self.assertEqual(str(e.exception), "invalid sampled value")

    def test_from_upstream_fails_on_invalid_flags(self):
        with self.assertRaises(ValueError) as e:
            TraceInfo.from_upstream(1, 2, 3, True, -1)
        self.assertEqual(str(e.exception), "invalid flags value")

    def test_from_upstream_handles_no_sampled_or_flags(self):
        span = TraceInfo.from_upstream(1, 2, 3, None, None)
        self.assertIsNone(span.sampled)
        self.assertIsNone(span.flags)


class AuthenticationContextTests(unittest.TestCase):
    VALID_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X3VzZXJfaWQiLCJleHAiOjQ2NTY1OTM0NTV9.Q8bz2qccFOHLTQ6H3MPdjSh7wDkRQtbBuBwGMzNRKjDFSkCoVF5kiwhBUdwbW8UXO5iZn4Bh7oKdj69lIEOATUxFBblU8Do05EfjECXLYGdbr6ClNmldrB8SsdAtQYQ4Ud-70Z8_75QvkqX_TY5OA4asGJZwH9MC7oHey47-38I"
    EXPIRED_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X3VzZXJfaWQiLCJleHAiOjE1MDI5OTM3NTN9.OPBIxaOEx0hnnB_wrfCuqfSIeP0a1abNdoZ2KejXReKeETQathr-PW2GqAhjGcUdCG3rXK8ezFKXdlB65kloqNdQii5b3qaJ5PDIdMNxY0Oi7TAqH86oog_umm7G-_p4MPPhRjxUm6Qp85-EaJUgyv26BUKSYY7-KyySjnrmP8g"
    TOKEN_SECRET = "-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC0Kd3qYtc6zI5tj3iKBux70BhE\nZLLJ7fAKNBUO7h9FCwUcYku+SFigzNOu3AAYt3seNgxl+cvMR2+SNwsa605J9D1v\n9eGmpcITQi85SeJnfR7LJUMu7RieY5wEl0RyuwnSkX3Gkv0+hZISC/XYcWEYolIi\n8725u7u/8HRtUeHoLwIDAQAB\n-----END PUBLIC KEY-----"

    def setUp(self):
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
        self.store = store.SecretsStore("/secrets")
        self.store._filewatcher = mock_filewatcher

    def test_empty_context(self):
        new_auth_context = AuthenticationContext()
        self.assertEqual(new_auth_context.token, None)

    def test_no_secrets(self):
        new_auth_context = AuthenticationContext("test token")
        with self.assertRaises(UndefinedSecretsException) as e:
            new_auth_context.valid

    @unittest.skipIf(not cryptography_installed, "cryptography not installed")
    def test_valid_context(self):
        new_auth_context = AuthenticationContext(self.VALID_TOKEN, self.store)
        self.assertTrue(new_auth_context.valid)
        self.assertEqual(new_auth_context.account_id, "test_user_id")

    @unittest.skipIf(not cryptography_installed, "cryptography not installed")
    def test_expired_context(self):
        new_auth_context = AuthenticationContext(self.EXPIRED_TOKEN, self.store)
        self.assertFalse(new_auth_context.valid)
        self.assertEqual(new_auth_context.account_id, None)

    @unittest.skipIf(not cryptography_installed, "cryptography not installed")
    def test_no_context(self):
        new_auth_context = AuthenticationContext(None, self.store)
        self.assertEqual(new_auth_context.valid, None)

        with self.assertRaises(UndefinedAuthenticationError) as e:
            new_auth_context.account_id
