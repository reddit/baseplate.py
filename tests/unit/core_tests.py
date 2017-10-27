from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
import unittest
import jwt

from baseplate.core import (
    AuthenticationContext,
    Baseplate,
    BaseplateObserver,
    EdgeRequestContext,
    LocalSpan,
    ServerSpan,
    ServerSpanObserver,
    Span,
    SpanObserver,
    TraceInfo,
    UndefinedSecretsException,
    WithheldAuthenticationError,
)
from baseplate.integration import WrappedRequestContext
from baseplate.file_watcher import FileWatcher
from baseplate.secrets import store
from baseplate.thrift.ttypes import Loid as TLoid
from baseplate.thrift.ttypes import Request as TRequest
from baseplate.thrift.ttypes import Session as TSession

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

    @mock.patch("random.getrandbits", autospec=True)
    def test_make_child(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE

        mock_observer = mock.Mock(spec=SpanObserver)
        mock_context = mock.Mock()

        local_span = LocalSpan("trace", "parent", "id", None, 0, "name", mock_context)
        local_span.register(mock_observer)
        child_span = local_span.make_child("child_name")

        self.assertEqual(child_span.name, "child_name")
        self.assertEqual(child_span.id, 0xCAFE)
        self.assertEqual(child_span.trace_id, "trace")
        self.assertEqual(child_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args,
            mock.call(child_span))


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


class ContextHeaderTestsBase(unittest.TestCase):
    VALID_TOKEN = b"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0X3VzZXJfaWQiLCJleHAiOjQ2NTY1OTM0NTV9.Q8bz2qccFOHLTQ6H3MPdjSh7wDkRQtbBuBwGMzNRKjDFSkCoVF5kiwhBUdwbW8UXO5iZn4Bh7oKdj69lIEOATUxFBblU8Do05EfjECXLYGdbr6ClNmldrB8SsdAtQYQ4Ud-70Z8_75QvkqX_TY5OA4asGJZwH9MC7oHey47-38I"
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


class AuthenticationContextTests(ContextHeaderTestsBase):

    def test_empty_context(self):
        new_auth_context = AuthenticationContext()
        self.assertEqual(new_auth_context._token, None)

    def test_no_secrets(self):
        new_auth_context = AuthenticationContext("test token")
        with self.assertRaises(UndefinedSecretsException) as e:
            new_auth_context.valid

    @unittest.skipIf(sys.version_info.major != 3, "python 3 only")
    def test_python_3_ensure_token_is_bytes(self):
        auth_context = AuthenticationContext(token=self.VALID_TOKEN)
        self.assertEqual(auth_context._token, self.VALID_TOKEN)
        self.assertIs(type(auth_context._token), bytes)
        auth_context = AuthenticationContext(token=self.VALID_TOKEN.decode())
        self.assertEqual(auth_context._token, self.VALID_TOKEN)
        self.assertIs(type(auth_context._token), bytes)

    @unittest.skipIf(sys.version_info.major != 2, "python 2 only")
    def test_python_2_ensure_token_is_str(self):
        # Note, we don't use assertEqual in this test because
        # `"test" == u"test"` is True
        auth_context = AuthenticationContext(token=self.VALID_TOKEN.encode())
        self.assertIs(type(auth_context._token), str)
        auth_context = AuthenticationContext(token=self.VALID_TOKEN.decode())
        self.assertIs(type(auth_context._token), str)

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
        self.assertFalse(new_auth_context.defined)
        self.assertFalse(new_auth_context.valid)

        with self.assertRaises(WithheldAuthenticationError) as e:
            new_auth_context.account_id


class EdgeRequestContextTests(ContextHeaderTestsBase):

    SERIALIZED_HEADER = b"\x0c\x00\x01\x0b\x00\x01\x00\x00\x00\x0bt2_deadbeef\n\x00\x02\x00\x00\x00\x00\x00\x01\x86\xa0\x00\x0c\x00\x02\x0b\x00\x01\x00\x00\x00\x08beefdead\x00\x00"  # noqa
    LOID_ID = "t2_deadbeef"
    LOID_CREATED_MS = 100000
    SESSION_ID = "beefdead"

    def test_create(self):
        authentication = AuthenticationContext(self.VALID_TOKEN, self.store)
        request_context = EdgeRequestContext.create(
            authentication_context=authentication,
            loid_id=self.LOID_ID,
            loid_created_ms=self.LOID_CREATED_MS,
            session_id=self.SESSION_ID,
        )
        self.assertIsNot(request_context._t_request, None)
        self.assertEqual(request_context._header, self.SERIALIZED_HEADER)
        self.assertEqual(
            request_context.header_values(),
            {
                "Edge-Request": self.SERIALIZED_HEADER,
                "Authentication": self.VALID_TOKEN,
            }
        )

    def test_create_validation(self):
        authentication = AuthenticationContext(self.VALID_TOKEN, self.store)
        with self.assertRaises(ValueError):
            EdgeRequestContext.create(
                authentication_context=authentication,
                loid_id="abc123",
                loid_created_ms=self.LOID_CREATED_MS,
                session_id=self.SESSION_ID,
            )

    def test_create_empty_context(self):
        request_context = EdgeRequestContext.create()
        self.assertEqual(
            request_context.header_values(),
            {
                "Edge-Request": b'\x0c\x00\x01\x00\x0c\x00\x02\x00\x00',
                "Authentication": None,
            },
        )

    def test_logged_out_user(self):
        authentication = AuthenticationContext()
        request_context = EdgeRequestContext(self.SERIALIZED_HEADER, authentication)
        with self.assertRaises(WithheldAuthenticationError):
            request_context.user.id
        with self.assertRaises(WithheldAuthenticationError):
           request_context.user.roles
        self.assertFalse(request_context.user.is_logged_in)
        self.assertEqual(request_context.user.loid, self.LOID_ID)
        self.assertEqual(request_context.user.cookie_created_ms, self.LOID_CREATED_MS)
        self.assertEqual(
            request_context.user.event_fields(),
            {
                "user_id": self.LOID_ID,
                "user_logged_in": False,
                "cookie_created": self.LOID_CREATED_MS,
            },
        )
        with self.assertRaises(WithheldAuthenticationError):
            request_context.oauth_client.id
        with self.assertRaises(WithheldAuthenticationError):
            request_context.oauth_client.is_type("third_party")
        self.assertEqual(request_context.session.id, self.SESSION_ID)
        self.assertEqual(
            request_context.event_fields(),
            {
                "user_id": self.LOID_ID,
                "user_logged_in": False,
                "cookie_created": self.LOID_CREATED_MS,
                "session_id": self.SESSION_ID,
                "oauth_client_id": None,
            },
        )

    def test_missing_secrets(self):
        authentication = AuthenticationContext(self.VALID_TOKEN)
        request_context = EdgeRequestContext(self.SERIALIZED_HEADER, authentication)
        with self.assertRaises(UndefinedSecretsException):
            request_context.user.id
        with self.assertRaises(UndefinedSecretsException):
           request_context.user.roles
        self.assertFalse(request_context.user.is_logged_in)
        self.assertEqual(request_context.user.loid, self.LOID_ID)
        self.assertEqual(request_context.user.cookie_created_ms, self.LOID_CREATED_MS)
        self.assertEqual(
            request_context.user.event_fields(),
            {
                "user_id": self.LOID_ID,
                "user_logged_in": False,
                "cookie_created": self.LOID_CREATED_MS,
            },
        )
        with self.assertRaises(UndefinedSecretsException):
            request_context.oauth_client.id
        with self.assertRaises(UndefinedSecretsException):
            request_context.oauth_client.is_type("third_party")
        self.assertEqual(request_context.session.id, self.SESSION_ID)
        self.assertEqual(
            request_context.event_fields(),
            {
                "user_id": self.LOID_ID,
                "user_logged_in": False,
                "cookie_created": self.LOID_CREATED_MS,
                "session_id": self.SESSION_ID,
                "oauth_client_id": None,
            },
        )

    @unittest.skipIf(not cryptography_installed, "cryptography not installed")
    def test_logged_in_user(self):
        authentication = AuthenticationContext(self.VALID_TOKEN, self.store)
        request_context = EdgeRequestContext(self.SERIALIZED_HEADER, authentication)
        self.assertEqual(request_context.user.id, "test_user_id")
        self.assertTrue(request_context.user.is_logged_in)
        self.assertEqual(request_context.user.loid, self.LOID_ID)
        self.assertEqual(request_context.user.cookie_created_ms, self.LOID_CREATED_MS)
        self.assertEqual(request_context.user.roles, set())
        self.assertEqual(
            request_context.user.event_fields(),
            {
                "user_id": "test_user_id",
                "user_logged_in": True,
                "cookie_created": self.LOID_CREATED_MS,
            },
        )
        self.assertIs(request_context.oauth_client.id, None)
        self.assertFalse(request_context.oauth_client.is_type("third_party"))
        self.assertEqual(request_context.session.id, self.SESSION_ID)
        self.assertEqual(
            request_context.event_fields(),
            {
                "user_id": "test_user_id",
                "user_logged_in": True,
                "cookie_created": self.LOID_CREATED_MS,
                "session_id": self.SESSION_ID,
                "oauth_client_id": None,
            },
        )

    @unittest.skipIf(not cryptography_installed, "cryptography not installed")
    def test_expired_token(self):
        authentication = AuthenticationContext(self.EXPIRED_TOKEN, self.store)
        request_context = EdgeRequestContext(self.SERIALIZED_HEADER, authentication)
        self.assertEqual(request_context.user.id, None)
        self.assertEqual(request_context.user.roles, set())
        self.assertFalse(request_context.user.is_logged_in)
        self.assertEqual(request_context.user.loid, self.LOID_ID)
        self.assertEqual(request_context.user.cookie_created_ms, self.LOID_CREATED_MS)
        self.assertEqual(
            request_context.user.event_fields(),
            {
                "user_id": self.LOID_ID,
                "user_logged_in": False,
                "cookie_created": self.LOID_CREATED_MS,
            },
        )
        self.assertIs(request_context.oauth_client.id, None)
        self.assertFalse(request_context.oauth_client.is_type("third_party"))
        self.assertEqual(request_context.session.id, self.SESSION_ID)
        self.assertEqual(
            request_context.event_fields(),
            {
                "user_id": self.LOID_ID,
                "user_logged_in": False,
                "cookie_created": self.LOID_CREATED_MS,
                "session_id": self.SESSION_ID,
                "oauth_client_id": None,
            },
        )
