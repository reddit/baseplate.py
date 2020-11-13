import unittest

from unittest import mock

from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate import LocalSpan
from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate import ServerSpanObserver
from baseplate import Span
from baseplate import SpanObserver
from baseplate import TraceInfo
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.edge_context import EdgeRequestContextFactory
from baseplate.lib.edge_context import NoAuthenticationError
from baseplate.testing.lib.secrets import FakeSecretsStore

from .. import AUTH_TOKEN_PUBLIC_KEY
from .. import AUTH_TOKEN_VALID
from .. import SERIALIZED_EDGECONTEXT_WITH_ANON_AUTH
from .. import SERIALIZED_EDGECONTEXT_WITH_EXPIRED_AUTH
from .. import SERIALIZED_EDGECONTEXT_WITH_NO_AUTH
from .. import SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH


cryptography_installed = True
try:
    import cryptography

    del cryptography
except ImportError:
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
        baseplate = Baseplate()
        mock_context = baseplate.make_context_object()
        mock_observer = mock.Mock(spec=BaseplateObserver)
        baseplate.register(mock_observer)
        server_span = baseplate.make_server_span(mock_context, "name", TraceInfo(1, 2, 3, None, 0))

        self.assertEqual(baseplate.observers, [mock_observer])
        self.assertEqual(mock_observer.on_server_span_created.call_count, 1)
        self.assertEqual(
            mock_observer.on_server_span_created.call_args, mock.call(mock_context, server_span)
        )

    def test_null_server_observer(self):
        baseplate = Baseplate()
        mock_context = baseplate.make_context_object()
        mock_observer = mock.Mock(spec=BaseplateObserver)
        mock_observer.on_server_span_created.return_value = None
        baseplate.register(mock_observer)
        server_span = baseplate.make_server_span(mock_context, "name", TraceInfo(1, 2, 3, None, 0))

        self.assertEqual(server_span.observers, [])

    def test_configure_context_supports_complex_specs(self):
        from baseplate.clients.thrift import ThriftClient
        from baseplate.thrift import BaseplateServiceV2

        app_config = {
            "enable_some_fancy_feature": "true",
            "thrift.foo.endpoint": "localhost:9090",
            "thrift.bar.endpoint": "localhost:9091",
        }

        baseplate = Baseplate()
        baseplate.configure_context(
            app_config,
            {
                "enable_some_fancy_feature": config.Boolean,
                "thrift": {
                    "foo": ThriftClient(BaseplateServiceV2.Client),
                    "bar": ThriftClient(BaseplateServiceV2.Client),
                },
            },
        )

        context = baseplate.make_context_object()
        with baseplate.make_server_span(context, "test"):
            self.assertTrue(context.enable_some_fancy_feature)
            self.assertIsNotNone(context.thrift.foo)
            self.assertIsNotNone(context.thrift.bar)

    def test_with_server_context(self):
        baseplate = Baseplate()
        observer = mock.Mock(spec=BaseplateObserver)
        baseplate.register(observer)

        observer.on_server_span_created.assert_not_called()
        with baseplate.server_context("example") as context:
            observer.on_server_span_created.assert_called_once()
            self.assertIsInstance(context, RequestContext)

    def test_add_to_context(self):
        baseplate = Baseplate()
        forty_two_factory = mock.Mock(spec=ContextFactory)
        forty_two_factory.make_object_for_context = mock.Mock(return_value=42)
        baseplate.add_to_context("forty_two", forty_two_factory)
        baseplate.add_to_context("true", True)

        context = baseplate.make_context_object()

        self.assertEqual(42, context.forty_two)
        self.assertTrue(context.true)

    def test_add_to_context_supports_complex_specs(self):
        baseplate = Baseplate()
        forty_two_factory = mock.Mock(spec=ContextFactory)
        forty_two_factory.make_object_for_context = mock.Mock(return_value=42)
        context_spec = {
            "forty_two": forty_two_factory,
            "true": True,
            "nested": {"foo": "bar"},
        }
        baseplate.add_to_context("complex", context_spec)

        context = baseplate.make_context_object()

        self.assertEqual(42, context.complex.forty_two)
        self.assertTrue(context.complex.true)
        self.assertEqual("bar", context.complex.nested.foo)


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

        # span = Span(1, 2, 3, None, 0, "name")
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
        self.assertEqual(mock_observer.on_child_span_created.call_args, mock.call(child_span))

    def test_null_child(self):
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_observer.on_child_span_created.return_value = None

        server_span = make_test_server_span()
        # server_span = ServerSpan("trace", "parent", "id", None, 0, "name")
        server_span.register(mock_observer)
        child_span = server_span.make_child("child_name")

        self.assertEqual(child_span.observers, [])

    @mock.patch("random.getrandbits", autospec=True)
    def test_make_local_span(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_context = mock.Mock()
        mock_cloned_context = mock.Mock()
        mock_context.clone.return_value = mock_cloned_context

        server_span = ServerSpan("trace", "parent", "id", None, 0, "name", mock_context)
        server_span.register(mock_observer)
        local_span = server_span.make_child("test_op", local=True, component_name="test_component")

        self.assertEqual(local_span.name, "test_op")
        self.assertEqual(local_span.id, 0xCAFE)
        self.assertEqual(local_span.trace_id, "trace")
        self.assertEqual(local_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args, mock.call(local_span))

    @mock.patch("random.getrandbits", autospec=True)
    def test_make_local_span_copies_context(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_context = mock.Mock()
        mock_cloned_context = mock.Mock()
        mock_context.clone.return_value = mock_cloned_context

        server_span = ServerSpan("trace", "parent", "id", None, 0, "name", mock_context)
        server_span.register(mock_observer)
        local_span = server_span.make_child("test_op", local=True, component_name="test_component")
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

        span.make_child("local_child")
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
        self.assertEqual(mock_observer.on_child_span_created.call_args, mock.call(child_span))


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
            TraceInfo.from_upstream(1, 2, 3, "True", None)
        self.assertEqual(str(e.exception), "invalid sampled value")

    def test_from_upstream_fails_on_invalid_flags(self):
        with self.assertRaises(ValueError) as e:
            TraceInfo.from_upstream(1, 2, 3, True, -1)
        self.assertEqual(str(e.exception), "invalid flags value")

    def test_from_upstream_handles_no_sampled_or_flags(self):
        span = TraceInfo.from_upstream(1, 2, 3, None, None)
        self.assertIsNone(span.sampled)
        self.assertIsNone(span.flags)


class EdgeRequestContextTests(unittest.TestCase):
    LOID_ID = "t2_deadbeef"
    LOID_CREATED_MS = 100000
    SESSION_ID = "beefdead"
    DEVICE_ID = "becc50f6-ff3d-407a-aa49-fa49531363be"
    ORIGIN_NAME = "baseplate"
    COUNTRY_CODE = "OK"

    def setUp(self):
        self.store = FakeSecretsStore(
            {
                "secrets": {
                    "secret/authentication/public-key": {
                        "type": "versioned",
                        "current": AUTH_TOKEN_PUBLIC_KEY,
                    }
                },
            }
        )
        self.factory = EdgeRequestContextFactory(self.store)

    def test_create(self):
        request_context = self.factory.new(
            authentication_token=AUTH_TOKEN_VALID,
            loid_id=self.LOID_ID,
            loid_created_ms=self.LOID_CREATED_MS,
            session_id=self.SESSION_ID,
            device_id=self.DEVICE_ID,
            origin_service_name=self.ORIGIN_NAME,
            country_code=self.COUNTRY_CODE,
        )
        self.assertIsNot(request_context._t_request, None)
        self.assertEqual(request_context._header, SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH)

    def test_create_validation(self):
        with self.assertRaises(ValueError):
            self.factory.new(
                authentication_token=None,
                loid_id="abc123",
                loid_created_ms=self.LOID_CREATED_MS,
                session_id=self.SESSION_ID,
            )
        with self.assertRaises(ValueError):
            self.factory.new(
                authentication_token=AUTH_TOKEN_VALID,
                loid_id=self.LOID_ID,
                loid_created_ms=self.LOID_CREATED_MS,
                session_id=self.SESSION_ID,
                country_code="aa",
            )

    def test_create_empty_context(self):
        request_context = self.factory.new()
        self.assertEqual(
            request_context._header,
            b"\x0c\x00\x01\x00\x0c\x00\x02\x00\x0c\x00\x04\x00\x0c\x00\x05\x00\x0c\x00\x06\x00\x00",
        )

    def test_logged_out_user(self):
        request_context = self.factory.from_upstream(SERIALIZED_EDGECONTEXT_WITH_NO_AUTH)

        with self.assertRaises(NoAuthenticationError):
            request_context.user.id
        with self.assertRaises(NoAuthenticationError):
            request_context.user.roles

        self.assertFalse(request_context.user.is_logged_in)
        self.assertEqual(request_context.user.loid, self.LOID_ID)
        self.assertEqual(request_context.user.cookie_created_ms, self.LOID_CREATED_MS)

        with self.assertRaises(NoAuthenticationError):
            request_context.oauth_client.id
        with self.assertRaises(NoAuthenticationError):
            request_context.oauth_client.is_type("third_party")

        self.assertEqual(request_context.session.id, self.SESSION_ID)
        self.assertEqual(request_context.device.id, self.DEVICE_ID)
        self.assertEqual(
            request_context.event_fields(),
            {
                "user_id": self.LOID_ID,
                "logged_in": False,
                "cookie_created_timestamp": self.LOID_CREATED_MS,
                "session_id": self.SESSION_ID,
                "oauth_client_id": None,
                "device_id": self.DEVICE_ID,
            },
        )

    @unittest.skipIf(not cryptography_installed, "cryptography not installed")
    def test_logged_in_user(self):
        request_context = self.factory.from_upstream(SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH)

        self.assertEqual(request_context.user.id, "t2_example")
        self.assertTrue(request_context.user.is_logged_in)
        self.assertEqual(request_context.user.loid, self.LOID_ID)
        self.assertEqual(request_context.user.cookie_created_ms, self.LOID_CREATED_MS)
        self.assertEqual(request_context.user.roles, set())
        self.assertFalse(request_context.user.has_role("test"))
        self.assertIs(request_context.oauth_client.id, None)
        self.assertFalse(request_context.oauth_client.is_type("third_party"))
        self.assertEqual(request_context.session.id, self.SESSION_ID)
        self.assertEqual(request_context.device.id, self.DEVICE_ID)
        self.assertEqual(request_context.origin_service.name, self.ORIGIN_NAME)
        self.assertEqual(request_context.geolocation.country_code, self.COUNTRY_CODE)
        self.assertEqual(
            request_context.event_fields(),
            {
                "user_id": "t2_example",
                "logged_in": True,
                "cookie_created_timestamp": self.LOID_CREATED_MS,
                "session_id": self.SESSION_ID,
                "oauth_client_id": None,
                "device_id": self.DEVICE_ID,
            },
        )

    @unittest.skipIf(not cryptography_installed, "cryptography not installed")
    def test_expired_token(self):
        request_context = self.factory.from_upstream(SERIALIZED_EDGECONTEXT_WITH_EXPIRED_AUTH)

        with self.assertRaises(NoAuthenticationError):
            request_context.user.id
        with self.assertRaises(NoAuthenticationError):
            request_context.user.roles
        with self.assertRaises(NoAuthenticationError):
            request_context.oauth_client.id
        with self.assertRaises(NoAuthenticationError):
            request_context.oauth_client.is_type("third_party")

        self.assertFalse(request_context.user.is_logged_in)
        self.assertEqual(request_context.user.loid, self.LOID_ID)
        self.assertEqual(request_context.user.cookie_created_ms, self.LOID_CREATED_MS)
        self.assertEqual(request_context.session.id, self.SESSION_ID)
        self.assertEqual(
            request_context.event_fields(),
            {
                "user_id": self.LOID_ID,
                "logged_in": False,
                "cookie_created_timestamp": self.LOID_CREATED_MS,
                "session_id": self.SESSION_ID,
                "oauth_client_id": None,
            },
        )

    @unittest.skipIf(not cryptography_installed, "cryptography not installed")
    def test_anonymous_token(self):
        request_context = self.factory.from_upstream(SERIALIZED_EDGECONTEXT_WITH_ANON_AUTH)

        with self.assertRaises(NoAuthenticationError):
            request_context.user.id
        self.assertFalse(request_context.user.is_logged_in)
        self.assertEqual(request_context.user.loid, self.LOID_ID)
        self.assertEqual(request_context.user.cookie_created_ms, self.LOID_CREATED_MS)
        self.assertEqual(request_context.session.id, self.SESSION_ID)
        self.assertTrue(request_context.user.has_role("anonymous"))
