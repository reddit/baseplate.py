from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import contextlib
import logging
import unittest
import jwt

try:
    from io import BytesIO as StringIO
except ImportError:
    from cStringIO import StringIO

from baseplate.core import (
    Baseplate,
    BaseplateObserver,
    EdgeRequestContext,
    EdgeRequestContextFactory,
    NoAuthenticationError,
    ServerSpanObserver,
)
from baseplate.context.thrift import ThriftContextFactory
from baseplate.file_watcher import FileWatcher
from baseplate.secrets import store

from baseplate.integration.thrift import BaseplateProcessorEventHandler

from thrift.protocol.THeaderProtocol import THeaderProtocol
from thrift.server.TServer import TRpcConnectionContext
from thrift.transport.TTransport import TMemoryBuffer, TTransportException

from .test_thrift import TestService, ttypes
from .. import (
    mock,
    AUTH_TOKEN_PUBLIC_KEY,
    AUTH_TOKEN_VALID,
    SERIALIZED_EDGECONTEXT_WITH_EXPIRED_AUTH,
    SERIALIZED_EDGECONTEXT_WITH_NO_AUTH,
    SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH,
)


class UnexpectedException(Exception):
    pass


class TestHandler(TestService.ContextIface):
    def example_simple(self, context):
        return True

    def example_throws(self, context, crash):
        if crash:
            raise UnexpectedException
        else:
            raise ttypes.ExpectedException


class ThriftTests(unittest.TestCase):
    def setUp(self):
        self.itrans = TMemoryBuffer()
        self.iprot = THeaderProtocol(self.itrans)

        self.otrans = TMemoryBuffer()
        self.oprot = THeaderProtocol(self.otrans)

        self.observer = mock.Mock(spec=BaseplateObserver)
        self.server_observer = mock.Mock(spec=ServerSpanObserver)

        def _register_mock(context, server_span):
            server_span.register(self.server_observer)

        self.observer.on_server_span_created.side_effect = _register_mock

        self.logger = mock.Mock(spec=logging.Logger)
        self.server_context = TRpcConnectionContext(
            self.itrans, self.iprot, self.oprot)

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
        self.secrets = store.SecretsStore("/secrets")
        self.secrets._filewatcher = mock_filewatcher

        baseplate = Baseplate()
        baseplate.register(self.observer)

        self.edge_context_factory = EdgeRequestContextFactory(self.secrets)

        event_handler = BaseplateProcessorEventHandler(
            self.logger,
            baseplate,
            edge_context_factory=self.edge_context_factory,
        )

        handler = TestHandler()
        self.processor = TestService.ContextProcessor(handler)
        self.processor.setEventHandler(event_handler)

    @mock.patch("random.getrandbits")
    def test_no_trace_headers(self, getrandbits):
        getrandbits.return_value = 1234

        client_memory_trans = TMemoryBuffer()
        client_prot = THeaderProtocol(client_memory_trans)
        client = TestService.Client(client_prot)
        try:
            client.example_simple()
        except TTransportException:
            pass  # we don't have a test response for the client
        self.itrans._readBuffer = StringIO(client_memory_trans.getvalue())

        self.processor.process(self.iprot, self.oprot, self.server_context)

        self.assertEqual(self.observer.on_server_span_created.call_count, 1)

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, 1234)
        self.assertEqual(server_span.parent_id, None)
        self.assertEqual(server_span.id, 1234)

        self.assertEqual(self.server_observer.on_start.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_args[0], (None,))

    def test_with_headers(self):
        client_memory_trans = TMemoryBuffer()
        client_prot = THeaderProtocol(client_memory_trans)
        client_header_trans = client_prot.trans
        client_header_trans.set_header("Trace", "1234")
        client_header_trans.set_header("Parent", "2345")
        client_header_trans.set_header("Span", "3456")
        client_header_trans.set_header("Sampled", "1")
        client_header_trans.set_header("Flags", "1")
        client = TestService.Client(client_prot)
        try:
            client.example_simple()
        except TTransportException:
            pass  # we don't have a test response for the client
        self.itrans._readBuffer = StringIO(client_memory_trans.getvalue())

        self.processor.process(self.iprot, self.oprot, self.server_context)
        self.assertEqual(self.observer.on_server_span_created.call_count, 1)

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, 1234)
        self.assertEqual(server_span.parent_id, 2345)
        self.assertEqual(server_span.id, 3456)
        self.assertTrue(server_span.sampled)
        self.assertEqual(server_span.flags, 1)

        with self.assertRaises(NoAuthenticationError):
            context.request_context.user.id

        self.assertEqual(self.server_observer.on_start.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_args[0], (None,))

    def test_edge_request_headers(self):
        client_memory_trans = TMemoryBuffer()
        client_prot = THeaderProtocol(client_memory_trans)
        client_header_trans = client_prot.trans
        client_header_trans.set_header("Edge-Request", SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH)
        client_header_trans.set_header("Trace", "1234")
        client_header_trans.set_header("Parent", "2345")
        client_header_trans.set_header("Span", "3456")
        client_header_trans.set_header("Sampled", "1")
        client_header_trans.set_header("Flags", "1")
        client = TestService.Client(client_prot)
        try:
            client.example_simple()
        except TTransportException:
            pass  # we don't have a test response for the client
        self.itrans._readBuffer = StringIO(client_memory_trans.getvalue())

        self.processor.process(self.iprot, self.oprot, self.server_context)

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

    def test_expected_exception_not_passed_to_server_span_finish(self):
        client_memory_trans = TMemoryBuffer()
        client_prot = THeaderProtocol(client_memory_trans)
        client = TestService.Client(client_prot)
        try:
            client.example_throws(crash=False)
        except TTransportException:
            pass  # we don't have a test response for the client
        self.itrans._readBuffer = StringIO(client_memory_trans.getvalue())

        self.processor.process(self.iprot, self.oprot, self.server_context)

        self.assertEqual(self.server_observer.on_start.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_args[0], (None,))

    def test_unexpected_exception_passed_to_server_span_finish(self):
        client_memory_trans = TMemoryBuffer()
        client_prot = THeaderProtocol(client_memory_trans)
        client = TestService.Client(client_prot)
        try:
            client.example_throws(crash=True)
        except TTransportException:
            pass  # we don't have a test response for the client
        self.itrans._readBuffer = StringIO(client_memory_trans.getvalue())

        self.processor.process(self.iprot, self.oprot, self.server_context)

        self.assertEqual(self.server_observer.on_start.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_count, 1)
        _, captured_exc, _ = self.server_observer.on_finish.call_args[0][0]
        self.assertIsInstance(captured_exc, UnexpectedException)

    def test_client_proxy_flow(self):
        client_memory_trans = TMemoryBuffer()
        client_prot = THeaderProtocol(client_memory_trans)

        class Pool(object):
            @contextlib.contextmanager
            def connection(self):
                yield client_prot

        client_factory = ThriftContextFactory(Pool(), TestService.Client)
        span = mock.MagicMock()
        child_span = span.make_child().__enter__()
        child_span.trace_id = 1
        child_span.parent_id = 1
        child_span.id = 1
        child_span.sampled = True
        child_span.flags = None

        edge_context = self.edge_context_factory.from_upstream(
            SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH)
        edge_context.attach_context(child_span.context)
        client = client_factory.make_object_for_context("test", span)
        try:
            client.example_simple()
        except TTransportException:
            pass  # we don't have a test response for the client
        self.itrans._readBuffer = StringIO(client_memory_trans.getvalue())

        self.processor.process(self.iprot, self.oprot, self.server_context)

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
