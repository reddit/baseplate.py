from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import unittest

try:
    from io import BytesIO as StringIO
except ImportError:
    from cStringIO import StringIO

from baseplate.core import Baseplate, BaseplateObserver, ServerSpanObserver
from baseplate.integration.thrift import BaseplateProcessorEventHandler

from thrift.protocol.THeaderProtocol import THeaderProtocol
from thrift.server.TServer import TRpcConnectionContext
from thrift.transport.TTransport import TMemoryBuffer, TTransportException

from .test_thrift import TestService, ttypes
from .. import mock


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

        baseplate = Baseplate()
        baseplate.register(self.observer)

        event_handler = BaseplateProcessorEventHandler(self.logger, baseplate)

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

        self.assertEqual(self.server_observer.on_start.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_count, 1)
        self.assertEqual(self.server_observer.on_finish.call_args[0], (None,))

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
