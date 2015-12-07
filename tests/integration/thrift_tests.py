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

from baseplate.core import Baseplate, BaseplateObserver
from baseplate.integration.thrift import BaseplateProcessorEventHandler
from baseplate.thrift import BaseplateService

from thrift.protocol.THeaderProtocol import THeaderProtocol
from thrift.server.TServer import TRpcConnectionContext
from thrift.transport.TTransport import TMemoryBuffer

from .. import mock


class TestHandler(BaseplateService.ContextIface):
    def is_healthy(self, context):
        return True


class ThriftTests(unittest.TestCase):
    def setUp(self):
        self.itrans = TMemoryBuffer()
        self.iprot = THeaderProtocol(self.itrans)

        self.otrans = TMemoryBuffer()
        self.oprot = THeaderProtocol(self.otrans)

        self.observer = mock.Mock(spec=BaseplateObserver)
        self.logger = mock.Mock(spec=logging.Logger)
        self.server_context = TRpcConnectionContext(
            self.itrans, self.iprot, self.oprot)

        baseplate = Baseplate()
        baseplate.register(self.observer)

        event_handler = BaseplateProcessorEventHandler(self.logger, baseplate)

        handler = TestHandler()
        self.processor = BaseplateService.ContextProcessor(handler)
        self.processor.setEventHandler(event_handler)

    def test_no_headers(self):
        client_memory_trans = TMemoryBuffer()
        client_prot = THeaderProtocol(client_memory_trans)
        client = BaseplateService.Client(client_prot)
        try:
            client.is_healthy()
        except:
            pass  # we don't have a test response for the client
        self.itrans._readBuffer = StringIO(client_memory_trans.getvalue())

        self.processor.process(self.iprot, self.oprot, self.server_context)

        self.assertEqual(self.observer.on_root_span_created.call_count, 1)

        context, root_span = self.observer.on_root_span_created.call_args[0]
        self.assertEqual(root_span.trace_id, "no-trace")
        self.assertEqual(root_span.parent_id, "no-parent")
        self.assertEqual(root_span.id, "no-span")

        mock_root_observer = self.observer.on_root_span_created.return_value
        self.assertTrue(mock_root_observer.on_start.called)
        self.assertTrue(mock_root_observer.on_stop.called)

    def test_with_headers(self):
        client_memory_trans = TMemoryBuffer()
        client_prot = THeaderProtocol(client_memory_trans)
        client_header_trans = client_prot.trans
        client_header_trans.set_header("Trace", "1234")
        client_header_trans.set_header("Parent", "2345")
        client_header_trans.set_header("Span", "3456")
        client = BaseplateService.Client(client_prot)
        try:
            client.is_healthy()
        except:
            pass  # we don't have a test response for the client
        self.itrans._readBuffer = StringIO(client_memory_trans.getvalue())

        self.processor.process(self.iprot, self.oprot, self.server_context)

        self.assertEqual(self.observer.on_root_span_created.call_count, 1)

        context, root_span = self.observer.on_root_span_created.call_args[0]
        self.assertEqual(root_span.trace_id, "1234")
        self.assertEqual(root_span.parent_id, "2345")
        self.assertEqual(root_span.id, "3456")

        mock_root_observer = self.observer.on_root_span_created.return_value
        self.assertTrue(mock_root_observer.on_start.called)
        self.assertTrue(mock_root_observer.on_stop.called)
