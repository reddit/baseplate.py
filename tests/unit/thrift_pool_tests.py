from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import socket
import unittest

from baseplate import config, thrift_pool
from baseplate._compat import queue
from thrift.Thrift import TException
from thrift.transport import TTransport, THeaderTransport
from thrift.protocol import THeaderProtocol

from .. import mock


EXAMPLE_ENDPOINT = config.EndpointConfiguration(
    socket.AF_INET, ("127.0.0.1", 1234))


class MakeProtocolTests(unittest.TestCase):
    def test_inet(self):
        endpoint = config.EndpointConfiguration(
            socket.AF_INET, ("localhost", 1234))
        protocol = thrift_pool._make_protocol(endpoint)

        socket_transport = protocol.trans.getTransport()
        self.assertFalse(socket_transport._unix_socket)

    def test_unix(self):
        endpoint = config.EndpointConfiguration(
            socket.AF_UNIX, "/tmp/socket")
        protocol = thrift_pool._make_protocol(endpoint)

        socket_transport = protocol.trans.getTransport()
        self.assertTrue(socket_transport._unix_socket)

    def test_unknown(self):
        endpoint = config.EndpointConfiguration(
            socket.AF_UNSPEC, None)

        with self.assertRaises(Exception):
            thrift_pool._make_protocol(endpoint)


class ThriftConnectionPoolTests(unittest.TestCase):
    def setUp(self):
        self.mock_queue = mock.Mock(spec=queue.Queue)
        self.pool = thrift_pool.ThriftConnectionPool(EXAMPLE_ENDPOINT)
        self.pool.pool = self.mock_queue

    def test_pool_empty_timeout(self):
        self.mock_queue.get.side_effect = queue.Empty

        with self.assertRaises(TTransport.TTransportException):
            self.pool._acquire()

    @mock.patch("time.time")
    def test_pool_has_valid_connection(self, mock_time):
        mock_time.return_value = 123
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.baseplate_birthdate = 122
        self.mock_queue.get.return_value = mock_prot

        prot = self.pool._acquire()

        self.assertEqual(prot, mock_prot)

    @mock.patch("baseplate.thrift_pool._make_protocol")
    @mock.patch("time.time")
    def test_pool_closes_stale_connection(self, mock_time, mock_make_protocol):
        stale_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        stale_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)
        fresh_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        fresh_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)

        stale_prot.baseplate_birthdate = 10
        mock_time.return_value = 200
        self.mock_queue.get.return_value = stale_prot
        mock_make_protocol.return_value = fresh_prot

        prot = self.pool._acquire()

        self.assertTrue(stale_prot.trans.close.called)
        self.assertEqual(prot, fresh_prot)

    @mock.patch("baseplate.thrift_pool._make_protocol")
    @mock.patch("time.time")
    def test_retry_on_failed_connect(self, mock_time, mock_make_protocol):
        self.mock_queue.get.return_value = None
        mock_time.return_value = 200

        broken_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        broken_prot.baseplate_birthdate = 200
        broken_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)
        broken_prot.trans.open.side_effect = TTransport.TTransportException

        ok_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        ok_prot.baseplate_birthdate = 200
        ok_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)

        mock_make_protocol.side_effect = [broken_prot, ok_prot]

        prot = self.pool._acquire()

        self.assertEqual(prot, ok_prot)
        self.assertEqual(ok_prot.trans.open.call_count, 1)

    @mock.patch("baseplate.thrift_pool._make_protocol")
    @mock.patch("time.time")
    def test_max_retry_on_connect(self, mock_time, mock_make_protocol):
        self.mock_queue.get.return_value = None
        mock_time.return_value = 200

        broken_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        broken_prot.baseplate_birthdate = 200
        broken_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)
        broken_prot.trans.open.side_effect = TTransport.TTransportException

        mock_make_protocol.side_effect = [broken_prot] * 3

        with self.assertRaises(TTransport.TTransportException):
            self.pool._acquire()

    def test_release_open(self):
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)
        mock_prot.trans.isOpen.return_value = True

        self.pool._release(mock_prot)

        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(mock_prot))

    def test_release_closed(self):
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)
        mock_prot.trans.isOpen.return_value = False

        self.pool._release(mock_prot)

        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(None))

    @mock.patch("time.time")
    def test_context_normal(self, mock_time):
        mock_time.return_value = 123
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.baseplate_birthdate = 122
        mock_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)
        mock_prot.trans.isOpen.return_value = True
        self.mock_queue.get.return_value = mock_prot

        with self.pool.connection() as prot:
            pass

        self.assertEqual(prot, mock_prot)
        self.assertEqual(self.mock_queue.get.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(mock_prot))

    @mock.patch("time.time")
    def test_context_thrift_exception(self, mock_time):
        mock_time.return_value = 123
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.baseplate_birthdate = 122
        mock_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)
        mock_prot.trans.isOpen.return_value = True
        self.mock_queue.get.return_value = mock_prot

        with self.assertRaises(TException):
            with self.pool.connection() as prot:
                mock_prot.trans.isOpen.return_value = False
                raise TTransport.TTransportException

        self.assertEqual(prot, mock_prot)
        self.assertEqual(mock_prot.trans.close.call_count, 1)
        self.assertEqual(self.mock_queue.get.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(None))

    @mock.patch("time.time")
    def test_context_non_thrift_exception(self, mock_time):
        mock_time.return_value = 123
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.baseplate_birthdate = 122
        mock_prot.trans = mock.Mock(spec=THeaderTransport.THeaderTransport)
        mock_prot.trans.isOpen.return_value = True
        self.mock_queue.get.return_value = mock_prot

        with self.assertRaises(Exception):
            with self.pool.connection() as prot:
                raise Exception

        self.assertEqual(prot, mock_prot)
        self.assertEqual(self.mock_queue.get.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(mock_prot))
