import queue
import socket
import unittest

from unittest import mock

from thrift.protocol import TBinaryProtocol
from thrift.protocol import THeaderProtocol
from thrift.Thrift import TException
from thrift.transport import THeaderTransport
from thrift.transport import TSocket
from thrift.transport import TTransport

from baseplate.lib import config
from baseplate.lib import thrift_pool
from baseplate.observers.timeout import ServerTimeout


EXAMPLE_ENDPOINT = config.EndpointConfiguration(socket.AF_INET, ("127.0.0.1", 1234))


class MakeTransportTests(unittest.TestCase):
    def test_inet(self):
        endpoint = config.EndpointConfiguration(socket.AF_INET, ("localhost", 1234))
        socket_transport = thrift_pool._make_transport(endpoint)

        self.assertFalse(socket_transport._unix_socket)

    def test_unix(self):
        endpoint = config.EndpointConfiguration(socket.AF_UNIX, "/tmp/socket")
        socket_transport = thrift_pool._make_transport(endpoint)

        self.assertTrue(socket_transport._unix_socket)

    def test_unknown(self):
        endpoint = config.EndpointConfiguration(socket.AF_UNSPEC, None)

        with self.assertRaises(Exception):
            thrift_pool._make_transport(endpoint)


@mock.patch("baseplate.lib.thrift_pool.RetryPolicy.new")
class MaxRetriesRenameTests(unittest.TestCase):
    def test_default_is_3(self, new_retry_policy):
        thrift_pool.ThriftConnectionPool(EXAMPLE_ENDPOINT)
        new_retry_policy.assert_called_with(attempts=3)

    def test_default_through_parser(self, new_retry_policy):
        config = {"example.endpoint": "127.0.0.1:1234"}
        thrift_pool.thrift_pool_from_config(config, prefix="example.")
        new_retry_policy.assert_called_with(attempts=3)

    def test_max_retries_config_error(self, new_retry_policy):
        config = {
            "example.endpoint": "127.0.0.1:1234",
            "example.max_retries": "5",
        }
        with self.assertRaises(Exception):
            thrift_pool.thrift_pool_from_config(config, prefix="example.")

    def test_max_connection_attempts_works(self, new_retry_policy):
        thrift_pool.ThriftConnectionPool(EXAMPLE_ENDPOINT, max_connection_attempts=5)
        new_retry_policy.assert_called_with(attempts=5)

    def test_max_connection_attempts_works_through_config(self, new_retry_policy):
        config = {"example.endpoint": "127.0.0.1:1234", "example.max_connection_attempts": "5"}
        thrift_pool.thrift_pool_from_config(config, prefix="example.")
        new_retry_policy.assert_called_with(attempts=5)


class ThriftConnectionPoolTests(unittest.TestCase):
    def setUp(self):
        self.mock_queue = mock.Mock(spec=queue.Queue)
        self.pool = thrift_pool.ThriftConnectionPool(EXAMPLE_ENDPOINT)
        self.pool.pool = self.mock_queue

    def test_pool_empty_timeout(self):
        self.mock_queue.get.side_effect = queue.Empty

        with self.assertRaises(TTransport.TTransportException):
            self.pool._get_from_pool()

    def test_pool_with_framed_protocol_factory(self):
        def framed_protocol_factory(trans):
            trans = TTransport.TFramedTransport(trans)
            return TBinaryProtocol.TBinaryProtocol(trans)

        framed_pool = thrift_pool.ThriftConnectionPool(
            EXAMPLE_ENDPOINT, protocol_factory=framed_protocol_factory
        )
        trans = thrift_pool._make_transport(EXAMPLE_ENDPOINT)
        prot = framed_pool.protocol_factory(trans)

        self.assertTrue(isinstance(prot, TBinaryProtocol.TBinaryProtocol))
        self.assertTrue(isinstance(prot.trans, TTransport.TFramedTransport))

    @mock.patch("time.time")
    def test_pool_has_valid_connection(self, mock_time):
        mock_time.return_value = 123
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.baseplate_birthdate = 122
        self.mock_queue.get.return_value = mock_prot

        prot = self.pool._get_from_pool()

        self.assertEqual(prot, mock_prot)

    @mock.patch("baseplate.lib.thrift_pool._make_transport")
    @mock.patch("time.time")
    def test_pool_closes_stale_connection(self, mock_time, mock_make_transport):
        stale_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        stale_prot.trans = mock.Mock(spec=TSocket.TSocket)

        stale_prot.baseplate_birthdate = 10
        mock_time.return_value = 200
        self.mock_queue.get.return_value = stale_prot

        fresh_trans = mock.Mock(spec=TSocket.TSocket)
        fresh_trans.protocol_id = THeaderTransport.THeaderSubprotocolID.BINARY
        mock_make_transport.return_value = fresh_trans

        with self.pool.connection() as prot:
            pass

        self.assertTrue(stale_prot.trans.close.called)
        self.assertEqual(prot.trans._transport, fresh_trans)

    @mock.patch("baseplate.lib.thrift_pool._make_transport")
    @mock.patch("time.time")
    def test_pool_replaces_closed_connection(self, mock_time, mock_make_transport):
        stale_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        stale_prot.trans = mock.Mock(spec=TSocket.TSocket)
        stale_prot.trans.isOpen.return_value = False
        stale_prot.baseplate_birthdate = 199
        self.mock_queue.get.return_value = stale_prot
        mock_time.return_value = 200

        fresh_trans = mock.Mock(spec=TSocket.TSocket)
        fresh_trans.protocol_id = THeaderTransport.THeaderSubprotocolID.BINARY
        mock_make_transport.return_value = fresh_trans

        with self.pool.connection() as prot:
            pass

        self.assertTrue(stale_prot.trans.close.called)
        self.assertEqual(prot.trans._transport, fresh_trans)

    @mock.patch("baseplate.lib.thrift_pool._make_transport")
    @mock.patch("time.time")
    def test_retry_on_failed_connect(self, mock_time, mock_make_transport):
        self.mock_queue.get.return_value = None
        mock_time.return_value = 200

        broken_trans = mock.Mock(spec=TSocket.TSocket)
        broken_trans.protocol_id = THeaderTransport.THeaderSubprotocolID.BINARY
        broken_trans.open.side_effect = TTransport.TTransportException

        ok_trans = mock.Mock(spec=TSocket.TSocket)
        ok_trans.protocol_id = THeaderTransport.THeaderSubprotocolID.BINARY

        mock_make_transport.side_effect = [broken_trans, ok_trans]

        prot = self.pool._create_connection()

        self.assertEqual(prot.trans._transport, ok_trans)
        self.assertEqual(ok_trans.open.call_count, 1)

    @mock.patch("baseplate.lib.thrift_pool._make_transport")
    @mock.patch("time.time")
    def test_max_retry_on_connect(self, mock_time, mock_make_transport):
        self.mock_queue.get.return_value = None
        mock_time.return_value = 200

        broken_trans = mock.Mock(spec=TSocket.TSocket)
        broken_trans.protocol_id = THeaderTransport.THeaderSubprotocolID.BINARY
        broken_trans.open.side_effect = TTransport.TTransportException

        mock_make_transport.side_effect = [broken_trans] * 3

        with self.assertRaises(TTransport.TTransportException):
            self.pool._create_connection()

    def test_release_open(self):
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.trans = mock.Mock(spec=TSocket.TSocket)
        mock_prot.trans.isOpen.return_value = True

        self.pool._release(mock_prot)

        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(mock_prot))

    def test_release_closed(self):
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.trans = mock.Mock(spec=TSocket.TSocket)
        mock_prot.trans.isOpen.return_value = False

        self.pool._release(mock_prot)

        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(None))

    @mock.patch("time.time")
    def test_context_normal(self, mock_time):
        mock_time.return_value = 123
        mock_prot = mock.Mock(spec=THeaderProtocol.THeaderProtocol)
        mock_prot.baseplate_birthdate = 122
        mock_prot.trans = mock.Mock(spec=TSocket.TSocket)
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
        mock_prot.trans = mock.Mock(spec=TSocket.TSocket)
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
        mock_prot.trans = mock.Mock(spec=TSocket.TSocket)
        mock_prot.trans.isOpen.return_value = True
        self.mock_queue.get.return_value = mock_prot

        with self.assertRaises(Exception):
            with self.pool.connection() as prot:
                raise Exception

        self.assertEqual(prot, mock_prot)
        self.assertEqual(self.mock_queue.get.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(mock_prot))

    @mock.patch("baseplate.lib.thrift_pool._make_transport")
    @mock.patch("time.time")
    def test_context_server_timeout(self, mock_time, mock_make_transport):
        mock_time.return_value = 123

        self.mock_queue.get.return_value = None

        fresh_trans = mock.Mock(spec=TSocket.TSocket)
        fresh_trans.protocol_id = THeaderTransport.THeaderSubprotocolID.BINARY
        fresh_trans.open.side_effect = ServerTimeout("span", 10.0, False)
        mock_make_transport.return_value = fresh_trans

        with self.assertRaises(ServerTimeout):
            with self.pool.connection() as _:
                pass

        self.assertEqual(self.mock_queue.get.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(None))

    @mock.patch("baseplate.lib.thrift_pool._make_transport")
    @mock.patch("time.time")
    def test_context_t_transport_exception(self, mock_time, mock_make_transport):
        mock_time.return_value = 123

        self.mock_queue.get.return_value = None

        broken_trans = mock.Mock(spec=TSocket.TSocket)
        broken_trans.protocol_id = THeaderTransport.THeaderSubprotocolID.BINARY
        broken_trans.open.side_effect = TTransport.TTransportException
        mock_make_transport.return_value = broken_trans

        with self.assertRaises(TTransport.TTransportException):
            with self.pool.connection() as _:
                pass

        self.assertEqual(self.mock_queue.get.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_count, 1)
        self.assertEqual(self.mock_queue.put.call_args, mock.call(None))

    def test_pool_checkout_exception(self):
        pool = thrift_pool.ThriftConnectionPool(EXAMPLE_ENDPOINT)

        def mock_get():
            raise Exception

        pool.pool.get = mock_get

        with self.assertRaises(Exception):
            with pool.connection() as _:
                pass

        self.assertEqual(0, pool.checkedout)
