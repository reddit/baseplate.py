import socket
import unittest

from unittest import mock

from baseplate.server import einhorn


class NotEinhornWorkerTests(unittest.TestCase):
    def test_is_not_worker(self):
        self.assertFalse(einhorn.is_worker())

    def test_get_socket_count(self):
        with self.assertRaises(einhorn.NotEinhornWorker):
            einhorn.get_socket_count()

    def test_get_socket(self):
        with self.assertRaises(einhorn.NotEinhornWorker):
            einhorn.get_socket()

    def test_send_ack(self):
        with self.assertRaises(einhorn.NotEinhornWorker):
            einhorn.ack_startup()


@mock.patch.dict(
    "os.environ",
    {
        "EINHORN_MASTER_PID": "123",
        "EINHORN_SOCK_PATH": "/tmp/einhorn.sock",
        "EINHORN_FD_COUNT": "2",
        "EINHORN_FD_0": "5",
        "EINHORN_FD_1": "6",
        "EINHORN_FD_FAMILY_1": "AF_UNIX",
    },
    clear=True,
)
class EinhornWorkerTests(unittest.TestCase):
    def setUp(self):
        getppid_patcher = mock.patch("os.getppid")
        getppid = getppid_patcher.start()
        getppid.return_value = 123
        self.addCleanup(getppid_patcher.stop)

    def test_is_worker(self):
        self.assertTrue(einhorn.is_worker())

    def test_get_socket_count(self):
        self.assertEqual(einhorn.get_socket_count(), 2)

    @mock.patch("socket.fromfd", autospec=True)
    def test_get_socket(self, fromfd):
        sock = einhorn.get_socket()
        fromfd.assert_called_with(5, socket.AF_INET, socket.SOCK_STREAM)
        self.assertEqual(sock, fromfd.return_value)

    @mock.patch("socket.fromfd", autospec=True)
    def test_get_socket_unix(self, fromfd):
        sock = einhorn.get_socket(1)
        fromfd.assert_called_with(6, socket.AF_UNIX, socket.SOCK_STREAM)
        self.assertEqual(sock, fromfd.return_value)

    def test_get_socket_out_of_bounds(self):
        with self.assertRaises(IndexError):
            einhorn.get_socket(-1)

        with self.assertRaises(IndexError):
            einhorn.get_socket(2)

    @mock.patch("socket.socket")
    @mock.patch("os.getpid", autospec=True)
    def test_send_ack(self, getpid, sock):
        getpid.return_value = 1337

        einhorn.ack_startup()

        sock.assert_called_with(socket.AF_UNIX, socket.SOCK_STREAM)
        mocket = sock.return_value
        mocket.connect.assert_called_with("/tmp/einhorn.sock")

        self.assertEqual(mocket.sendall.call_count, 1)
        self.assertEqual(mocket.close.call_count, 1)
