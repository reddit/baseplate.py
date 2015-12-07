from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import socket
import unittest

from baseplate import config, server

from ... import mock


EXAMPLE_ENDPOINT = config.EndpointConfiguration(
    socket.AF_INET, ("127.0.0.1", 1234))


class ParseArgsTests(unittest.TestCase):
    def test_no_args(self):
        with mock.patch("sys.stderr", mock.Mock()):
            with self.assertRaises(SystemExit):
                server.parse_args([])

    def test_filename(self):
        with mock.patch("argparse.FileType", autospec=True) as make_file:
            args = server.parse_args(["filename"])

        mock_file = make_file.return_value
        self.assertEqual(mock_file.call_args, mock.call("filename"))
        self.assertEqual(args.config_file, mock_file.return_value)

    @mock.patch("argparse.FileType", autospec=True)
    def test_options(self, make_file):
        args = server.parse_args([
            "filename",
            "--debug",
            "--app-name", "app",
            "--server-name", "server",
            "--bind", "1.2.3.4:81",
        ])
        self.assertTrue(args.debug)
        self.assertEqual(args.app_name, "app")
        self.assertEqual(args.server_name, "server")
        self.assertEqual(args.bind,
            config.EndpointConfiguration(socket.AF_INET, ("1.2.3.4", 81)))


class MakeListenerTests(unittest.TestCase):
    @mock.patch.dict("os.environ", {"EINHORN_FDS": "123"})
    @mock.patch("socket.fromfd", autospec=True)
    def test_einhorn_managed(self, fromfd):
        listener = server.make_listener(EXAMPLE_ENDPOINT)
        self.assertEqual(listener, fromfd.return_value)
        self.assertEqual(fromfd.call_count, 1)
        self.assertEqual(fromfd.call_args,
            mock.call(123, socket.AF_INET, socket.SOCK_STREAM))

    @mock.patch.dict("os.environ", {}, clear=True)
    @mock.patch("socket.socket", autospec=True)
    def test_manually_bound(self, mocket):
        listener = server.make_listener(EXAMPLE_ENDPOINT)

        self.assertEqual(mocket.call_args,
            mock.call(socket.AF_INET, socket.SOCK_STREAM))
        self.assertEqual(listener, mocket.return_value)

        self.assertEqual(listener.bind.call_args, mock.call(("127.0.0.1", 1234)))


class LoadFactoryTests(unittest.TestCase):
    @mock.patch("importlib.import_module", autospec=True)
    def test_full_url(self, import_module):
        factory = server._load_factory("package.module:callable", "default_name")

        self.assertEqual(import_module.call_args, mock.call("package.module"))
        self.assertEqual(factory, import_module.return_value.callable)

    @mock.patch("importlib.import_module", autospec=True)
    def test_default_name(self, import_module):
        factory = server._load_factory("package.module", "default_name")

        self.assertEqual(import_module.call_args, mock.call("package.module"))
        self.assertEqual(factory, import_module.return_value.default_name)


class EinhornAckStartupTests(unittest.TestCase):
    @mock.patch.dict("os.environ", {}, clear=True)
    @mock.patch("socket.fromfd", autospec=True)
    def test_no_sock(self, fromfd):
        server.einhorn_ack_startup()
        self.assertEqual(fromfd.call_count, 0)

    @mock.patch.dict("os.environ", {"EINHORN_SOCK_FD": "a"}, clear=True)
    @mock.patch("socket.fromfd", autospec=True)
    def test_invalid_sock(self, fromfd):
        server.einhorn_ack_startup()
        self.assertEqual(fromfd.call_count, 0)

    @mock.patch.dict("os.environ", {"EINHORN_SOCK_FD": "42"})
    @mock.patch("socket.fromfd", autospec=True)
    @mock.patch("os.getpid", autospec=True)
    def test_no_sock(self, getpid, fromfd):
        getpid.return_value = 1337

        server.einhorn_ack_startup()

        self.assertEqual(fromfd.call_args,
            mock.call(42, socket.AF_INET, socket.SOCK_STREAM))
        mocket = fromfd.return_value

        self.assertEqual(mocket.sendall.call_args,
            mock.call(b'{"command": "worker:ack", "pid": 1337}\n'))
        self.assertEqual(mocket.close.call_count, 1)
