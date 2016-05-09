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
    @mock.patch("baseplate.server.einhorn.get_socket")
    @mock.patch("baseplate.server.einhorn.is_worker")
    def test_einhorn_managed(self, is_worker, get_socket):
        is_worker.return_value = True

        listener = server.make_listener(EXAMPLE_ENDPOINT)

        self.assertEqual(listener, get_socket.return_value)

    @mock.patch.dict("os.environ", {}, clear=True)
    @mock.patch("fcntl.fcntl")
    @mock.patch("socket.socket")
    def test_manually_bound(self, mocket, fcntl):
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
