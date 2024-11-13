import io
import socket
import sys
import unittest

from unittest import mock

import pytest

from baseplate import server
from baseplate.lib import config


EXAMPLE_ENDPOINT = config.EndpointConfiguration(socket.AF_INET, ("127.0.0.1", 1234))


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
        args = server.parse_args(
            [
                "filename",
                "--debug",
                "--app-name",
                "app",
                "--server-name",
                "server",
                "--bind",
                "1.2.3.4:81",
            ]
        )
        self.assertTrue(args.debug)
        self.assertEqual(args.app_name, "app")
        self.assertEqual(args.server_name, "server")
        self.assertEqual(args.bind, config.EndpointConfiguration(socket.AF_INET, ("1.2.3.4", 81)))


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

        self.assertEqual(mocket.call_args, mock.call(socket.AF_INET, socket.SOCK_STREAM))
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


class CheckFnSignatureTests(unittest.TestCase):
    def test_no_args(self):
        def foo():
            pass

        with self.assertRaises(ValueError):
            server._fn_accepts_additional_args(foo, [])

    def test_var_args(self):
        def foo(*args):
            pass

        server._fn_accepts_additional_args(foo, [])
        server._fn_accepts_additional_args(foo, ["arg1"])
        server._fn_accepts_additional_args(foo, ["arg1", "arg2"])

    def test_config_arg_only(self):
        def foo(app_config):
            pass

        server._fn_accepts_additional_args(foo, [])
        with self.assertRaises(ValueError):
            server._fn_accepts_additional_args(foo, ["extra_arg"])

    def test_config_arg_with_var_args(self):
        def foo(app_config, *args):
            pass

        server._fn_accepts_additional_args(foo, [])
        server._fn_accepts_additional_args(foo, ["arg1"])
        server._fn_accepts_additional_args(foo, ["arg1", "arg2"])

    def test_additional_args(self):
        def foo(app_config, args):
            pass

        server._fn_accepts_additional_args(foo, [])
        server._fn_accepts_additional_args(foo, ["arg1"])
        server._fn_accepts_additional_args(foo, ["arg1", "arg2"])

    def test_additional_args_with_var_args(self):
        def foo(app_config, args, *extra):
            pass

        server._fn_accepts_additional_args(foo, [])
        server._fn_accepts_additional_args(foo, ["arg1"])
        server._fn_accepts_additional_args(foo, ["arg1", "arg2"])

    def test_kwargs(self):
        def foo(app_config, arg1, *, bar, **kwargs):
            pass

        server._fn_accepts_additional_args(foo, [])
        server._fn_accepts_additional_args(foo, ["arg1", "arg2", "arg3"])
        server._fn_accepts_additional_args(foo, ["arg1"])


class ParseBaseplateScriptArgs(unittest.TestCase):
    @mock.patch.object(sys, "argv", ["baseplate-script", "mock.ini", "package.module:callable"])
    @mock.patch("baseplate.server._load_factory")
    @mock.patch("builtins.open", mock.mock_open())
    def test_simple_call(self, _load_factory):
        args, extra_args = server._parse_baseplate_script_args()
        self.assertEqual(args.app_name, "main")
        self.assertEqual(extra_args, [])

    @mock.patch.object(
        sys, "argv", ["baseplate-script", "mock.ini", "package.module:callable", "--app-name", "ci"]
    )
    @mock.patch("baseplate.server._load_factory")
    @mock.patch("builtins.open", mock.mock_open())
    def test_specifying_app_name(self, _load_factory):
        args, extra_args = server._parse_baseplate_script_args()
        self.assertEqual(args.app_name, "ci")
        self.assertEqual(extra_args, [])

    @mock.patch.object(
        sys,
        "argv",
        ["baseplate-script", "mock.ini", "package.module:callable", "extra_arg1", "extra_arg2"],
    )
    @mock.patch("baseplate.server._load_factory")
    @mock.patch("builtins.open", mock.mock_open())
    def test_extra_args(self, _load_factory):
        args, extra_args = server._parse_baseplate_script_args()
        self.assertEqual(args.app_name, "main")
        self.assertEqual(extra_args, ["extra_arg1", "extra_arg2"])


@mock.patch.dict("os.environ", {"FOO_FROM_ENV": "environmental"})
@pytest.mark.parametrize(
    "config_text,expected",
    (
        ("", None),
        ("foo = bar", "bar"),
        ("foo = $FOO_FROM_ENV", "environmental"),
        ("foo = ${FOO_FROM_ENV}", "environmental"),
        ("foo = ${this:is:not:valid}", "${this:is:not:valid}"),
    ),
)
def test_read_config(config_text, expected):
    config_file = io.StringIO(f"[app:main]\n{config_text}\n")
    config_file.name = "<test>"
    config = server.read_config(config_file, server_name=None, app_name="main")
    assert config.app.get("foo") == expected
