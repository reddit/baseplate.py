"""The baseplate server.

This command serves your application from the given configuration file.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import collections
import importlib
import json
import logging
import os
import socket
import sys

from .._compat import configparser
from ..config import Endpoint


logger = logging.getLogger(__name__)


def parse_args(args):
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--debug", action="store_true", default=False,
        help="enable extra-verbose debug logging")
    parser.add_argument("--app-name", default="main", metavar="NAME",
        help="name of app to load from config_file (default: main)")
    parser.add_argument("--server-name", default="main", metavar="NAME",
        help="name of server to load from config_file (default: main)")
    parser.add_argument("--bind", type=Endpoint,
        default=Endpoint("localhost:9090"), metavar="ENDPOINT",
        help="endpoint to bind to (ignored if under Einhorn)")
    parser.add_argument("config_file", type=argparse.FileType("r"),
        help="path to a configuration file")

    return parser.parse_args(args)


Configuration = collections.namedtuple("Configuration", ["server", "app"])


def read_config(config_file, server_name, app_name):
    parser = configparser.SafeConfigParser()
    parser.readfp(config_file)
    server_config = dict(parser.items("server:" + server_name))
    app_config = dict(parser.items("app:" + app_name))
    return Configuration(server_config, app_config)


def configure_logging(debug):
    if debug:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO

    formatter = logging.Formatter(
        "%(process)s:%(threadName)s:%(name)s:%(levelname)s:%(message)s")

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)
    root_logger.addHandler(handler)


def make_listener(endpoint):
    try:
        fd_count = int(os.environ["EINHORN_FD_COUNT"])
    except KeyError:
        sock = socket.socket(endpoint.family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(endpoint.address)
        sock.listen(128)
        return sock
    else:
        assert fd_count > 0, "Running under Einhorn  but no sockets were bound."
        fileno = int(os.environ["EINHORN_FD_0"])
        family = getattr(socket, os.environ.get("EINHORN_FD_FAMILY_0", "AF_INET"))
        return socket.fromfd(fileno, family, socket.SOCK_STREAM)


def _load_factory(url, default_name):
    module_name, sep, func_name = url.partition(":")
    if not sep:
        if not default_name:
            raise ValueError("no name and no default specified")
        func_name = default_name
    module = importlib.import_module(module_name)
    factory = getattr(module, func_name)
    return factory


def make_server(server_config, listener, app):
    server_url = server_config["factory"]
    factory = _load_factory(server_url, default_name="make_server")
    return factory(server_config, listener, app)


def make_app(app_config):
    app_url = app_config["factory"]
    factory = _load_factory(app_url, default_name="make_app")
    return factory(app_config)


def einhorn_ack_startup():
    try:
        control_sock_fd = int(os.environ["EINHORN_SOCK_FD"])
    except (KeyError, ValueError):
        return

    control_sock = socket.fromfd(
        control_sock_fd, socket.AF_INET, socket.SOCK_STREAM)

    control_sock.sendall((json.dumps({
        "command": "worker:ack",
        "pid": os.getpid(),
    }, sort_keys=True) + "\n").encode("utf-8"))

    control_sock.close()


def load_app_and_run_server():
    args = parse_args(sys.argv[1:])
    config = read_config(args.config_file, args.server_name, args.app_name)
    configure_logging(args.debug)

    app = make_app(config.app)
    listener = make_listener(args.bind)
    server = make_server(config.server, listener, app)
    einhorn_ack_startup()

    logger.info("Listening on %s", listener.getsockname())

    server.serve_forever()
