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
import signal
import socket
import sys
import traceback

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
    """Inspect the environment and make an appropriate listening socket.

    If the server is running under Einhorn, we'll use the bound socket it
    provides otherwise we'll do a best effort to bind one ourselves.

    Einhorn can bind multiple sockets (via multiple -b arguments) but we are
    only going to care about one because we're not smart enough to use multiple
    listeners. When sockets are bound, Einhorn provides several environment
    variables to child worker processes:

    - EINHORN_FD_COUNT: the number of sockets bound
    - EINHORN_FD_#: for each socket bound, the file descriptor for that socket
    - EINHORN_FD_FAMILY_#: for each socket bound, the protocol family of that
        socket (this is a recent addition, so if it's not present default to
        AF_INET)

    """
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
    """Helper to load a factory function from a config file."""
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
    """Send acknowledgement that we started up to the Einhorn master.

    If running under Einhorn, this will inform the master that this worker
    process has successfully started up.

    """
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


def register_signal_handlers():
    def _handle_debug_signal(signal_number, frame):
        if not frame:
            logger.warning("Received SIGUSR1, but no frame found.")
            return

        lines = traceback.format_stack(frame)
        logger.warning("Received SIGUSR1, dumping stack trace:")
        for line in lines:
            logger.warning(line.rstrip("\n"))

    signal.signal(signal.SIGUSR1, _handle_debug_signal)
    signal.siginterrupt(signal.SIGUSR1, False)


def load_app_and_run_server():
    """Parse arguments, read configuration, and start the server."""

    register_signal_handlers()

    args = parse_args(sys.argv[1:])
    config = read_config(args.config_file, args.server_name, args.app_name)
    configure_logging(args.debug)

    app = make_app(config.app)
    listener = make_listener(args.bind)
    server = make_server(config.server, listener, app)
    einhorn_ack_startup()

    logger.info("Listening on %s", listener.getsockname())

    server.serve_forever()
