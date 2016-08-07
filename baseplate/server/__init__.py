"""The baseplate server.

This command serves your application from the given configuration file.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import collections
import fcntl
import importlib
import logging
import logging.config
import signal
import socket
import sys
import traceback
import warnings

from . import einhorn, reloader
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
    parser.add_argument("--reload", action="store_true", default=False,
        help="restart the server when code changes (development only)")
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


Configuration = collections.namedtuple(
    "Configuration", ["filename", "server", "app", "has_logging_options"])


def read_config(config_file, server_name, app_name):
    parser = configparser.SafeConfigParser()
    parser.readfp(config_file)

    filename = config_file.name
    server_config = (dict(parser.items("server:" + server_name))
                     if server_name else None)
    app_config = dict(parser.items("app:" + app_name))
    has_logging_config = parser.has_section("loggers")

    return Configuration(
        filename,
        server_config,
        app_config,
        has_logging_config,
    )


def configure_logging(config, debug):
    logging.captureWarnings(capture=True)

    if debug:
        logging_level = logging.DEBUG
        warnings.simplefilter('always')  # enable DeprecationWarning etc.
    else:
        logging_level = logging.INFO

    formatter = logging.Formatter(
        "%(process)s:%(threadName)s:%(name)s:%(levelname)s:%(message)s")

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)
    root_logger.addHandler(handler)

    if config.has_logging_options:
        logging.config.fileConfig(config.filename)


def make_listener(endpoint):
    if einhorn.is_worker():
        return einhorn.get_socket()
    else:
        sock = socket.socket(endpoint.family, socket.SOCK_STREAM)

        # configure the socket to be auto-closed if we exec() e.g. on reload
        flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFD)
        fcntl.fcntl(sock.fileno(), fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(endpoint.address)
        sock.listen(128)
        return sock


def _load_factory(url, default_name=None):
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


def register_signal_handlers():
    def _handle_debug_signal(_, frame):
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
    configure_logging(config, args.debug)

    app = make_app(config.app)
    listener = make_listener(args.bind)
    server = make_server(config.server, listener, app)

    if einhorn.is_worker():
        einhorn.ack_startup()

    if args.reload:
        reloader.start_reload_watcher(extra_files=[args.config_file.name])

    logger.info("Listening on %s", listener.getsockname())

    server.serve_forever()


def load_and_run_script():
    """Launch a script with an entrypoint similar to a server."""
    parser = argparse.ArgumentParser(
        description="Run a function with app configuration loaded.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--debug", action="store_true", default=False,
        help="enable extra-verbose debug logging")
    parser.add_argument("--app-name", default="main", metavar="NAME",
        help="name of app to load from config_file (default: main)")
    parser.add_argument("config_file", type=argparse.FileType("r"),
        help="path to a configuration file")
    parser.add_argument("entrypoint", type=_load_factory,
        help="function to call, e.g. module.path:fn_name")

    args = parser.parse_args(sys.argv[1:])
    config = read_config(args.config_file, server_name=None, app_name=args.app_name)
    configure_logging(config, args.debug)
    args.entrypoint(config.app)
