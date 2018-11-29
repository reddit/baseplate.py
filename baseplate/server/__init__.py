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
from ..integration.thrift import RequestContext


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
    "Configuration", ["filename", "server", "app", "has_logging_options", "tshell"])


def read_config(config_file, server_name, app_name):
    # we use RawConfigParser to reduce surprise caused by interpolation and so
    # that config.Percent works more naturally (no escaping %).
    parser = configparser.RawConfigParser()
    parser.readfp(config_file)

    filename = config_file.name
    server_config = (dict(parser.items("server:" + server_name))
                     if server_name else None)
    app_config = dict(parser.items("app:" + app_name))
    has_logging_config = parser.has_section("loggers")
    tshell_config = None
    if parser.has_section("tshell"):
        tshell_config = dict(parser.items("tshell"))

    return Configuration(
        filename,
        server_config,
        app_config,
        has_logging_config,
        tshell_config,
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
    try:
        return einhorn.get_socket()
    except (einhorn.NotEinhornWorker, IndexError):
        # we're not under einhorn or it didn't bind any sockets for us
        pass

    sock = socket.socket(endpoint.family, socket.SOCK_STREAM)

    # configure the socket to be auto-closed if we exec() e.g. on reload
    flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFD)
    fcntl.fcntl(sock.fileno(), fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

    # on linux, SO_REUSEPORT is supported for IPv4 and IPv6 but not other
    # families. we prefer it when available because it more evenly spreads load
    # among multiple processes sharing a port. (though it does have some
    # downsides, specifically regarding behaviour during restarts)
    socket_options = socket.SO_REUSEADDR
    if endpoint.family in (socket.AF_INET, socket.AF_INET6):
        socket_options |= socket.SO_REUSEPORT
    sock.setsockopt(socket.SOL_SOCKET, socket_options, 1)

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


def load_and_run_tshell():
    """Launch a shell for a thrift service."""
    parser = argparse.ArgumentParser(
        description="Open a shell for a Thrift service with app configuration loaded.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--debug", action="store_true", default=False,
        help="enable extra-verbose debug logging")
    parser.add_argument("--app-name", default="main", metavar="NAME",
        help="name of app to load from config_file (default: main)")
    parser.add_argument("config_file", type=argparse.FileType("r"),
        help="path to a configuration file")

    args = parser.parse_args(sys.argv[1:])
    config = read_config(args.config_file, server_name=None, app_name=args.app_name)
    logging.basicConfig(level=logging.INFO)

    env = dict()
    env_banner = {
        'app': "This project's app instance",
        'context': "The context for this shell instance's span",
    }

    app = make_app(config.app)
    env['app'] = app

    span = app.baseplate.make_server_span(RequestContext(), 'shell')
    env['context'] = span.context

    if config.tshell and 'setup' in config.tshell:
        setup = _load_factory(config.tshell['setup'])
        setup(env, env_banner)

    # generate banner text
    banner = "Available Objects:\n"
    for var in sorted(env_banner.keys()):
        banner += '\n  %-12s %s' % (var, env_banner[var])

    try:
        # try to use IPython if possible
        from IPython.terminal.embed import InteractiveShellEmbed
        shell = InteractiveShellEmbed(banner2=banner)
        shell(local_ns=env, global_ns={})
    except ImportError:
        import code
        newbanner = "Baseplate Interactive Shell\nPython {}\n\n".format(sys.version)
        banner = newbanner + banner
        shell = code.InteractiveConsole(locals=env)
        shell.interact(banner)
