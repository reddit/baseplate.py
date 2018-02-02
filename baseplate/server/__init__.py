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
import os
from paste.deploy import loadapp
import paste.registry
import pylons
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
    "Configuration", ["filename", "server", "app", "has_logging_options"])


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


def load_app_and_run_shell():
    """Launch a shell."""
    # modified from r2.commands:ShellCommand, but with support for thrift
    # services
    parser = argparse.ArgumentParser(
        description="Open a shell with app configuration loaded.",
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
    configure_logging(config, args.debug)

    locs = dict(__name__="pylons-admin")

    is_http_service = pylons.config['pylons.package'] is not None
    if is_http_service:
        banner = _configure_http_service_shell(args.config_file.name, locs)
    else:
        banner = _configure_thrift_service_shell(config, locs)

    _run_shell(sys.argv[1:], banner, locs)


def _configure_thrift_service_shell(config, locs):
    app = make_app(config.app)
    locs['app'] = app

    baseplate = app._event_handler.baseplate
    span = baseplate.make_server_span(RequestContext(), 'shell')
    locs['context'] = span.context
    banner = "Available Objects:\n"
    banner += "  %-10s -  %s\n" % ('app',
        "This project's app instance")
    banner += "  %-10s -  %s\n" % ('context',
        "The context for this shell instance's span")
    return banner

def _configure_http_service_shell(config_name, locs):
    here_dir = os.getcwd()

    # Load locals and populate with objects for use in shell
    sys.path.insert(0, here_dir)

    # Load the wsgi app first so that everything is initialized right
    wsgiapp = loadapp('config:{}'.format(config_name), relative_to=here_dir)
    test_app = paste.fixture.TestApp(wsgiapp)

    # Query the test app to setup the environment
    tresponse = test_app.get('/_test_vars')
    request_id = int(tresponse.body)

    # Disable restoration during test_app requests
    test_app.pre_request_hook = lambda self: \
        paste.registry.restorer.restoration_end()
    test_app.post_request_hook = lambda self: \
        paste.registry.restorer.restoration_begin(request_id)

    # Restore the state of the Pylons special objects
    # (StackedObjectProxies)
    paste.registry.restorer.restoration_begin(request_id)

    # Determine the package name from the pylons.config object
    pkg_name = pylons.config['pylons.package']

    # Start the rest of our imports now that the app is loaded
    if is_minimal_template(pkg_name, True):
        model_module = None
        helpers_module = pkg_name + '.helpers'
        base_module = pkg_name + '.controllers'
    else:
        model_module = pkg_name + '.model'
        helpers_module = pkg_name + '.lib.helpers'
        base_module = pkg_name + '.lib.base'

    if model_module and can_import(model_module):
        locs['model'] = sys.modules[model_module]

    if can_import(helpers_module):
        locs['h'] = sys.modules[helpers_module]

    exec ('from pylons import app_globals, config, request, response, '
          'session, tmpl_context, url') in locs
    exec ('from pylons.controllers.util import abort, redirect') in locs
    exec 'from pylons.i18n import _, ungettext, N_' in locs
    locs.pop('__builtins__', None)

    # Import all objects from the base module
    __import__(base_module)

    base = sys.modules[base_module]
    base_public = [__name for __name in dir(base) if not \
                   __name.startswith('_') or __name == '_']
    locs.update((name, getattr(base, name)) for name in base_public)
    locs.update(dict(wsgiapp=wsgiapp, app=test_app))

    mapper = tresponse.config.get('routes.map')
    if mapper:
        locs['mapper'] = mapper

    make_server_span('reddit-shell')

    banner = "  All objects from %s are available\n" % base_module
    banner += "  Additional Objects:\n"
    if mapper:
        banner += "  %-10s -  %s\n" % ('mapper', 'Routes mapper object')
    banner += "  %-10s -  %s\n" % ('wsgiapp',
        "This project's WSGI App instance")
    banner += "  %-10s -  %s\n" % ('app',
        'paste.fixture wrapped around wsgiapp')

    return banner


def _run_shell(args, banner, locs):
    try:
        # try to use IPython if possible
        try:
            try:
                # 1.0 <= ipython
                from IPython.terminal.embed import InteractiveShellEmbed
            except ImportError:
                # 0.11 <= ipython < 1.0
                from IPython.frontend.terminal.embed import InteractiveShellEmbed
            shell = InteractiveShellEmbed(banner2=banner)
        except ImportError:
            # ipython < 0.11
            from IPython.Shell import IPShellEmbed
            shell = IPShellEmbed(argv=args)
            shell.set_banner(shell.IP.BANNER + '\n\n' + banner)

        try:
            shell(local_ns=locs, global_ns={})
        finally:
            paste.registry.restorer.restoration_end()
    except ImportError:
        import code
        py_prefix = sys.platform.startswith('java') and 'J' or 'P'
        newbanner = "Pylons Interactive Shell\n%sython %s\n\n" % \
            (py_prefix, sys.version)
        banner = newbanner + banner
        shell = code.InteractiveConsole(locals=locs)
        try:
            import readline
        except ImportError:
            pass
        try:
            shell.interact(banner)
        finally:
            paste.registry.restorer.restoration_end()
