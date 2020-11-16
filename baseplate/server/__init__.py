"""The baseplate server.

This command serves your application from the given configuration file.

"""
import argparse
import configparser
import fcntl
import gc
import importlib
import inspect
import logging.config
import os
import signal
import socket
import sys
import threading
import traceback
import warnings

from types import FrameType
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import TextIO
from typing import Tuple

from gevent.server import StreamServer

from baseplate import Baseplate
from baseplate.lib.config import Endpoint
from baseplate.lib.config import EndpointConfiguration
from baseplate.lib.log_formatter import CustomJsonFormatter
from baseplate.server import einhorn
from baseplate.server import reloader


logger = logging.getLogger(__name__)


def parse_args(args: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--debug", action="store_true", default=False, help="enable extra-verbose debug logging"
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        default=False,
        help="restart the server when code changes (development only)",
    )
    parser.add_argument(
        "--app-name",
        default="main",
        metavar="NAME",
        help="name of app to load from config_file (default: main)",
    )
    parser.add_argument(
        "--server-name",
        default="main",
        metavar="NAME",
        help="name of server to load from config_file (default: main)",
    )
    parser.add_argument(
        "--bind",
        type=Endpoint,
        default=Endpoint("localhost:9090"),
        metavar="ENDPOINT",
        help="endpoint to bind to (ignored if under Einhorn)",
    )
    parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="path to a configuration file"
    )

    return parser.parse_args(args)


class Configuration(NamedTuple):
    filename: str
    server: Optional[Dict[str, str]]
    app: Dict[str, str]
    has_logging_options: bool
    shell: Optional[Dict[str, str]]


def read_config(config_file: TextIO, server_name: Optional[str], app_name: str) -> Configuration:
    # we use RawConfigParser to reduce surprise caused by interpolation and so
    # that config.Percent works more naturally (no escaping %).
    parser = configparser.RawConfigParser()
    parser.read_file(config_file)

    filename = config_file.name
    server_config = dict(parser.items("server:" + server_name)) if server_name else None
    app_config = dict(parser.items("app:" + app_name))
    has_logging_config = parser.has_section("loggers")
    shell_config = None
    if parser.has_section("shell"):
        shell_config = dict(parser.items("shell"))
    elif parser.has_section("tshell"):
        shell_config = dict(parser.items("tshell"))

    return Configuration(filename, server_config, app_config, has_logging_config, shell_config)


def configure_logging(config: Configuration, debug: bool) -> None:
    logging.captureWarnings(capture=True)

    if debug:
        logging_level = logging.DEBUG
        warnings.simplefilter("always")  # enable DeprecationWarning etc.
    else:
        logging_level = logging.INFO

    formatter = CustomJsonFormatter(
        "%(levelname)s %(message)s %(funcName)s %(lineno)d %(module)s %(name)s %(pathname)s %(process)d %(processName)s %(thread)d %(threadName)s"
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)
    root_logger.addHandler(handler)

    sentry_logger = logging.getLogger("raven.base.Client")
    sentry_logger.setLevel(logging.WARNING)

    if config.has_logging_options:
        logging.config.fileConfig(config.filename)


def make_listener(endpoint: EndpointConfiguration) -> socket.socket:
    try:
        return einhorn.get_socket()
    except (einhorn.NotEinhornWorker, IndexError):
        # we're not under einhorn or it didn't bind any sockets for us
        pass

    sock = socket.socket(endpoint.family, socket.SOCK_STREAM)

    # configure the socket to be auto-closed if we exec() e.g. on reload
    flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFD)
    fcntl.fcntl(sock.fileno(), fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # on linux, SO_REUSEPORT is supported for IPv4 and IPv6 but not other
    # families. we prefer it when available because it more evenly spreads load
    # among multiple processes sharing a port. (though it does have some
    # downsides, specifically regarding behaviour during restarts)
    if endpoint.family in (socket.AF_INET, socket.AF_INET6):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    sock.bind(endpoint.address)
    sock.listen(128)
    return sock


def _load_factory(url: str, default_name: Optional[str] = None) -> Callable:
    """Load a factory function from a config file."""
    module_name, sep, func_name = url.partition(":")
    if not sep:
        if not default_name:
            raise ValueError("no name and no default specified")
        func_name = default_name
    module = importlib.import_module(module_name)
    factory = getattr(module, func_name)
    return factory


def make_server(
    server_config: Dict[str, str], listener: socket.socket, app: Callable
) -> StreamServer:
    server_url = server_config["factory"]
    factory = _load_factory(server_url, default_name="make_server")
    return factory(server_config, listener, app)


def make_app(app_config: Dict[str, str]) -> Callable:
    app_url = app_config["factory"]
    factory = _load_factory(app_url, default_name="make_app")
    return factory(app_config)


def register_signal_handlers() -> threading.Event:
    def _handle_debug_signal(_signo: int, frame: FrameType) -> None:
        if not frame:
            logger.warning("Received SIGUSR1, but no frame found.")
            return

        lines = traceback.format_stack(frame)
        logger.warning("Received SIGUSR1, dumping stack trace:")
        for line in lines:
            logger.warning(line.rstrip("\n"))

    signal.signal(signal.SIGUSR1, _handle_debug_signal)
    signal.siginterrupt(signal.SIGUSR1, False)

    shutdown_event = threading.Event()

    def _handle_shutdown_signal(_signo: int, _frame: FrameType) -> None:
        shutdown_event.set()

    # shutdown is signalled differently in different contexts:
    # - SIGINT  - Ctrl-C on the command line
    # - SIGTERM - Kubernetes
    # - SIGUSR2 - Einhorn
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGUSR2):
        signal.signal(sig, _handle_shutdown_signal)
        signal.siginterrupt(sig, False)
    return shutdown_event


def load_app_and_run_server() -> None:
    """Parse arguments, read configuration, and start the server."""

    sys.path.append(os.getcwd())

    shutdown_event = register_signal_handlers()

    args = parse_args(sys.argv[1:])
    with args.config_file:
        config = read_config(args.config_file, args.server_name, args.app_name)
    assert config.server

    configure_logging(config, args.debug)

    app = make_app(config.app)
    listener = make_listener(args.bind)
    server = make_server(config.server, listener, app)

    if einhorn.is_worker():
        einhorn.ack_startup()

    if args.reload:
        reloader.start_reload_watcher(extra_files=[args.config_file.name])

    # clean up leftovers from initialization before we get into requests
    gc.collect()

    logger.info("Listening on %s, PID:%s", listener.getsockname(), os.getpid())
    server.start()
    try:
        shutdown_event.wait()
        logger.info("Finally stopping server, PID:%s", os.getpid())
    finally:
        server.stop()


def load_and_run_script() -> None:
    """Launch a script with an entrypoint similar to a server."""

    sys.path.append(os.getcwd())

    args, extra_args = _parse_baseplate_script_args()
    with args.config_file:
        config = read_config(args.config_file, server_name=None, app_name=args.app_name)
    configure_logging(config, args.debug)

    if _fn_accepts_additional_args(args.entrypoint, extra_args):
        args.entrypoint(config.app, extra_args)
    else:
        args.entrypoint(config.app)


def _parse_baseplate_script_args() -> Tuple[argparse.Namespace, List[str]]:
    parser = argparse.ArgumentParser(
        description="Run a function with app configuration loaded.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--debug", action="store_true", default=False, help="enable extra-verbose debug logging"
    )
    parser.add_argument(
        "--app-name",
        default="main",
        metavar="NAME",
        help="name of app to load from config_file (default: main)",
    )
    parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="path to a configuration file"
    )
    parser.add_argument(
        "entrypoint", type=_load_factory, help="function to call, e.g. module.path:fn_name"
    )
    return parser.parse_known_args(sys.argv[1:])


def _fn_accepts_additional_args(script_fn: Callable[..., Any], fn_args: Sequence[str]) -> bool:
    additional_args_provided = len(fn_args) > 0
    signature = inspect.signature(script_fn)

    positional_arg_count = 0
    allows_var_args = False
    for param in signature.parameters.values():
        if param.kind in {param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD}:
            positional_arg_count += 1
        elif param.kind == param.VAR_POSITIONAL:
            allows_var_args = True

    allows_additional_args = allows_var_args or positional_arg_count > 1

    if positional_arg_count < 1 and not allows_var_args:
        raise ValueError("script function accepts too few positional arguments")
    if positional_arg_count > 2:
        raise ValueError("script function accepts too many positional arguments")
    if additional_args_provided and not allows_additional_args:
        raise ValueError(
            "script function does not accept additional arguments, "
            "but additional arguments were provided"
        )

    return allows_additional_args


def load_and_run_shell() -> None:
    """Launch a shell for a thrift service."""

    sys.path.append(os.getcwd())

    parser = argparse.ArgumentParser(
        description="Open a shell for a Thrift service with app configuration loaded.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--debug", action="store_true", default=False, help="enable extra-verbose debug logging"
    )
    parser.add_argument(
        "--app-name",
        default="main",
        metavar="NAME",
        help="name of app to load from config_file (default: main)",
    )
    parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="path to a configuration file"
    )

    args = parser.parse_args(sys.argv[1:])
    with args.config_file:
        config = read_config(args.config_file, server_name=None, app_name=args.app_name)
    logging.basicConfig(level=logging.INFO)

    env: Dict[str, Any] = {}
    env_banner = {
        "app": "This project's app instance",
        "context": "The context for this shell instance's span",
    }

    app = make_app(config.app)
    env["app"] = app

    baseplate: Baseplate = app.baseplate  # type: ignore
    context = baseplate.make_context_object()
    span = baseplate.make_server_span(context, "shell")
    env["context"] = span.context

    if config.shell and "setup" in config.shell:
        setup = _load_factory(config.shell["setup"])
        setup(env, env_banner)

    # generate banner text
    banner = "Available Objects:\n"
    for var in sorted(env_banner.keys()):
        banner += "\n  {:<12} {}".format(var, env_banner[var])

    try:
        # try to use IPython if possible
        from IPython import start_ipython

        try:
            # IPython 5.x+
            from traitlets.config.loader import Config
        except ImportError:
            # IPython 4 and below
            from IPython import Config

        ipython_config = Config()
        ipython_config.TerminalInteractiveShell.banner2 = banner
        start_ipython(argv=[], user_ns=env, config=ipython_config)
        raise SystemExit
    except ImportError:
        import code

        newbanner = f"Baseplate Interactive Shell\nPython {sys.version}\n\n"
        banner = newbanner + banner

        # import this just for its side-effects (of enabling nice keyboard
        # movement while editing text)
        try:
            import readline

            del readline
        except ImportError:
            pass

        shell = code.InteractiveConsole(locals=env)
        shell.interact(banner)
