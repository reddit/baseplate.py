"""The baseplate server.

This command serves your application from the given configuration file.

"""
import argparse
import code
import configparser
import enum
import gc
import importlib
import inspect
import logging.config
import os
import signal
import socket
import sys
import syslog
import threading
import time
import traceback
import warnings

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from rlcompleter import Completer
from types import FrameType
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import TextIO
from typing import Tuple

from gevent.server import StreamServer

from baseplate import Baseplate
from baseplate.lib import warn_deprecated
from baseplate.lib.config import Endpoint
from baseplate.lib.config import EndpointConfiguration
from baseplate.lib.config import Optional as OptionalConfig
from baseplate.lib.config import parse_config
from baseplate.lib.config import Timespan
from baseplate.lib.log_formatter import CustomJsonFormatter
from baseplate.server import einhorn
from baseplate.server import reloader
from baseplate.server.net import bind_socket


logger = logging.getLogger(__name__)


class ServerLifecycle(Enum):
    RUNNING = enum.auto()
    SHUTTING_DOWN = enum.auto()


@dataclass
class ServerState:
    state: ServerLifecycle = ServerLifecycle.RUNNING

    @property
    def shutting_down(self) -> bool:
        warn_deprecated("SERVER_STATE.shutting_down is deprecated in favor of SERVER_STATE.state")
        return self.state == ServerLifecycle.SHUTTING_DOWN


SERVER_STATE = ServerState()


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


class EnvironmentInterpolation(configparser.Interpolation):
    def before_get(
        self,
        parser: MutableMapping[str, Mapping[str, str]],
        section: str,
        option: str,
        value: str,
        defaults: Mapping[str, str],
    ) -> str:
        return os.path.expandvars(value)


class Configuration(NamedTuple):
    filename: str
    server: Optional[Dict[str, str]]
    app: Dict[str, str]
    has_logging_options: bool
    shell: Optional[Dict[str, str]]


def read_config(config_file: TextIO, server_name: Optional[str], app_name: str) -> Configuration:
    # we use RawConfigParser to reduce surprise caused by interpolation and so
    # that config.Percent works more naturally (no escaping %).
    parser = configparser.RawConfigParser(interpolation=EnvironmentInterpolation())
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

    formatter: logging.Formatter
    if not sys.stdin.isatty():
        formatter = CustomJsonFormatter(
            "%(levelname)s %(message)s %(funcName)s %(lineno)d %(module)s %(name)s %(pathname)s %(process)d %(processName)s %(thread)d %(threadName)s"
        )
    else:
        formatter = logging.Formatter("%(levelname)-8s %(message)s")

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)
    root_logger.addHandler(handler)

    # add PID 1 stdout logging if we're containerized and not running under an init system
    if _is_containerized() and not _has_PID1_parent():
        file_handler = logging.FileHandler("/proc/1/fd/1", mode="w")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if config.has_logging_options:
        logging.config.fileConfig(config.filename)


def make_listener(endpoint: EndpointConfiguration) -> socket.socket:
    try:
        return einhorn.get_socket()
    except (einhorn.NotEinhornWorker, IndexError):
        # we're not under einhorn or it didn't bind any sockets for us
        pass

    return bind_socket(endpoint)


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

    cfg = parse_config(config.server, {"drain_time": OptionalConfig(Timespan)})

    if einhorn.is_worker():
        einhorn.ack_startup()

    if "metrics.tagging" in config.app or "metrics.namespace" in config.app:
        from baseplate.server.prometheus import start_prometheus_exporter

        start_prometheus_exporter()
    else:
        logger.info("Metrics are not configured, Prometheus metrics will not be exported.")

    if args.reload:
        reloader.start_reload_watcher(extra_files=[args.config_file.name])

    # clean up leftovers from initialization before we get into requests
    gc.collect()

    logger.info("Listening on %s", listener.getsockname())
    server.start()
    try:
        shutdown_event.wait()

        SERVER_STATE.state = ServerLifecycle.SHUTTING_DOWN

        if cfg.drain_time:
            logger.debug("Draining inbound requests...")
            time.sleep(cfg.drain_time.total_seconds())
    finally:
        logger.debug("Gracefully shutting down...")
        server.stop()
        logger.info("Exiting")


def load_and_run_script() -> None:
    """Launch a script with an entrypoint similar to a server."""
    sys.path.append(os.getcwd())

    args, extra_args = _parse_baseplate_script_args()
    with args.config_file:
        config = read_config(args.config_file, server_name=None, app_name=args.app_name)
    configure_logging(config, args.debug)

    entrypoint = _load_factory(args.entrypoint)

    if _fn_accepts_additional_args(entrypoint, extra_args):
        entrypoint(config.app, extra_args)
    else:
        entrypoint(config.app)


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
    parser.add_argument("entrypoint", type=str, help="function to call, e.g. module.path:fn_name")
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

    configure_logging(config, args.debug)

    # generate banner text
    banner = "Available Objects:\n"
    for var in sorted(env_banner.keys()):
        banner += f"\n  {var:<12} {env_banner[var]}"

    console_logpath = _get_shell_log_path()

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
        ipython_config.InteractiveShellApp.exec_lines = [
            # monkeypatch IPython's log-write() to enable formatted input logging, copying original code:
            # https://github.com/ipython/ipython/blob/a54bf00feb5182fa821bd5457897b3b30a313436/IPython/core/logger.py#L187-L201
            f"""
            ip = get_ipython()
            from functools import partial
            def log_write(self, data, kind="input", message_id="IEXC"):
                import datetime, os
                if self.log_active and data:
                    write = self.logfile.write
                    if kind=='input':
                        # Generate an RFC 5424 compliant syslog format
                        write(f'<13>1 {{datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")}} {{os.uname().nodename}} baseplate-shell {{os.getpid()}} {{message_id}} - {{data}}')
                    elif kind=='output' and self.log_output:
                        odata = u'\\n'.join([u'#[Out]# %s' % s
                                        for s in data.splitlines()])
                        write(u'%s\\n' % odata)
                    self.logfile.flush()
            ip.logger.logstop = None
            ip.logger.log_write = partial(log_write, ip.logger)
            ip.magic('logstart {console_logpath} append')
            ip.logger.log_write(data="Start IPython logging\\n", message_id="ISTR")
            """
        ]
        ipython_config.TerminalInteractiveShell.banner2 = banner
        ipython_config.LoggingMagics.quiet = True
        start_ipython(argv=[], user_ns=env, config=ipython_config)
        raise SystemExit
    except ImportError:
        pass

    newbanner = f"Baseplate Interactive Shell\nPython {sys.version}\n\n"
    banner = newbanner + banner

    try:
        import readline

        readline.set_completer(Completer(env).complete)
        readline.parse_and_bind("tab: complete")

    except ImportError:
        pass

    shell = LoggedInteractiveConsole(_locals=env, logpath=console_logpath)
    shell.interact(banner)


def _get_shell_log_path() -> str:
    """Determine where to write shell audit logs."""
    if _is_containerized():
        # write to PID 1 stdout for log aggregation
        return "/proc/1/fd/1"
    # otherwise write to a local file
    return "/var/log/baseplate-shell.log"


def _is_containerized() -> bool:
    """Determine if we're running in a container based on cgroup awareness for various container runtimes."""
    if os.path.exists("/.dockerenv"):
        return True

    try:
        with open("/proc/self/cgroup", encoding="UTF-8") as my_cgroups_file:
            my_cgroups = my_cgroups_file.read()

            for hint in ["kubepods", "docker", "containerd"]:
                if hint in my_cgroups:
                    return True
    except OSError:
        pass

    return False


def _has_PID1_parent() -> bool:
    """Determine parent PIDs up the tree until PID 1 or 0 is reached, do this natively"""
    parent_pid = os.getppid()
    while parent_pid > 1:
        with open(f"/proc/{parent_pid}/status", encoding="UTF-8") as proc_status:
            for line in proc_status.readlines():
                if line.startswith("PPid:"):
                    parent_pid = int(line.replace("PPid:", ""))
                    break
    return bool(parent_pid)


class LoggedInteractiveConsole(code.InteractiveConsole):
    def __init__(self, _locals: Dict[str, Any], logpath: str) -> None:
        code.InteractiveConsole.__init__(self, _locals)
        self.output_file = logpath
        self.pid = os.getpid()
        self.pri = syslog.LOG_USER | syslog.LOG_NOTICE
        self.hostname = os.uname().nodename
        self.log_event(message="Start InteractiveConsole logging", message_id="CSTR")

    def raw_input(self, prompt: Optional[str] = "") -> str:
        data = input(prompt)
        self.log_event(message=data, message_id="CEXC")
        return data

    def log_event(
        self, message: str, message_id: Optional[str] = "-", structured: Optional[str] = "-"
    ) -> None:
        """Generate an RFC 5424 compliant syslog format."""
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        prompt = f"<{self.pri}>1 {timestamp} {self.hostname} baseplate-shell {self.pid} {message_id} {structured} {message}"
        with open(self.output_file, "w", encoding="UTF-8") as f:
            print(prompt, file=f)
            f.flush()
