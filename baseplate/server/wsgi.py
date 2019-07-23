import logging
import socket

from typing import Any
from typing import Dict

import gevent

from gevent.pool import Pool
from gevent.pywsgi import WSGIServer
from gevent.server import StreamServer

from baseplate.lib import config
from baseplate.server import _load_factory
from baseplate.server import runtime_monitor

try:
    # pylint: disable=no-name-in-module,ungrouped-imports
    from gevent.pywsgi import LoggingLogAdapter  # type: ignore
except ImportError:
    # LoggingLogAdapter is from gevent 1.1+
    class LoggingLogAdapter:  # type: ignore
        def __init__(self, logger_: logging.Logger, level: int):
            self._logger = logger_
            self._level = level

        def write(self, msg: str) -> None:
            self._logger.log(self._level, msg)


logger = logging.getLogger(__name__)


def make_server(server_config: Dict[str, str], listener: socket.socket, app: Any) -> StreamServer:
    """Make a gevent server for WSGI apps."""
    # pylint: disable=maybe-no-member
    cfg = config.parse_config(
        server_config,
        {
            "handler": config.Optional(config.String, default=None),
            "max_concurrency": config.Integer,
            "stop_timeout": config.Optional(config.Integer, default=0),
        },
    )

    pool = Pool(size=cfg.max_concurrency)
    log = LoggingLogAdapter(logger, level=logging.DEBUG)

    kwargs: Dict[str, Any] = {}
    if gevent.version_info[:2] >= (1, 1):  # error_log is new in 1.1
        kwargs["error_log"] = LoggingLogAdapter(logger, level=logging.ERROR)

    if cfg.handler:
        kwargs["handler_class"] = _load_factory(cfg.handler, default_name=None)

    server = WSGIServer(listener, application=app, spawn=pool, log=log, **kwargs)
    server.stop_timeout = cfg.stop_timeout

    runtime_monitor.start(server_config, app, pool)
    return server
