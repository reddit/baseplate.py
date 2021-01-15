import datetime
import logging
import socket

from typing import Any
from typing import Dict

from gevent.pool import Pool
from gevent.pywsgi import LoggingLogAdapter
from gevent.pywsgi import WSGIServer
from gevent.server import StreamServer

from baseplate.lib import config
from baseplate.server import _load_factory
from baseplate.server import runtime_monitor


logger = logging.getLogger(__name__)


def make_server(server_config: Dict[str, str], listener: socket.socket, app: Any) -> StreamServer:
    """Make a gevent server for WSGI apps."""
    # pylint: disable=maybe-no-member
    cfg = config.parse_config(
        server_config,
        {
            "handler": config.Optional(config.String, default=None),
            "max_concurrency": config.Optional(config.Integer),
            "stop_timeout": config.Optional(
                config.TimespanWithLegacyFallback, default=datetime.timedelta(seconds=10)
            ),
        },
    )

    if cfg.max_concurrency is not None:
        raise Exception(
            "The max_concurrency setting is not allowed for WSGI servers. See https://git.io/Jeywc."
        )

    pool = Pool()
    log = LoggingLogAdapter(logger, level=logging.DEBUG)

    kwargs: Dict[str, Any] = {}
    if cfg.handler:
        kwargs["handler_class"] = _load_factory(cfg.handler, default_name=None)

    server = WSGIServer(
        listener,
        application=app,
        spawn=pool,
        log=log,
        error_log=LoggingLogAdapter(logger, level=logging.ERROR),
        **kwargs,
    )
    server.stop_timeout = cfg.stop_timeout.total_seconds()

    runtime_monitor.start(server_config, app, pool)
    return server
