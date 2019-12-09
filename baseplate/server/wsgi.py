import datetime
import logging
import socket

from typing import Any
from typing import Dict

from gevent.pool import Pool
from gevent.pywsgi import LoggingLogAdapter
from gevent.pywsgi import WSGIHandler
from gevent.pywsgi import WSGIServer
from gevent.server import StreamServer

from baseplate.lib import config
from baseplate.server import _load_factory
from baseplate.server import runtime_monitor

logger = logging.getLogger(__name__)


class CircuitBreakingWSGIHandlerFactory(object):
    def __init__(self, max_concurrency):
        self.max_concurrency = max_concurrency
        self.open_requests = 0

    def create_handler(self, sock, address, server):
        return CircuitBreakingWSGIHandler(sock, address, server, self)


class CircuitBreakingWSGIHandler(WSGIHandler):
    def __init__(self, sock, address, server, factory):
        super(CircuitBreakingWSGIHandler, self).__init__(sock, address, server)
        self.factory = factory

    def run_application(self):
        if self.factory.open_requests < self.factory.max_concurrency:
            try:
                self.factory.open_requests += 1
                self.result = self.application(self.environ, self.start_response)
                self.process_result()
            finally:
                self.factory.open_requests -= 1
                close = getattr(self.result, "close", None)
                try:
                    if close is not None:
                        close()
                finally:
                    close = None
                    self.result = None
        else:
            status = "503 Service Unavailable"
            body = b"503 Service Unavailable (temporarily)"
            headers = [
                ("Content-Type", "text/plain"),
                ("Connection", "close"),
                ("Content-Length", str(len(body))),
            ]
            try:
                self.start_response(status, headers[:])
                self.write(body)
            except socket.error:
                if not PY3:
                    sys.exc_clear()
                self.close_connection = True


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

    pool = Pool(size=None)

    log = LoggingLogAdapter(logger, level=logging.DEBUG)
    kwargs: Dict[str, Any] = {}
    if cfg.handler:
        # pdb.set_trace()
        kwargs["handler_class"] = _load_factory(cfg.handler, default_name=None)
    elif cfg.max_concurrency:
        # pdb.set_trace()
        kwargs["handler_class"] = CircuitBreakingWSGIHandlerFactory(
            cfg.max_concurrency
        ).create_handler

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
