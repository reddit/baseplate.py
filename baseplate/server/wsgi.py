from __future__ import annotations

import datetime
import logging
import socket
from typing import Any, Literal

import gevent
from gevent.event import Event
from gevent.pool import Pool
from gevent.pywsgi import LoggingLogAdapter, WSGIHandler, WSGIServer
from gevent.server import StreamServer

from baseplate.lib import config
from baseplate.server import _load_factory, runtime_monitor

logger = logging.getLogger(__name__)


class BaseplateWSGIServer(WSGIServer):
    """WSGI server which closes existing keepalive connections when shutting down.

    The default gevent WSGIServer prevents new *connections* once the server
    enters shutdown, but does not prevent new *requests* over existing
    keepalive connections. This results in slow shutdowns and in some cases
    requests being killed mid-flight once the server reaches stop_timeout.

    This server may be used with any gevent WSGIHandler, but the keepalive
    behavior only works when using BaseplateWSGIHandler.
    """

    shutdown_event: Event

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.shutdown_event = Event()
        super().__init__(*args, **kwargs)

    def stop(self, *args: Any, **kwargs: Any) -> None:
        self.shutdown_event.set()
        super().stop(*args, **kwargs)


class BaseplateWSGIHandler(WSGIHandler):
    """WSGI handler which avoids processing requests when the server is in shutdown.

    This handler may only be used with BaseplateWSGIServer.
    """

    _shutdown_event: Event

    # Flag representing whether the base class thinks the connection should be
    # closed. The base class sets `self.close_connection` based on the HTTP
    # version and headers, which we intercept using a property setter into this
    # attribute.
    _close_connection: bool = False

    def __init__(
        self, sock: socket.socket, address: tuple[str, int], server: BaseplateWSGIServer
    ) -> None:
        self._shutdown_event = server.shutdown_event
        super().__init__(sock, address, server)

    @property
    def close_connection(self) -> bool:
        # This property overrides `close_connection` in the base class which is
        # used to control keepalive behavior.
        return self._close_connection or self._shutdown_event.is_set()

    @close_connection.setter
    def close_connection(self, value: bool) -> None:
        # This setter allows the base class to set `self.close_connection`
        # directly, while still allowing us to override the value when we know
        # the Baseplate server is in shutdown.
        self._close_connection = value

    def read_requestline(self) -> str | None:
        real_read_requestline = super().read_requestline

        # We can't let any exceptions (e.g. socket errors) raise to the top of
        # a greenlet because they will get reported as uncaught exceptions in
        # our Sentry observer, even though we handle the error. So, we catch
        # any exceptions and return a tuple (result, exception) instead.
        def wrapped_read_requestline() -> tuple[str | None, Exception | None]:
            try:
                return real_read_requestline(), None
            except Exception as ex:
                return None, ex

        read_requestline = gevent.spawn(wrapped_read_requestline)
        ready = gevent.wait([self._shutdown_event, read_requestline], count=1)

        if self._shutdown_event in ready:
            read_requestline.kill()
            read_requestline.join()
            # None triggers the base class to close the connection.
            return None

        result = read_requestline.get()

        if isinstance(result, BaseException):
            # This shouldn't normally happen, but can with e.g. GreenletExit if
            # the greenlet is killed.
            raise result

        ret, ex = result
        if ex:
            raise ex
        return ret

    def handle_one_request(
        self,
    ) -> (
        # 'None' is used to indicate that the connection should be closed by the caller.
        None
        # 'True' is used to indicate that the connection should be kept open for future requests.
        | Literal[True]
        # Tuple of status line and response body is used for returning an error response.
        | tuple[str, bytes]
    ):
        ret = super().handle_one_request()
        if ret is True and self._shutdown_event.is_set():
            return None
        return ret


def make_server(server_config: dict[str, str], listener: socket.socket, app: Any) -> StreamServer:
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
        raise ValueError(
            "The max_concurrency setting is not allowed for WSGI servers. See https://github.com/reddit/baseplate.py-upgrader/wiki/v1.2#max_concurrency-is-deprecated."
        )

    pool = Pool()
    log = LoggingLogAdapter(logger, level=logging.DEBUG)

    kwargs: dict[str, Any] = {
        "handler_class": BaseplateWSGIHandler,
    }
    if cfg.handler:
        kwargs["handler_class"] = _load_factory(cfg.handler, default_name=None)
        if not issubclass(kwargs["handler_class"], BaseplateWSGIHandler):
            logger.warning(
                "Custom handler %r is not a subclass of BaseplateWSGIHandler. "
                "This may prevent proper shutdown behavior.",
                cfg.handler,
            )

    server = BaseplateWSGIServer(
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
