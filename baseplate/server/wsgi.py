from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

import gevent
from gevent.pool import Pool
from gevent.pywsgi import WSGIServer

from . import _load_factory
from baseplate import config
from baseplate.server.metrics import start_runtime_metrics_reporter

try:
    # pylint: disable=no-name-in-module
    from gevent.pywsgi import LoggingLogAdapter
except ImportError:
    # LoggingLogAdapter is from gevent 1.1+
    class LoggingLogAdapter(object):
        def __init__(self, logger_, level):
            self._logger = logger_
            self._level = level

        def write(self, msg):
            self._logger.log(self._level, msg)


logger = logging.getLogger(__name__)


def make_server(server_config, listener, app):
    """Make a gevent server for WSGI apps."""
    # pylint: disable=maybe-no-member
    cfg = config.parse_config(server_config, {
        "handler": config.Optional(config.String, default=None),
        "max_concurrency": config.Integer,
        "stop_timeout": config.Optional(config.Integer, default=0),
    })

    pool = Pool(size=cfg.max_concurrency)
    log = LoggingLogAdapter(logger, level=logging.DEBUG)

    kwargs = {}
    if gevent.version_info[:2] >= (1, 1):  # error_log is new in 1.1
        kwargs["error_log"] = LoggingLogAdapter(logger, level=logging.ERROR)

    if cfg.handler:
        kwargs["handler_class"] = _load_factory(cfg.handler, default_name=None)

    # pylint: disable=star-args
    server = WSGIServer(
        listener,
        application=app,
        spawn=pool,
        log=log,
        **kwargs
    )
    server.stop_timeout = cfg.stop_timeout

    start_runtime_metrics_reporter(app, pool)
    return server
