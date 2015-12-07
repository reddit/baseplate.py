from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

import gevent
from gevent.pool import Pool
from gevent.pywsgi import WSGIServer

try:
    from gevent.pywsgi import LoggingLogAdapter
except ImportError:
    # LoggingLogAdapter is from gevent 1.1+
    class LoggingLogAdapter(object):
        def __init__(self, logger, level):
            self._logger = logger
            self._level = level

        def write(self, msg):
            self._logger.log(self._level, msg)


logger = logging.getLogger(__name__)


def make_server(config, listener, app):
    """Make a Gevent server for WSGI apps."""
    max_concurrency = int(config.get("max_concurrency", 0)) or None
    stop_timeout = int(config.get("stop_timeout", 0))

    pool = Pool(size=max_concurrency)
    log = LoggingLogAdapter(logger, level=logging.DEBUG)

    kwargs = {}
    if gevent.version_info[:2] >= (1, 1):  # error_log is new in 1.1
        kwargs["error_log"] = LoggingLogAdapter(logger, level=logging.ERROR)

    server = WSGIServer(
        listener,
        application=app,
        spawn=pool,
        log=log,
        **kwargs
    )
    server.stop_timeout = stop_timeout
    return server
