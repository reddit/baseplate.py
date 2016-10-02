from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import threading

from ..core import BaseplateObserver


class LoggingBaseplateObserver(BaseplateObserver):
    """Logging observer.

    This observer adds request context to the thread-local state so that the
    log formatters can give more informative logs. Currently, this just sets
    the thread name to the current request's trace ID.

    """
    def on_server_span_created(self, context, server_span):  # pragma: nocover
        threading.current_thread().name = str(server_span.trace_id)
