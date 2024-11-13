import threading

from baseplate import BaseplateObserver
from baseplate import RequestContext
from baseplate import Span


class LoggingBaseplateObserver(BaseplateObserver):
    """Logging observer.

    This observer adds request context to the thread-local state so that the
    log formatters can give more informative logs. Currently, this just sets
    the thread name to the current request's trace ID.

    """

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        threading.current_thread().name = str(server_span.trace_id)
