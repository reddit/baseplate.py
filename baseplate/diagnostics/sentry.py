from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ..core import BaseplateObserver, ServerSpanObserver


class SentryBaseplateObserver(BaseplateObserver):
    """Error reporting observer.

    This observer reports unexpected exceptions to Sentry.

    The raven client is accessible to your application during requests as the
    ``sentry`` attribute on the :term:`context object`.

    :param raven.Client client: A configured raven client.

    """
    def __init__(self, raven):
        self.raven = raven

    def on_server_span_created(self, context, server_span):
        observer = SentryServerSpanObserver(self.raven, server_span)
        server_span.register(observer)
        context.sentry = self.raven


class SentryServerSpanObserver(ServerSpanObserver):
    def __init__(self, raven, server_span):
        self.raven = raven
        self.server_span = server_span

    def on_start(self):
        self.raven.context.activate()

        # for now, this is just a tag for us humans to use
        # https://github.com/getsentry/sentry/issues/716
        self.raven.tags_context({"trace_id": self.server_span.trace_id})

    def on_set_tag(self, key, value):
        if key.startswith("http"):
            self.raven.http_context({key[len("http."):]: value})
        else:
            self.raven.tags_context({key: value})

    def on_log(self, name, payload):
        self.raven.captureBreadcrumb(category=name, data=payload)

    def on_finish(self, exc_info=None):
        if exc_info is not None:
            self.raven.captureException(exc_info=exc_info)
        self.raven.context.clear(deactivate=True)


class SentryUnhandledErrorReporter(object):
    """Hook into the Gevent hub and report errors outside request context."""

    def __init__(self, hub, raven):
        self.original_print_exception = getattr(hub, "print_exception")
        self.raven = raven

    def __call__(self, context, exc_type, value, tb):
        self.raven.captureException((exc_type, value, tb))
        self.original_print_exception(context, exc_type, value, tb)
