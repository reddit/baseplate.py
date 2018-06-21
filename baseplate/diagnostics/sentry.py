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
    def __init__(self, raven, ignore_exception_cls=None):
        self.raven = raven
        self.ignore_exception_cls = ignore_exception_cls

    def on_server_span_created(self, context, server_span):
        observer = SentryServerSpanObserver(
            self.raven,
            server_span,
            self.ignore_exception_cls,
        )
        server_span.register(observer)
        context.sentry = self.raven


class SentryServerSpanObserver(ServerSpanObserver):
    def __init__(self, raven, server_span, ignore_exception_cls=None):
        self.raven = raven
        self.server_span = server_span
        self.ignore_exception_cls = ignore_exception_cls

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
            _, exc, _ = exc_info
            if (self.ignore_exception_cls is None or
                    not isinstance(exc, self.ignore_exception_cls)):
                self.raven.captureException(exc_info=exc_info)
        self.raven.context.clear(deactivate=True)
