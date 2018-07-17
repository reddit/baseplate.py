from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ..core import (
    BaseplateObserver,
    LocalSpan,
    SpanObserver,
)


class MetricsBaseplateObserver(BaseplateObserver):
    """Metrics collecting observer.

    This observer reports metrics to statsd. It does two important things:

    * it tracks the time taken in serving each request.
    * it batches all metrics generated during a request into as few packets
      as possible.

    The batch is accessible to your application during requests as the
    ``metrics`` attribute on the :term:`context object`.

    :param baseplate.metrics.Client client: The client where metrics will be
        sent.

    """
    def __init__(self, client):
        self.client = client

    def on_server_span_created(self, context, server_span):
        batch = self.client.batch()
        context.metrics = batch
        observer = MetricsServerSpanObserver(batch, server_span)
        server_span.register(observer)


class MetricsServerSpanObserver(SpanObserver):
    def __init__(self, batch, server_span):
        self.batch = batch
        self.base_name = "server." + server_span.name
        self.timer = batch.timer(self.base_name)

    def on_start(self):
        self.timer.start()

    def on_finish(self, exc_info):
        self.timer.stop()
        suffix = "success" if not exc_info else "failure"
        self.batch.counter(self.base_name + "." + suffix).increment()
        self.batch.flush()

    def on_child_span_created(self, span):
        if isinstance(span, LocalSpan):
            observer = MetricsLocalSpanObserver(self.batch, span)
        else:
            observer = MetricsClientSpanObserver(self.batch, span)
        span.register(observer)


class MetricsLocalSpanObserver(SpanObserver):
    def __init__(self, batch, span):
        self.timer = batch.timer(span.component_name + "." + span.name)

    def on_start(self):
        self.timer.start()

    def on_finish(self, exc_info):
        self.timer.stop()


class MetricsClientSpanObserver(SpanObserver):
    def __init__(self, batch, span):
        self.batch = batch
        self.base_name = "clients." + span.name
        self.timer = batch.timer(self.base_name)

    def on_start(self):
        self.timer.start()

    def on_finish(self, exc_info):
        self.timer.stop()
        suffix = "success" if not exc_info else "failure"
        self.batch.counter(self.base_name + "." + suffix).increment()

    def on_log(self, name, payload):
        if name == "error.object":
            self.batch.counter("errors.%s" % payload.__class__.__name__).increment()
