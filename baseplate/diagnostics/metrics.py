from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ..core import BaseplateObserver, SpanObserver


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
        context.metrics = self.client.batch()
        observer = MetricsServerSpanObserver(context.metrics, "server." + server_span.name)
        server_span.register(observer)


class MetricsSpanObserver(SpanObserver):
    def __init__(self, batch, name):
        self.batch = batch
        self.timer = batch.timer(name)

    def on_start(self):
        self.timer.start()

    def on_finish(self, exc_info):
        self.timer.stop()


class MetricsServerSpanObserver(MetricsSpanObserver):
    def on_child_span_created(self, span):  # pragma: nocover
        observer = MetricsSpanObserver(self.batch, "clients." + span.name)
        span.register(observer)

    def on_finish(self, exc_info):
        super(MetricsServerSpanObserver, self).on_finish(exc_info)
        self.batch.flush()
