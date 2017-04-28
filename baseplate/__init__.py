from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
#from __future__ import unicode_literals This breaks __all__ on PY2

from . import config, metrics
from .core import Baseplate
from .diagnostics import tracing


def make_metrics_client(raw_config):
    """Configure and return a metrics client.

    This expects two configuration options:

    ``metrics.namespace``
        The root key to namespace all metrics in this application under.
    ``metrics.endpoint``
        A ``host:port`` pair, e.g. ``localhost:2014``. If an empty string, a
        client that discards all metrics will be returned.

    :param dict raw_config: The app configuration which should have settings
        for the metrics client.
    :return: A configured client.
    :rtype: :py:class:`baseplate.metrics.Client`

    """
    cfg = config.parse_config(raw_config, {
        "metrics": {
            "namespace": config.String,
            "endpoint": config.Optional(config.Endpoint),
        },
    })

    # pylint: disable=no-member
    return metrics.make_client(cfg.metrics.namespace, cfg.metrics.endpoint)


def make_tracing_client(raw_config, log_if_unconfigured=True):
    """Configure and return a tracing client.

    This expects one configuration option and can take many optional ones:

    ``tracing.service_name``
        The name for the service this observer is registered to.
    ``tracing.endpoint`` (optional)
        Destination to record span data.
    ``tracing.max_span_queue_size`` (optional)
        Span processing queue limit.
    ``tracing.num_span_workers`` (optional)
        Number of worker threads for span processing.
    ``tracing.span_batch_interval`` (optional)
        Wait time for span processing in seconds.
    ``tracing.num_conns`` (optional)
        Pool size for remote recorder connection pool.
    ``tracing.sample_rate`` (optional)
        Percentage of unsampled requests to record traces for (e.g. 37%)

    :param dict raw_config: The app configuration which should have settings
        for the tracing client.
    :param bool log_if_unconfigured: When the client is not configured, should
        trace spans be logged or discarded silently?
    :return: A configured client.
    :rtype: :py:class:`baseplate.diagnostics.tracing.TracingClient`

    """

    cfg = config.parse_config(raw_config, {
        "tracing": {
            "service_name": config.String,
            "endpoint": config.Optional(config.Endpoint),
            "max_span_queue_size": config.Optional(
                config.Integer, default=50000),
            "num_span_workers": config.Optional(config.Integer, default=5),
            "span_batch_interval": config.Optional(
                config.Timespan, default=config.Timespan("500 milliseconds")),
            "num_conns": config.Optional(config.Integer, default=100),
            "sample_rate": config.Optional(
                config.Fallback(config.Percent, config.Float), default=0.1),
        },
    })

    # pylint: disable=no-member
    return tracing.make_client(
        service_name=cfg.tracing.service_name,
        tracing_endpoint=cfg.tracing.endpoint,
        max_span_queue_size=cfg.tracing.max_span_queue_size,
        num_span_workers=cfg.tracing.num_span_workers,
        span_batch_interval=cfg.tracing.span_batch_interval.total_seconds(),
        num_conns=cfg.tracing.num_conns,
        sample_rate=cfg.tracing.sample_rate,
        log_if_unconfigured=log_if_unconfigured,
    )


__all__ = [
    "make_metrics_client",
    "make_tracing_client",
    "Baseplate",
]
