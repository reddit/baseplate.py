from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
#from __future__ import unicode_literals This breaks __all__ on PY2

import os
import sys

from . import config, metrics
from .core import Baseplate
from .diagnostics import tracing
from ._utils import warn_deprecated


def metrics_client_from_config(raw_config):
    """Configure and return a metrics client.

    This expects two configuration options:

    ``metrics.namespace``
        The root key to prefix all metrics in this application with.
    ``metrics.endpoint``
        A ``host:port`` pair, e.g. ``localhost:2014``. If an empty string, a
        client that discards all metrics will be returned.

    :param dict raw_config: The application configuration which should have
        settings for the metrics client.
    :return: A configured client.
    :rtype: :py:class:`baseplate.metrics.Client`

    """
    cfg = config.parse_config(raw_config, {
        "metrics": {
            "namespace": config.String,
            "endpoint": config.Optional(config.Endpoint),
        },
    })

    # pylint: disable=maybe-no-member
    return metrics.make_client(cfg.metrics.namespace, cfg.metrics.endpoint)


def make_metrics_client(raw_config):
    warn_deprecated("make_metrics_client is deprecated in favor of the more "
                    "consistently named metrics_client_from_config")
    return metrics_client_from_config(raw_config)


def tracing_client_from_config(raw_config, log_if_unconfigured=True):
    """Configure and return a tracing client.

    This expects one configuration option and can take many optional ones:

    ``tracing.service_name``
        The name for the service this observer is registered to.
    ``tracing.endpoint`` (optional)
        (Deprecated in favor of the sidecar model.) Destination to record span data.
    ``tracing.queue_name`` (optional)
        Name of POSIX queue where spans are recorded
    ``tracing.max_span_queue_size`` (optional)
        Span processing queue limit.
    ``tracing.num_span_workers`` (optional)
        Number of worker threads for span processing.
    ``tracing.span_batch_interval`` (optional)
        Wait time for span processing in seconds.
    ``tracing.num_conns`` (optional)
        Pool size for remote recorder connection pool.
    ``tracing.sample_rate`` (optional)
        Percentage of unsampled requests to record traces for (e.g. "37%")

    :param dict raw_config: The application configuration which should have
        settings for the tracing client.
    :param bool log_if_unconfigured: When the client is not configured, should
        trace spans be logged or discarded silently?
    :return: A configured client.
    :rtype: :py:class:`baseplate.diagnostics.tracing.TracingClient`

    """

    cfg = config.parse_config(raw_config, {
        "tracing": {
            "service_name": config.String,
            "endpoint": config.Optional(config.Endpoint),
            "queue_name": config.Optional(config.String),
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

    # pylint: disable=maybe-no-member
    return tracing.make_client(
        service_name=cfg.tracing.service_name,
        tracing_endpoint=cfg.tracing.endpoint,
        tracing_queue_name=cfg.tracing.queue_name,
        max_span_queue_size=cfg.tracing.max_span_queue_size,
        num_span_workers=cfg.tracing.num_span_workers,
        span_batch_interval=cfg.tracing.span_batch_interval.total_seconds(),
        num_conns=cfg.tracing.num_conns,
        sample_rate=cfg.tracing.sample_rate,
        log_if_unconfigured=log_if_unconfigured,
    )


def make_tracing_client(raw_config, log_if_unconfigured=True):
    warn_deprecated("make_tracing_client is deprecated in favor of the more "
                    "consistently named tracing_client_from_config")
    return tracing_client_from_config(raw_config, log_if_unconfigured)


def error_reporter_from_config(raw_config, module_name):
    """Configure and return a error reporter.

    This expects one configuration option and can take many optional ones:

    ``sentry.dsn``
        The DSN provided by Sentry. If blank, the reporter will discard events.
    ``sentry.site`` (optional)
        An arbitrary string to identify this client installation.
    ``sentry.environment`` (optional)
        The environment your application is running in.
    ``sentry.exclude_paths`` (optional)
        Comma-delimited list of module prefixes to ignore when discovering
        where an error came from.
    ``sentry.include_paths`` (optional)
        Comma-delimited list of paths to include for consideration when
        drilling down to an exception.
    ``sentry.ignore_exceptions`` (optional)
        Comma-delimited list of fully qualified names of exception classes
        (potentially with * globs) to not report.
    ``sentry.sample_rate`` (optional)
        Percentage of errors to report. (e.g. "37%")
    ``sentry.processors`` (optional)
        Comma-delimited list of fully qualified names of processor classes
        to apply to events before sending to Sentry.

    Example usage::

        error_reporter_from_config(app_config, __name__)

    :param dict raw_config: The application configuration which should have
        settings for the error reporter.
    :param str module_name: ``__name__`` of the root module of the application.
    :rtype: :py:class:`raven.Client`

    """
    import raven

    cfg = config.parse_config(raw_config, {
        "sentry": {
            "dsn": config.Optional(config.String, default=None),
            "site": config.Optional(config.String, default=None),
            "environment": config.Optional(config.String, default=None),
            "include_paths": config.Optional(config.String, default=None),
            "exclude_paths": config.Optional(config.String, default=None),
            "ignore_exceptions": config.Optional(
                config.TupleOf(config.String), default=[]),
            "sample_rate": config.Optional(config.Percent, default=1),
            "processors": config.Optional(
                config.TupleOf(config.String), default=[
                    "raven.processors.SanitizePasswordsProcessor",
                ],
            ),
        },
    })

    application_module = sys.modules[module_name]
    directory = os.path.dirname(application_module.__file__)
    release = None
    while directory != "/":
        try:
            release = raven.fetch_git_sha(directory)
        except raven.exceptions.InvalidGitRepository:
            directory = os.path.dirname(directory)
        else:
            break

    # pylint: disable=maybe-no-member
    return raven.Client(
        dsn=cfg.sentry.dsn,
        site=cfg.sentry.site,
        release=release,
        environment=cfg.sentry.environment,
        include_paths=cfg.sentry.include_paths,
        exclude_paths=cfg.sentry.exclude_paths,
        ignore_exceptions=cfg.sentry.ignore_exceptions,
        sample_rate=cfg.sentry.sample_rate,
        processors=cfg.sentry.processors,
    )


__all__ = [
    "error_reporter_from_config",
    "make_metrics_client",
    "make_tracing_client",
    "metrics_client_from_config",
    "tracing_client_from_config",
    "Baseplate",
]
