import logging
import os
import sys

from types import TracebackType
from typing import Any
from typing import Optional
from typing import Type
from typing import TYPE_CHECKING

import raven

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import RequestContext
from baseplate import ServerSpanObserver
from baseplate import Span
from baseplate.lib import config
from baseplate.lib import warn_deprecated
from baseplate.observers.timeout import ServerTimeout

if TYPE_CHECKING:
    from gevent.hub import Hub


ALWAYS_IGNORE_EXCEPTIONS = (
    "ConnectionError",
    "ConnectionRefusedError",
    "ConnectionResetError",
    "HTTPError",
    "TApplicationException",
    "TProtocolException",
    "TTransportException",
    "MemcacheServerError",
)


def error_reporter_from_config(raw_config: config.RawConfig, module_name: str) -> raven.Client:
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

    :param raw_config: The application configuration which should have
        settings for the error reporter.
    :param module_name: ``__name__`` of the root module of the application.

    """
    cfg = config.parse_config(
        raw_config,
        {
            "sentry": {
                "dsn": config.Optional(config.String, default=None),
                "site": config.Optional(config.String, default=None),
                "environment": config.Optional(config.String, default=None),
                "include_paths": config.Optional(config.String, default=None),
                "exclude_paths": config.Optional(config.String, default=None),
                "ignore_exceptions": config.Optional(
                    config.TupleOf(config.String), default=[]
                ),  # Deprecated in favor of `additional_ignore_exception
                "additional_ignore_exceptions": config.Optional(
                    config.TupleOf(config.String), default=[]
                ),
                "sample_rate": config.Optional(config.Percent, default=1),
                "processors": config.Optional(
                    config.TupleOf(config.String),
                    default=["raven.processors.SanitizePasswordsProcessor"],
                ),
            }
        },
    )

    application_module = sys.modules[module_name]
    module_path = os.path.abspath(application_module.__file__)
    directory = os.path.dirname(module_path)
    release = None
    while directory != "/":
        try:
            release = raven.fetch_git_sha(directory)
        except raven.exceptions.InvalidGitRepository:
            directory = os.path.dirname(directory)
        else:
            break

    cfg_ignore_exceptions = cfg.sentry.ignore_exceptions
    cfg_additional_ignore_exceptions = cfg.sentry.additional_ignore_exceptions

    if cfg_ignore_exceptions:
        warn_deprecated(
            "'sentry.ignore_exceptions' is deprecated. "
            "Please use 'sentry.additional_ignore_exceptions' instead.",
        )

    if cfg_additional_ignore_exceptions and cfg_ignore_exceptions:
        raise config.ConfigurationError(
            "sentry.ignore_exceptions",
            "Can not define 'sentry.ignore_exceptions' and 'sentry.additional_ignore_exceptions'",
        )

    all_ignore_exceptions = cfg_ignore_exceptions or list(ALWAYS_IGNORE_EXCEPTIONS)
    if cfg_additional_ignore_exceptions:
        all_ignore_exceptions.extend(cfg_additional_ignore_exceptions)

    # pylint: disable=maybe-no-member
    client = raven.Client(
        dsn=cfg.sentry.dsn,
        site=cfg.sentry.site,
        release=release,
        environment=cfg.sentry.environment,
        include_paths=cfg.sentry.include_paths,
        exclude_paths=cfg.sentry.exclude_paths,
        ignore_exceptions=all_ignore_exceptions,
        sample_rate=cfg.sentry.sample_rate,
        processors=cfg.sentry.processors,
    )

    client.ignore_exceptions.add("ServerTimeout")
    return client


class SentryBaseplateObserver(BaseplateObserver):
    """Error reporting observer.

    This observer reports unexpected exceptions to Sentry.

    The raven client is accessible to your application during requests as the
    ``sentry`` attribute on the :py:class:`~baseplate.RequestContext`.

    :param raven.Client client: A configured raven client.

    """

    def __init__(self, client: raven.Client):
        self.raven = client

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        observer = SentryServerSpanObserver(self.raven, server_span)
        server_span.register(observer)
        context.sentry = self.raven


class SentryServerSpanObserver(ServerSpanObserver):
    def __init__(self, client: raven.Client, server_span: Span):
        self.raven = client
        self.server_span = server_span

    def on_start(self) -> None:
        self.raven.context.activate()

        # for now, this is just a tag for us humans to use
        # https://github.com/getsentry/sentry/issues/716
        self.raven.tags_context({"trace_id": self.server_span.trace_id})

    def on_set_tag(self, key: str, value: Any) -> None:
        if key.startswith("http"):
            self.raven.http_context({key[len("http.") :]: value})
        else:
            self.raven.tags_context({key: value})

    def on_log(self, name: str, payload: Any) -> None:
        self.raven.captureBreadcrumb(category=name, data=payload)

    def on_finish(self, exc_info: Optional[_ExcInfo] = None) -> None:
        if exc_info is not None:
            self.raven.captureException(exc_info=exc_info)
        self.raven.context.clear(deactivate=True)


class SentryUnhandledErrorReporter:
    """Hook into the Gevent hub and report errors outside request context."""

    def __init__(self, hub: "Hub", client: raven.Client):
        self.original_print_exception = getattr(hub, "print_exception")
        self.raven = client
        self.logger = logging.getLogger(__name__)

    def __call__(
        self,
        context: Any,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.raven.captureException((exc_type, value, tb))

        if value and isinstance(value, ServerTimeout):
            self.logger.warning(
                "Server timed out processing for %r after %0.2f seconds",
                value.span_name,
                value.timeout_seconds,
                exc_info=(exc_type, value, tb) if value.debug else None,  # type: ignore
            )
        else:
            self.original_print_exception(context, exc_type, value, tb)
