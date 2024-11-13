from __future__ import annotations

import logging

from types import TracebackType
from typing import Any
from typing import List
from typing import Optional
from typing import Type
from typing import TYPE_CHECKING
from typing import Union

import sentry_sdk

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import RequestContext
from baseplate import ServerSpanObserver
from baseplate import Span
from baseplate.lib import config
from baseplate.observers.timeout import ServerTimeout

if TYPE_CHECKING:
    from gevent.hub import Hub as GeventHub


ALWAYS_IGNORE_ERRORS = (
    "baseplate.observers.timeout.ServerTimeout",
    "ConnectionError",
    "ConnectionRefusedError",
    "ConnectionResetError",
    "pymemcache.exceptions.MemcacheServerError",
    "requests.exceptions.HTTPError",
    "thrift.Thrift.TApplicationException",
    "thrift.Thrift.TProtocolException",
    "thrift.Thrift.TTransportException",
)


def init_sentry_client_from_config(raw_config: config.RawConfig, **kwargs: Any) -> None:
    """Configure the Sentry client.

    This expects one configuration option and can take many optional ones:

    ``sentry.dsn``
        The DSN provided by Sentry. If blank, the reporter will discard events.
    ``sentry.environment`` (optional)
        The environment your application is running in.
    ``sentry.sample_rate`` (optional)
        Percentage of errors to report. (e.g. "37%")
    ``sentry.ignore_errors`` (optional)
        A comma-delimited list of exception names, unqualified (e.g.
        ServerTimeout) or fully qualified (e.g.
        baseplate.observers.timeout.ServerTimeout) to not notify sentry about.
        Note: a minimal list of common exceptions is hard-coded in Baseplate,
        this option only extends that list.

    Example usage::

        init_sentry_client_from_config(app_config)

    :param raw_config: The application configuration which should have
        settings for the error reporter.

    """
    cfg = config.parse_config(
        raw_config,
        {
            "sentry": {
                "dsn": config.Optional(config.String, default=None),
                "environment": config.Optional(config.String, default=None),
                "sample_rate": config.Optional(config.Percent, default=1),
                "ignore_errors": config.Optional(config.TupleOf(config.String), default=()),
            }
        },
    )

    if cfg.sentry.dsn:
        kwargs.setdefault("dsn", cfg.sentry.dsn)

    if cfg.sentry.environment:
        kwargs.setdefault("environment", cfg.sentry.environment)

    kwargs.setdefault("sample_rate", cfg.sentry.sample_rate)

    ignore_errors: List[Union[type, str]] = []
    ignore_errors.extend(ALWAYS_IGNORE_ERRORS)
    ignore_errors.extend(cfg.sentry.ignore_errors)
    kwargs.setdefault("ignore_errors", ignore_errors)

    kwargs.setdefault("with_locals", False)

    client = sentry_sdk.Client(**kwargs)
    sentry_sdk.Hub.current.bind_client(client)


class SentryBaseplateObserver(BaseplateObserver):
    """Error reporting observer.

    This observer reports unexpected exceptions to Sentry.

    """

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
        sentry_hub = sentry_sdk.Hub.current
        observer = _SentryServerSpanObserver(sentry_hub, server_span)
        server_span.register(observer)
        context.sentry = sentry_hub


class _SentryServerSpanObserver(ServerSpanObserver):
    def __init__(self, sentry_hub: sentry_sdk.Hub, server_span: Span):
        self.sentry_hub = sentry_hub
        self.scope_manager = self.sentry_hub.push_scope()
        self.scope = self.scope_manager.__enter__()
        self.server_span = server_span

    def on_start(self) -> None:
        self.scope.set_tag("trace_id", self.server_span.trace_id)

    def on_set_tag(self, key: str, value: Any) -> None:
        self.scope.set_tag(key, value)

    def on_log(self, name: str, payload: Any) -> None:
        self.sentry_hub.add_breadcrumb({"category": name, "message": str(payload)})

    def on_finish(self, exc_info: Optional[_ExcInfo] = None) -> None:
        if exc_info is not None:
            self.sentry_hub.capture_exception(error=exc_info)
        self.scope_manager.__exit__(None, None, None)


class _SentryUnhandledErrorReporter:
    """Hook into the Gevent hub and report errors outside request context."""

    @classmethod
    def install(cls) -> None:
        from gevent import get_hub

        gevent_hub = get_hub()
        gevent_hub.print_exception = cls(gevent_hub)

    @classmethod
    def uninstall(cls) -> None:
        from gevent import get_hub

        gevent_hub = get_hub()
        assert isinstance(gevent_hub.print_exception, cls)
        gevent_hub.print_exception = gevent_hub.print_exception.original_print_exception

    def __init__(self, hub: GeventHub):
        self.original_print_exception = getattr(hub, "print_exception")
        self.logger = logging.getLogger(__name__)

    def __call__(
        self,
        context: Any,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        sentry_sdk.capture_exception((exc_type, value, tb))

        if value and isinstance(value, ServerTimeout):
            self.logger.warning(
                "Server timed out processing for %r after %0.2f seconds",
                value.span_name,
                value.timeout_seconds,
                exc_info=(exc_type, value, tb) if value.debug else None,  # type: ignore
            )
        else:
            self.original_print_exception(context, exc_type, value, tb)
