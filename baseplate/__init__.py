import logging
import os
import random

from contextlib import contextmanager
from types import TracebackType
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import Type

import gevent.monkey
import opentelemetry.context

from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind
from pkg_resources import DistributionNotFound
from pkg_resources import get_distribution

from baseplate.lib import config
from baseplate.lib import get_calling_module_name
from baseplate.lib import metrics
from baseplate.lib import UnknownCallerError


try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    __version__ = "unknown"


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class BaseplateObserver:
    """Interface for an observer that watches Baseplate."""

    def on_server_span_created(self, context: "RequestContext", server_span: trace.Span) -> None:
        """Do something when a server span is created.

        :py:class:`Baseplate` calls this when a new request begins.

        :param context: The :py:class:`~baseplate.RequestContext` for this
            request.
        :param server_span: The span representing this request.

        """
        raise NotImplementedError


_ExcInfo = Tuple[Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]


class SpanObserver:
    """Interface for an observer that watches a span."""

    def on_start(self) -> None:
        """Do something when the observed span is started."""

    def on_set_tag(self, key: str, value: Any) -> None:
        """Do something when a tag is set on the observed span."""

    def on_incr_tag(self, key: str, delta: float) -> None:
        """Do something when a tag value is incremented on the observed span."""

    def on_log(self, name: str, payload: Any) -> None:
        """Do something when a log entry is added to the span."""

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        """Do something when the observed span is finished.

        :param exc_info: If the span ended because of an exception, the
            exception info. Otherwise, :py:data:`None`.

        """

    def on_child_span_created(self, span: trace.Span) -> None:
        """Do something when a child span is created.

        :py:class:`SpanObserver` objects call this when a new child span is
        created.

        :param span: The new child span.

        """


class ServerSpanObserver(SpanObserver):
    """Interface for an observer that watches the server span."""


class TraceInfo(NamedTuple):
    """Trace context for a span.

    If this request was made at the behest of an upstream service, the upstream
    service should have passed along trace information. This class is used for
    collecting the trace context and passing it along to the server span.

    """

    #: The ID of the whole trace. This will be the same for all downstream requests.
    trace_id: str

    #: The ID of the parent span, or None if this is the root span.
    parent_id: Optional[str]

    #: The ID of the current span. Should be unique within a trace.
    span_id: str

    #: True if this trace was selected for sampling. Will be propagated to child spans.
    sampled: Optional[bool]

    #: A bit field of extra flags about this trace.
    flags: Optional[int]

    @classmethod
    def new(cls) -> "TraceInfo":
        """Generate IDs for a new initial server span.

        This span has no parent and has a random ID. It cannot be correlated
        with any upstream requests.

        """
        trace_id = str(random.getrandbits(128))
        span_id = str(random.getrandbits(64))
        return cls(trace_id=trace_id, parent_id=None, span_id=span_id, sampled=None, flags=None)

    @classmethod
    def from_upstream(
        cls,
        trace_id: str,
        parent_id: Optional[str],
        span_id: str,
        sampled: Optional[bool],
        flags: Optional[int],
    ) -> "TraceInfo":
        """Build a TraceInfo from individual headers.

        :param trace_id: The ID of the trace.
        :param parent_id: The ID of the parent span.
        :param span_id: The ID of this span within the tree.
        :param sampled: Boolean flag to determine request sampling.
        :param flags: Bit flags for communicating feature flags downstream

        :raises: :py:exc:`ValueError` if any of the values are inappropriate.

        """
        if trace_id is None:
            raise ValueError("invalid trace_id")

        if span_id is None:
            raise ValueError("invalid span_id")

        if sampled is not None and not isinstance(sampled, bool):
            raise ValueError("invalid sampled value")

        if flags is not None:
            if not 0 <= flags < 2 ** 64:
                raise ValueError("invalid flags value")

        return cls(trace_id, parent_id, span_id, sampled, flags)

    def to_otel(self) -> trace.Span:
        if self.sampled:
            sampled = trace.TraceFlags.SAMPLED
        else:
            sampled = trace.TraceFlags.DEFAULT
        ctx = trace.SpanContext(
            trace_id=int(self.trace_id, 16),
            span_id=int(self.span_id, 16),
            trace_flags=trace.TraceFlags(sampled),
            is_remote=True,
        )
        return trace.NonRecordingSpan(ctx)

    @classmethod
    def from_otel(cls, ctx: opentelemetry.context.Context) -> "TraceInfo":
        data: Dict = {}
        # Inject our tracing headers into the data dict. Then we can use our redditb3 headers to capture
        # the values needed.
        inject(data, ctx)
        sampled: bool = bool(data["X-Sampled"] == "1")
        return cls(data["X-Trace"], None, data["X-Span"], sampled, None)


class RequestContext:
    """The request context object.

    The context object is passed into each request handler by the framework
    you're using. In some cases (e.g. Pyramid) the request object will also
    inherit from another base class and carry extra framework-specific
    information.

    Clients and configuration added to the context via
    :py:meth:`~baseplate.Baseplate.configure_context` or
    :py:meth:`~baseplate.Baseplate.add_to_context` will be available as an
    attribute on this object.  To take advantage of Baseplate's automatic
    monitoring, any interactions with external services should be done through
    these clients.

    """

    def __init__(
        self,
        context_config: Dict[str, Any],
        prefix: Optional[str] = None,
        span: Optional[trace.Span] = None,
        wrapped: Optional["RequestContext"] = None,
    ):
        self.__context_config = context_config
        self.__prefix = prefix
        self.__wrapped = wrapped

        # the context and span reference eachother (unfortunately) so we can't
        # construct 'em both with references from the start. however, we can
        # guarantee that during the valid life of a span, there will be a
        # reference. so we fake it here and say "trust us".
        #
        # this would be much cleaner with a different API but this is where we are.
        self.span: trace.Span = span  # type: ignore

    def __getattr__(self, name: str) -> Any:
        try:
            config_item = self.__context_config[name]
        except KeyError:
            try:
                return getattr(self.__wrapped, name)
            except AttributeError:
                raise AttributeError(
                    f"{repr(self.__class__.__name__)} object has no attribute {repr(name)}"
                ) from None

        if self.__prefix:
            full_name = f"{self.__prefix}.{name}"
        else:
            full_name = name

        if isinstance(config_item, dict):
            obj = RequestContext(context_config=config_item, prefix=full_name, span=self.span)
        elif hasattr(config_item, "make_object_for_context"):
            obj = config_item.make_object_for_context(full_name, self.span)
        else:
            obj = config_item

        setattr(self, name, obj)
        return obj

    # this is just here for type checking
    # pylint: disable=useless-super-delegation
    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)

    def clone(self) -> "RequestContext":
        return RequestContext(
            context_config=self.__context_config,
            prefix=self.__prefix,
            span=self.span,
            wrapped=self,
        )


class ReusedContextObjectError(Exception):
    def __init__(self) -> None:
        super().__init__(
            "Context objects cannot be re-used. See https://baseplate.readthedocs.io/en/latest/guide/faq.html#what-do-i-do-about-context-objects-cannot-be-re-used"
        )


class Baseplate:
    """The core of the Baseplate framework.

    This class coordinates monitoring and tracing of service calls made to
    and from this service. See :py:mod:`baseplate.frameworks` for how to
    integrate it with the application framework you are using.

    """

    def __init__(self, app_config: Optional[config.RawConfig] = None) -> None:
        """Initialize the core observability framework.

        :param app_config: The raw configuration dictionary for your
            application as supplied by :program:`baseplate-serve` or
            :program:`baseplate-script`. In addition to allowing
            framework-level configuration (described next), if this is supplied
            you do not need to pass the configuration again when calling
            :py:meth:`configure_observers` or :py:meth:`configure_context`.

        Baseplate services can identify themselves to downstream services in
        requests. The name a service identifies as defaults to the Python
        module the :py:class:`~baseplate.Baseplate` object is instantiated in.
        To override the default, make sure you are passing in an `app_config`
        and configure the name in your INI file:

        .. code-block:: ini

            [app:main]
            baseplate.service_name = foo_service

            ...

        """
        self.observers: List[BaseplateObserver] = []
        self._metrics_client: Optional[metrics.Client] = None
        self._context_config: Dict[str, Any] = {}
        self._app_config = app_config or {}

        self.service_name = self._app_config.get("baseplate.service_name")
        if not self.service_name:
            try:
                self.service_name = get_calling_module_name()
            except UnknownCallerError:
                # this happens e.g. when instantiating Baseplate() in a shell
                pass

    def register(self, observer: BaseplateObserver) -> None:
        """Register an observer.

        :param observer: An observer.

        """
        self.observers.append(observer)

    # pylint: disable=cyclic-import
    def configure_observers(self) -> None:
        """Configure diagnostics observers based on application configuration.

        This installs all the currently supported observers that have settings
        in the configuration file.

        See :py:mod:`baseplate.observers` for the configuration settings
        available for each observer.

        """
        skipped = []

        from baseplate.observers.logging import LoggingBaseplateObserver

        self.register(LoggingBaseplateObserver())

        if gevent.monkey.is_module_patched("socket"):
            from baseplate.observers.timeout import TimeoutBaseplateObserver

            timeout_observer = TimeoutBaseplateObserver.from_config(self._app_config)
            self.register(timeout_observer)
        else:
            skipped.append("timeout")

        if "sentry.dsn" in self._app_config or "SENTRY_DSN" in os.environ:
            from baseplate.observers.sentry import init_sentry_client_from_config
            from baseplate.observers.sentry import SentryBaseplateObserver
            from baseplate.observers.sentry import _SentryUnhandledErrorReporter

            init_sentry_client_from_config(self._app_config)
            _SentryUnhandledErrorReporter.install()
            self.register(SentryBaseplateObserver())
        else:
            skipped.append("sentry")

        if skipped:
            logger.debug(
                "The following observers are unconfigured and won't run: %s", ", ".join(skipped)
            )

    def configure_context(self, context_spec: Dict[str, Any]) -> None:
        """Add a number of objects to each request's context object.

        Configure and attach multiple clients to the
        :py:class:`~baseplate.RequestContext` in one place. This takes a full
        configuration spec like :py:func:`baseplate.lib.config.parse_config`
        and will attach the specified structure onto the context object each
        request.

        For example, a configuration like::

            baseplate = Baseplate(app_config)
            baseplate.configure_context({
                "cfg": {
                    "doggo_is_good": config.Boolean,
                },
                "cache": MemcachedClient(),
                "cassandra": {
                    "foo": CassandraClient("foo_keyspace"),
                    "bar": CassandraClient("bar_keyspace"),
                },
            })

        would build a context object that could be used like::

            assert context.cfg.doggo_is_good == True
            context.cache.get("example")
            context.cassandra.foo.execute()

        :param app_config: The raw stringy configuration dictionary.
        :param context_spec: A specification of what the configuration should
            look like.

        """
        cfg = config.parse_config(self._app_config, context_spec)
        self._context_config.update(cfg)

    def add_to_context(self, name: str, attribute_config: Any) -> None:
        """Add an attribute or a structure of attributes to each request's context object.

        The given attribute config object can be one of the following:

        * An arbitrary object to be added to the :py:class:`~baseplate.RequestContext`.

        * A factory with a method named ``make_object_for_context``.  On each
          request, the factory will be asked to create an appropriate object to
          attach to the :py:class:`~baseplate.RequestContext`.

        * A dict containing arbitrary objects, factories, or other dicts.  In
          this case, a nested object will be added to the context. Each item of
          the dict will be processed using the same rules to become an attribute
          of the nested object.

        :param name: The attribute on the context object to attach the
            created object to. This may also be used for metric/tracing
            purposes so it should be descriptive.
        :param attribute_config: A configuration object.

        """
        self._context_config[name] = attribute_config

    def make_context_object(self) -> RequestContext:
        """Make a context object for the request."""
        return RequestContext(self._context_config)

    def make_server_span(
        self, context: RequestContext, name: str, trace_info: Optional[TraceInfo] = None
    ) -> trace.Span:
        """Return a server span representing the request we are handling.

        In a server, a server span represents the time spent on a single
        incoming request. Any calls made to downstream services will be new
        child spans of the server span, and the server span will in turn be the
        child span of whatever upstream request it is part of, if any.

        :param context: The :py:class:`~baseplate.RequestContext` for this
            request.
        :param name: A name to identify the type of this request, e.g.  a route
            or RPC method name.
        :param trace_info: The trace context of this request as passed in from
            upstream. If :py:data:`None`, a new trace context will be generated.

        """
        assert isinstance(context, RequestContext)

        if context.span is not None:
            raise ReusedContextObjectError

        if trace_info is None:
            trace_info = TraceInfo.new()

        ctxspan = trace_info.to_otel()
        ctx = trace.set_span_in_context(ctxspan)
        span = tracer.start_span(name, ctx, kind=SpanKind.SERVER)
        context.span = span

        for observer in self.observers:
            observer.on_server_span_created(context, span)
        return span

    @contextmanager
    def server_context(self, name: str) -> Iterator[RequestContext]:
        """Create a server span and return a context manager for its lifecycle.

        This is a convenience wrapper around a common pattern seen outside of
        servers handling requests. For example, simple cron jobs or one-off
        scripts might want to create a temporary span and access the context
        object. Instead of calling
        :py:meth:`~baseplate.Baseplate.make_context_object` followed by
        :py:meth:`~baseplate.Baseplate.make_server_span` manually, this method
        bundles it all up for you::

            with baseplate.server_context("foo") as context:
                context.redis.ping()

        .. note::

            This should not be used within an existing span context (such as
            during request processing) as it creates a new span unrelated to
            any other ones.

        """
        context = self.make_context_object()
        with self.make_server_span(context, name):
            yield context


__all__ = ["Baseplate"]
