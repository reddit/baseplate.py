import logging
import random

from contextlib import contextmanager
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import overload
from typing import Tuple
from typing import Type
from typing import TYPE_CHECKING

import gevent.monkey

from pkg_resources import DistributionNotFound
from pkg_resources import get_distribution

from baseplate.lib import config
from baseplate.lib import get_calling_module_name
from baseplate.lib import metrics
from baseplate.lib import UnknownCallerError
from baseplate.lib import warn_deprecated


if TYPE_CHECKING:
    import baseplate.clients
    import baseplate.observers.tracing
    import raven


try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    __version__ = "unknown"


logger = logging.getLogger(__name__)


class BaseplateObserver:
    """Interface for an observer that watches Baseplate."""

    def on_server_span_created(self, context: "RequestContext", server_span: "ServerSpan") -> None:
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

    def on_child_span_created(self, span: "Span") -> None:
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
    trace_id: int

    #: The ID of the parent span, or None if this is the root span.
    parent_id: Optional[int]

    #: The ID of the current span. Should be unique within a trace.
    span_id: int

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
        trace_id = random.getrandbits(64)
        return cls(trace_id=trace_id, parent_id=None, span_id=trace_id, sampled=None, flags=None)

    @classmethod
    def from_upstream(
        cls,
        trace_id: Optional[int],
        parent_id: Optional[int],
        span_id: Optional[int],
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
        if trace_id is None or not 0 <= trace_id < 2 ** 64:
            raise ValueError("invalid trace_id")

        if span_id is None or not 0 <= span_id < 2 ** 64:
            raise ValueError("invalid span_id")

        if parent_id is None or not 0 <= parent_id < 2 ** 64:
            raise ValueError("invalid parent_id")

        if sampled is not None and not isinstance(sampled, bool):
            raise ValueError("invalid sampled value")

        if flags is not None:
            if not 0 <= flags < 2 ** 64:
                raise ValueError("invalid flags value")

        return cls(trace_id, parent_id, span_id, sampled, flags)


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
        span: Optional["Span"] = None,
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
        self.trace: "Span" = span  # type: ignore

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
            obj = RequestContext(context_config=config_item, prefix=full_name, span=self.trace)
        elif hasattr(config_item, "make_object_for_context"):
            obj = config_item.make_object_for_context(full_name, self.trace)
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
            span=self.trace,
            wrapped=self,
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

    def configure_logging(self) -> None:
        """Add request context to the logging system.

        .. deprecated:: 1.0

            Use :py:meth:`configure_observers` instead.

        """
        # pylint: disable=cyclic-import
        from baseplate.observers.logging import LoggingBaseplateObserver

        self.register(LoggingBaseplateObserver())

    def configure_tagged_metrics(self, metrics_client: metrics.Client) -> None:
        """Register a Tagged Metrics Observer on the metrics client.

        Only called if already determined that the user has configured tagging to be on.

        :param metrics_client: Metrics client to send request metrics to.
        :param app_config: The configuration file for the application to parse for metrics tags and flags

        """
        # pylint: disable=cyclic-import
        from baseplate.observers.metrics_tagged import TaggedMetricsBaseplateObserver

        self._metrics_client = metrics_client
        self.register(
            TaggedMetricsBaseplateObserver.from_config_and_client(self._app_config, metrics_client)
        )

    def configure_metrics(self, metrics_client: metrics.Client) -> None:
        """Send timing metrics to the given client.

        This also adds a :py:class:`baseplate.lib.metrics.Batch` object to the
        ``metrics`` attribute on the :py:class:`~baseplate.RequestContext`
        where you can add your own application-specific metrics. The batch is
        automatically flushed at the end of the request.

        .. deprecated:: 1.0

            Use :py:meth:`configure_observers` instead.

        :param metrics_client: Metrics client to send request metrics to.

        """
        # pylint: disable=cyclic-import
        from baseplate.observers.metrics import MetricsBaseplateObserver

        self._metrics_client = metrics_client
        self.register(
            MetricsBaseplateObserver.from_config_and_client(self._app_config, metrics_client)
        )

    def configure_tracing(
        self, tracing_client: "baseplate.observers.tracing.TracingClient"
    ) -> None:
        """Collect and send span information for request tracing.

        When configured, this will send tracing information automatically
        collected by Baseplate to the configured distributed tracing service.

        .. deprecated:: 1.0

            Use :py:meth:`configure_observers` instead.

        :param tracing_client: Tracing client to send request traces to.

        """
        # pylint: disable=cyclic-import
        from baseplate.observers.tracing import TraceBaseplateObserver

        self.register(TraceBaseplateObserver(tracing_client))

    def configure_error_reporting(self, client: "raven.Client") -> None:
        """Send reports for unexpected exceptions to the given client.

        This also adds a :py:class:`raven.Client` object to the ``sentry``
        attribute on the :py:class:`~baseplate.RequestContext` where you can
        send your own application-specific events.

        .. deprecated:: 1.0

            Use :py:meth:`configure_observers` instead.

        :param client: A configured raven client.

        """
        # pylint: disable=cyclic-import
        from baseplate.observers.sentry import SentryBaseplateObserver, SentryUnhandledErrorReporter

        from gevent import get_hub

        hub = get_hub()
        hub.print_exception = SentryUnhandledErrorReporter(hub, client)

        self.register(SentryBaseplateObserver(client))

    def configure_observers(
        self, app_config: Optional[config.RawConfig] = None, module_name: Optional[str] = None
    ) -> None:
        """Configure diagnostics observers based on application configuration.

        This installs all the currently supported observers that have settings
        in the configuration file.

        See :py:mod:`baseplate.observers` for the configuration settings
        available for each observer.

        :param app_config: The application configuration which should have
            settings for the error reporter. If not specified, the config must be passed
            to the Baseplate() constructor.
        :param module_name: Name of the root package of the application. If not specified,
            will be guessed from the package calling this function.

        """
        skipped = []

        if app_config:
            if self._app_config:
                raise Exception("pass app_config to the constructor or this method but not both")

            warn_deprecated(
                "Passing configuration to configure_observers is deprecated in "
                "favor of passing it to the Baseplate constructor"
            )
        else:
            app_config = self._app_config

        self.configure_logging()

        if gevent.monkey.is_module_patched("socket"):
            # pylint: disable=cyclic-import
            from baseplate.observers.timeout import TimeoutBaseplateObserver

            timeout_observer = TimeoutBaseplateObserver.from_config(app_config)
            self.register(timeout_observer)
        else:
            skipped.append("timeout")
        if "metrics.tagging" in app_config:
            if "metrics.namespace" in app_config:
                raise ValueError("metrics.namespace not allowed with metrics.tagging")
            from baseplate.lib.metrics import metrics_client_from_config

            metrics_client = metrics_client_from_config(app_config)
            self.configure_tagged_metrics(metrics_client)
        elif "metrics.namespace" in app_config:
            from baseplate.lib.metrics import metrics_client_from_config

            metrics_client = metrics_client_from_config(app_config)
            self.configure_metrics(metrics_client)
        else:
            skipped.append("metrics")

        if "tracing.service_name" in app_config:
            from baseplate.observers.tracing import tracing_client_from_config

            tracing_client = tracing_client_from_config(app_config)
            self.configure_tracing(tracing_client)
        else:
            skipped.append("tracing")

        if "sentry.dsn" in app_config:
            from baseplate.observers.sentry import error_reporter_from_config

            if module_name is None:
                module_name = get_calling_module_name()

            error_reporter = error_reporter_from_config(app_config, module_name)
            self.configure_error_reporting(error_reporter)
        else:
            skipped.append("error_reporter")

        if skipped:
            logger.debug(
                "The following observers are unconfigured and won't run: %s", ", ".join(skipped)
            )

    @overload
    def configure_context(self, context_spec: Dict[str, Any]) -> None:
        ...

    @overload  # noqa: F811
    def configure_context(self, app_config: config.RawConfig, context_spec: Dict[str, Any]) -> None:
        ...

    def configure_context(self, *args: Any, **kwargs: Any) -> None:  # noqa: F811
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

        if len(args) == 1:
            kwargs["context_spec"] = args[0]
        elif len(args) == 2:
            kwargs["app_config"] = args[0]
            kwargs["context_spec"] = args[1]
        else:
            raise Exception("bad parameters to configure_context")

        if "app_config" in kwargs:
            if self._app_config:
                raise Exception("pass app_config to the constructor or this method but not both")

            warn_deprecated(
                "Passing configuration to configure_context is deprecated in "
                "favor of passing it to the Baseplate constructor"
            )
            app_config = kwargs["app_config"]
        else:
            app_config = self._app_config
        context_spec = kwargs["context_spec"]

        cfg = config.parse_config(app_config, context_spec)
        self._context_config.update(cfg)

    def add_to_context(self, name: str, attribute_config: Any) -> None:
        """Add an attribute or a structure of attributes to each request's context object.

        The given attribute config object can be one of the following:

        * An arbitrary object to be added to the
        :py:class:`~baseplate.RequestContext`.

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
    ) -> "ServerSpan":
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

        if trace_info is None:
            trace_info = TraceInfo.new()

        server_span = ServerSpan(
            trace_info.trace_id,
            trace_info.parent_id,
            trace_info.span_id,
            trace_info.sampled,
            trace_info.flags,
            name,
            context,
            baseplate=self,
        )
        context.trace = server_span

        for observer in self.observers:
            observer.on_server_span_created(context, server_span)
        return server_span

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

    def get_runtime_metric_reporters(self) -> Dict[str, Callable[[Any], None]]:
        specs: List[Tuple[Optional[str], Dict[str, Any]]] = [(None, self._context_config)]
        result = {}
        while specs:
            prefix, spec = specs.pop(0)
            for name, value in spec.items():
                if prefix:
                    full_name = f"{prefix}.{name}"
                else:
                    full_name = name

                if isinstance(value, dict):
                    specs.append((full_name, value))
                elif hasattr(value, "report_runtime_metrics"):
                    result[full_name] = value.report_runtime_metrics
        return result


class Span:
    """A span represents a single RPC within a system."""

    def __init__(
        self,
        trace_id: int,
        parent_id: Optional[int],
        span_id: int,
        sampled: Optional[bool],
        flags: Optional[int],
        name: str,
        context: RequestContext,
        baseplate: Optional[Baseplate] = None,
    ):
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.id = span_id
        self.sampled = sampled
        self.flags = flags
        self.name = name
        self.context = context
        self.baseplate = baseplate
        self.component_name: Optional[str] = None
        self.observers: List[SpanObserver] = []

    def register(self, observer: SpanObserver) -> None:
        """Register an observer to receive events from this span."""
        self.observers.append(observer)

    def start(self) -> None:
        """Record the start of the span.

        This notifies any observers that the span has started, which indicates
        that timers etc. should start ticking.

        Spans also support the `context manager protocol`_, for use with
        Python's ``with`` statement. When the context is entered, the span
        calls :py:meth:`start` and when the context is exited it automatically
        calls :py:meth:`finish`.

        .. _context manager protocol:
            https://docs.python.org/3/reference/datamodel.html#context-managers

        """
        for observer in self.observers:
            observer.on_start()

    def set_tag(self, key: str, value: Any) -> None:
        """Set a tag on the span.

        Tags are arbitrary key/value pairs that add context and meaning to the
        span, such as a hostname or query string. Observers may interpret or
        ignore tags as they desire.

        :param key: The name of the tag.
        :param value: The value of the tag.

        """
        for observer in self.observers:
            observer.on_set_tag(key, value)

    def incr_tag(self, key: str, delta: float = 1) -> None:
        """Increment a tag value on the span.

        This is useful to count instances of an event in your application. In
        addition to showing up as a tag on the span, the value may also be
        aggregated separately as an independent counter.

        :param key: The name of the tag.
        :param value: The amount to increment the value. Defaults to 1.

        """
        for observer in self.observers:
            observer.on_incr_tag(key, delta)

    def log(self, name: str, payload: Optional[Any] = None) -> None:
        """Add a log entry to the span.

        Log entries are timestamped events recording notable moments in the
        lifetime of a span.

        :param name: The name of the log entry. This should be a stable
            identifier that can apply to multiple span instances.
        :param payload: Optional log entry payload. This can be arbitrary data.

        """
        for observer in self.observers:
            observer.on_log(name, payload)

    def finish(self, exc_info: Optional[_ExcInfo] = None) -> None:
        """Record the end of the span.

        :param exc_info: If the span ended because of an exception, this is
            the exception information. The default is :py:data:`None` which
            indicates normal exit.

        """
        for observer in self.observers:
            observer.on_finish(exc_info)

        # clean up reference cycles
        self.context = None  # type: ignore
        self.observers.clear()

    def __enter__(self) -> "Span":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if exc_type is not None:
            self.finish(exc_info=(exc_type, value, traceback))
        else:
            self.finish()

    def make_child(
        self, name: str, local: bool = False, component_name: Optional[str] = None
    ) -> "Span":
        """Return a child Span whose parent is this Span."""
        raise NotImplementedError


class LocalSpan(Span):
    def make_child(
        self, name: str, local: bool = False, component_name: Optional[str] = None
    ) -> "Span":
        """Return a child Span whose parent is this Span.

        The child span can either be a local span representing an in-request
        operation or a span representing an outbound service call.

        In a server, a local span represents the time spent within a
        local component performing an operation or set of operations.
        The local component is some grouping of business logic,
        which is then split up into operations which could each be wrapped
        in local spans.

        :param name: Name to identify the operation this span
            is recording.
        :param local: Make this span a LocalSpan if True, otherwise
            make this span a base Span.
        :param component_name: Name to identify local component
            this span is recording in if it is a local span.
        """
        span_id = random.getrandbits(64)

        context_copy = self.context.clone()
        span: Span
        if local:
            span = LocalSpan(
                self.trace_id,
                self.id,
                span_id,
                self.sampled,
                self.flags,
                name,
                context_copy,
                self.baseplate,
            )
            span.component_name = component_name
        else:
            span = Span(
                self.trace_id,
                self.id,
                span_id,
                self.sampled,
                self.flags,
                name,
                context_copy,
                self.baseplate,
            )
        context_copy.trace = span

        for observer in self.observers:
            observer.on_child_span_created(span)
        return span


class ServerSpan(LocalSpan):
    """A server span represents a request this server is handling.

    The server span is available on the :py:class:`~baseplate.RequestContext`
    during requests as the ``trace`` attribute.

    """


__all__ = ["Baseplate"]
