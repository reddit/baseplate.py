import logging
import random

from types import TracebackType
from typing import Any
from typing import Dict
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import Type

from baseplate.lib import config
from baseplate.lib import warn_deprecated


logger = logging.getLogger(__name__)


class BaseplateObserver:
    """Interface for an observer that watches Baseplate."""

    def on_server_span_created(self, context: Any, server_span: "Span") -> None:
        """Do something when a server span is created.

        :py:class:`Baseplate` calls this when a new request begins.

        :param context: The :term:`context object` for this request.
        :param baseplate.ServerSpan server_span: The span representing
            this request.

        """
        raise NotImplementedError


_ExcInfo = Tuple[Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]


class SpanObserver:
    """Interface for an observer that watches a span."""

    def on_start(self) -> None:
        """Do something when the observed span is started."""

    def on_set_tag(self, key: str, value: str) -> None:
        """Do something when a tag is set on the observed span."""

    def on_log(self, name: str, payload: str) -> None:
        """Do something when a log entry is added to the span."""

    def on_finish(self, exc_info: _ExcInfo) -> None:
        """Do something when the observed span is finished.

        :param exc_info: If the span ended because of an exception, the
            exception info. Otherwise, :py:data:`None`.

        """

    def on_child_span_created(self, span: "Span") -> None:
        """Do something when a child span is created.

        :py:class:`SpanObserver` objects call this when a new child span is
        created.

        :param baseplate.Span span: The new child span.

        """


class ServerSpanObserver(SpanObserver):
    """Interface for an observer that watches the server span."""


class TraceInfo(NamedTuple):
    """Trace context for a span.

    If this request was made at the behest of an upstream service, the upstream
    service should have passed along trace information. This class is used for
    collecting the trace context and passing it along to the server span.

    """

    trace_id: int
    parent_id: Optional[int]
    span_id: Optional[int]
    sampled: Optional[bool]
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
    def __init__(self, context_config, prefix=None, span=None, wrapped=None):
        self.__context_config = context_config
        self.__prefix = prefix
        self.__wrapped = wrapped
        self.trace: ServerSpan = span

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

    def clone(self) -> "RequestContext":
        return RequestContext(
            context_config=self.__context_config,
            prefix=self.__prefix,
            span=self.trace,
            wrapped=self,
        )


class Baseplate:
    """The core of the Baseplate diagnostics framework.

    This class coordinates monitoring and tracing of service calls made to
    and from this service. See :py:mod:`baseplate.frameworks` for how to
    integrate it with the application framework you are using.

    """

    def __init__(self):
        self.observers = []
        self._metrics_client = None
        self._context_config = {}

    def register(self, observer):
        """Register an observer.

        :param baseplate.BaseplateObserver observer: An observer.

        """
        self.observers.append(observer)

    def configure_logging(self):
        """Add request context to the logging system."""
        # pylint: disable=cyclic-import
        from baseplate.observers.logging import LoggingBaseplateObserver

        self.register(LoggingBaseplateObserver())

    def configure_metrics(self, metrics_client):
        """Send timing metrics to the given client.

        This also adds a :py:class:`baseplate.lib.metrics.Batch` object to the
        ``metrics`` attribute on the :term:`context object` where you can add
        your own application-specific metrics. The batch is automatically
        flushed at the end of the request.

        :param baseplate.lib.metrics.Client metrics_client: Metrics client to send
            request metrics to.

        """
        # pylint: disable=cyclic-import
        from baseplate.observers.metrics import MetricsBaseplateObserver

        self._metrics_client = metrics_client
        self.register(MetricsBaseplateObserver(metrics_client))

    def configure_tracing(self, tracing_client, *args, **kwargs):
        """Collect and send span information for request tracing.

        When configured, this will send tracing information automatically
        collected by Baseplate to the configured distributed tracing service.

        :param baseplate.observers.tracing.TracingClient tracing_client: Tracing
            client to send request traces to.

        """
        # pylint: disable=cyclic-import
        from baseplate.observers.tracing import make_client, TraceBaseplateObserver, TracingClient

        # the first parameter was service_name before, so if it's not a client
        # object we'll act like this is the old-style invocation and use the
        # first parameter as service_name instead, passing on the old arguments
        if not isinstance(tracing_client, TracingClient):
            warn_deprecated(
                "Passing tracing configuration directly to "
                "configure_tracing is deprecated in favor of "
                "using baseplate.observers.tracing.tracing_client_from_config and "
                "passing the constructed client on."
            )
            tracing_client = make_client(tracing_client, *args, **kwargs)

        self.register(TraceBaseplateObserver(tracing_client))

    def configure_error_reporting(self, client):
        """Send reports for unexpected exceptions to the given client.

        This also adds a :py:class:`raven.Client` object to the ``sentry``
        attribute on the :term:`context object` where you can send your own
        application-specific events.

        :param raven.Client client: A configured raven client.

        """
        # pylint: disable=cyclic-import
        from baseplate.observers.sentry import SentryBaseplateObserver, SentryUnhandledErrorReporter

        from gevent import get_hub

        hub = get_hub()
        hub.print_exception = SentryUnhandledErrorReporter(hub, client)

        self.register(SentryBaseplateObserver(client))

    def configure_observers(self, app_config: config.RawConfig, module_name: str) -> None:
        """Configure diagnostics observers based on application config file.

        This installs all the currently supported observers that have settings
        in the configuration file.

        For the individual configurables, see the documentation for:

        * :py:func:`~baseplate.observers.sentry.error_reporter_from_config`
        * :py:func:`~baseplate.lib.metrics.metrics_client_from_config`
        * :py:func:`~baseplate.observers.tracing.tracing_client_from_config`

        :param raw_config: The application configuration which should have
            settings for the error reporter.
        :param module_name: ``__name__`` of the root module of the application.

        """
        skipped = []

        self.configure_logging()

        if "metrics.namespace" in app_config:
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

            error_reporter = error_reporter_from_config(app_config, module_name)
            self.configure_error_reporting(error_reporter)
        else:
            skipped.append("error_reporter")

        logger.debug(
            "The following observers are unconfigured and won't run: %s", ",".join(skipped)
        )

    def configure_context(self, app_config: config.RawConfig, context_spec: Dict) -> None:
        """Add a number of objects to each request's context object.

        Configure and attach multiple clients to the :term:`context object` in
        one place. This takes a full configuration spec like
        :py:func:`baseplate.lib.config.parse_config` and will attach the specified
        structure onto the context object each request.

        For example, a configuration like::

            baseplate = Baseplate()
            baseplate.configure_context(app_config, {
                "cfg": {
                    "doggo_is_good": config.Boolean,
                },
                "cache": MemcachedClient(),
                "cassandra": {
                    "foo": CassandraClient(),
                    "bar": CassandraClient(),
                },
            })

        would build a context object that could be used like::

            assert context.cfg.doggo_is_good == True
            context.cache.get("example")
            context.cassandra.foo.execute()

        :param config: The raw stringy configuration dictionary.
        :param context_spec: A specification of what the config should look
            like. This should only contain context clients and nested dictionaries.
            Unrelated configuration values should not be included.

        """
        cfg = config.parse_config(app_config, context_spec)
        self._context_config.update(cfg)

    def add_to_context(self, name, context_factory):
        """Add an attribute to each request's context object.

        On each request, the factory will be asked to create an appropriate
        object to attach to the :term:`context object`.

        :param str name: The attribute on the context object to attach the
            created object to. This may also be used for metric/tracing
            purposes so it should be descriptive.
        :param baseplate.clients.ContextFactory context_factory: A factory.

        """
        self._context_config[name] = context_factory

    def make_context_object(self):
        """Make a context object for the request."""
        return RequestContext(self._context_config)

    def make_server_span(self, context, name, trace_info=None):
        """Return a server span representing the request we are handling.

        In a server, a server span represents the time spent on a single
        incoming request. Any calls made to downstream services will be new
        child spans of the server span, and the server span will in turn be the
        child span of whatever upstream request it is part of, if any.

        :param RequestContext context: The :term:`context object` for this
            request. Must have been created using :py:meth:`make_context_object`.
        :param str name: A name to identify the type of this request, e.g.
            a route or RPC method name.
        :param baseplate.TraceInfo trace_info: The trace context of this
            request as passed in from upstream. If :py:data:`None`, a new trace
            context will be generated.

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
        )
        context.trace = server_span

        for observer in self.observers:
            observer.on_server_span_created(context, server_span)
        return server_span

    def get_runtime_metric_reporters(self):
        specs = [(None, self._context_config)]
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

    def __init__(self, trace_id, parent_id, span_id, sampled, flags, name, context):
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.id = span_id
        self.sampled = sampled
        self.flags = flags
        self.name = name
        self.context = context
        self.observers = []

    def register(self, observer):
        """Register an observer to receive events from this span."""
        self.observers.append(observer)

    def start(self):
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

    def set_tag(self, key, value):
        """Set a tag on the span.

        Tags are arbitrary key/value pairs that add context and meaning to the
        span, such as a hostname or query string. Observers may interpret or
        ignore tags as they desire.

        :param str key: The name of the tag.
        :param value: The value of the tag, must be a string/boolean/number.

        """
        for observer in self.observers:
            observer.on_set_tag(key, value)

    def log(self, name, payload=None):
        """Add a log entry to the span.

        Log entries are timestamped events recording notable moments in the
        lifetime of a span.

        :param str name: The name of the log entry. This should be a stable
            identifier that can apply to multiple span instances.
        :param payload: Optional log entry payload. This can be arbitrary data.

        """
        for observer in self.observers:
            observer.on_log(name, payload)

    def finish(self, exc_info=None):
        """Record the end of the span.

        :param exc_info: If the span ended because of an exception, this is
            the exception information. The default is :py:data:`None` which
            indicates normal exit.

        """
        for observer in self.observers:
            observer.on_finish(exc_info)

        # clean up reference cycles
        self.context = None
        self.observers.clear()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.finish(exc_info=(exc_type, value, traceback))
        else:
            self.finish()

    def make_child(self, name, local=False, component_name=None):
        """Return a child Span whose parent is this Span."""
        raise NotImplementedError


class LocalSpan(Span):
    def make_child(self, name, local=False, component_name=None):
        """Return a child Span whose parent is this Span.

        The child span can either be a local span representing an in-request
        operation or a span representing an outbound service call.

        In a server, a local span represents the time spent within a
        local component performing an operation or set of operations.
        The local component is some grouping of business logic,
        which is then split up into operations which could each be wrapped
        in local spans.

        :param str name: Name to identify the operation this span
            is recording.
        :param bool local: Make this span a LocalSpan if True, otherwise
            make this span a base Span.
        :param str component_name: Name to identify local component
            this span is recording in if it is a local span.
        """
        span_id = random.getrandbits(64)

        context_copy = self.context.clone()
        if local:
            span = LocalSpan(
                self.trace_id, self.id, span_id, self.sampled, self.flags, name, context_copy
            )
            span.component_name = component_name
        else:
            span = Span(
                self.trace_id, self.id, span_id, self.sampled, self.flags, name, context_copy
            )
        context_copy.trace = span

        for observer in self.observers:
            observer.on_child_span_created(span)
        return span


class ServerSpan(LocalSpan):
    """A server span represents a request this server is handling.

    The server span is available on the :term:`context object` during requests
    as the ``trace`` attribute.

    """


__all__ = ["Baseplate"]
