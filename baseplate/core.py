from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import random

from .integration.wrapped_context import WrappedRequestContext
from ._utils import warn_deprecated


class BaseplateObserver(object):
    """Interface for an observer that watches Baseplate."""

    def on_server_span_created(self, context, server_span):
        """Called when a server span is created.

        :py:class:`Baseplate` calls this when a new request begins.

        :param context: The :term:`context object` for this request.
        :param baseplate.core.ServerSpan server_span: The span representing
            this request.

        """
        raise NotImplementedError


class SpanObserver(object):
    """Interface for an observer that watches a span."""

    def on_start(self):
        """Called when the observed span is started."""
        pass

    def on_set_tag(self, key, value):
        """Called when a tag is set on the observed span."""
        pass

    def on_log(self, name, payload):
        """Called when a log entry is added to the span."""
        pass

    def on_finish(self, exc_info):
        """Called when the observed span is finished.

        :param exc_info: If the span ended because of an exception, the
            exception info. Otherwise, :py:data:`None`.

        """
        pass

    def on_child_span_created(self, span):
        """Called when a child span is created.

        :py:class:`SpanObserver` objects call this when a new child span is
        created.

        :param baseplate.core.Span span: The new child span.

        """
        pass


class ServerSpanObserver(SpanObserver):
    """Interface for an observer that watches the server span."""
    pass


_TraceInfo = collections.namedtuple("_TraceInfo",
                                    "trace_id parent_id span_id sampled flags")


class TraceInfo(_TraceInfo):
    """Trace context for a span.

    If this request was made at the behest of an upstream service, the upstream
    service should have passed along trace information. This class is used for
    collecting the trace context and passing it along to the server span.

    """
    @classmethod
    def new(cls):
        """Generate IDs for a new initial server span.

        This span has no parent and has a random ID. It cannot be correlated
        with any upstream requests.

        """
        trace_id = random.getrandbits(64)
        return cls(trace_id=trace_id, parent_id=None,
                   span_id=trace_id, sampled=None, flags=None)

    @classmethod
    def from_upstream(cls, trace_id, parent_id, span_id, sampled, flags):
        """Build a TraceInfo from individual headers.

        :param int trace_id: The ID of the trace.
        :param int parent_id: The ID of the parent span.
        :param int span_id: The ID of this span within the tree.
        :param bool sampled: Boolean flag to determine request sampling.
        :param int flags: Bit flags for communicating feature flags downstream

        :raises: :py:exc:`ValueError` if any of the values are inappropriate.

        """
        if trace_id is None or not 0 <= trace_id < 2**64:
            raise ValueError("invalid trace_id")

        if span_id is None or not 0 <= span_id < 2**64:
            raise ValueError("invalid span_id")

        if parent_id is None or not 0 <= parent_id < 2**64:
            raise ValueError("invalid parent_id")

        if sampled is not None and not isinstance(sampled, bool):
            raise ValueError("invalid sampled value")

        if flags is not None:
            if not 0 <= flags < 2**64:
                raise ValueError("invalid flags value")

        return cls(trace_id, parent_id, span_id, sampled, flags)


class Baseplate(object):
    """The core of the Baseplate diagnostics framework.

    This class coordinates monitoring and tracing of service calls made to
    and from this service. See :py:mod:`baseplate.integration` for how to
    integrate it with the application framework you are using.

    """
    def __init__(self):
        self.observers = []

    def register(self, observer):
        """Register an observer.

        :param baseplate.core.BaseplateObserver observer: An observer.

        """
        self.observers.append(observer)

    def configure_logging(self):
        """Add request context to the logging system."""
        from .diagnostics.logging import LoggingBaseplateObserver
        self.register(LoggingBaseplateObserver())

    def configure_metrics(self, metrics_client):
        """Send timing metrics to the given client.

        This also adds a :py:class:`baseplate.metrics.Batch` object to the
        ``metrics`` attribute on the :term:`context object` where you can add
        your own application-specific metrics. The batch is automatically
        flushed at the end of the request.

        :param baseplate.metrics.Client metrics_client: Metrics client to send
            request metrics to.

        """
        from .diagnostics.metrics import MetricsBaseplateObserver
        self.register(MetricsBaseplateObserver(metrics_client))

    def configure_tracing(self, tracing_client, *args, **kwargs):
        """Collect and send span information for request tracing.

        When configured, this will send tracing information automatically
        collected by Baseplate to the configured distributed tracing service.

        :param baseplate.diagnostics.tracing.TracingClient tracing_client: Tracing
            client to send request traces to.

        """
        from .diagnostics.tracing import (
            make_client,
            TraceBaseplateObserver,
            TracingClient,
        )

        # the first parameter was service_name before, so if it's not a client
        # object we'll act like this is the old-style invocation and use the
        # first parameter as service_name instead, passing on the old arguments
        if not isinstance(tracing_client, TracingClient):
            warn_deprecated("Passing tracing configuration directly to "
                            "configure_tracing is deprecated in favor of "
                            "using baseplate.make_tracing_client and passing "
                            "the constructed client on.")
            tracing_client = make_client(tracing_client, *args, **kwargs)

        self.register(TraceBaseplateObserver(tracing_client))

    def configure_error_reporting(self, client):
        """Send reports for unexpected exceptions to the given client.

        This also adds a :py:class:`raven.Client` object to the ``sentry``
        attribute on the :term:`context object` where you can send your own
        application-specific events.

        :param raven.Client client: A configured raven client.

        """
        from .diagnostics.sentry import SentryBaseplateObserver
        self.register(SentryBaseplateObserver(client))

    def add_to_context(self, name, context_factory):
        """Add an attribute to each request's context object.

        On each request, the factory will be asked to create an appropriate
        object to attach to the :term:`context object`.

        :param str name: The attribute on the context object to attach the
            created object to. This may also be used for metric/tracing
            purposes so it should be descriptive.
        :param baseplate.context.ContextFactory context_factory: A factory.

        """
        from .context import ContextObserver
        self.register(ContextObserver(name, context_factory))

    def make_server_span(self, context, name, trace_info=None):
        """Return a server span representing the request we are handling.

        In a server, a server span represents the time spent on a single
        incoming request. Any calls made to downstream services will be new
        child spans of the server span, and the server span will in turn be the
        child span of whatever upstream request it is part of, if any.

        :param context: The :term:`context object` for this request.
        :param str name: A name to identify the type of this request, e.g.
            a route or RPC method name.
        :param baseplate.core.TraceInfo trace_info: The trace context of this
            request as passed in from upstream. If :py:data:`None`, a new trace
            context will be generated.

        """

        if trace_info is None:
            trace_info = TraceInfo.new()

        server_span = ServerSpan(trace_info.trace_id, trace_info.parent_id,
                                 trace_info.span_id, trace_info.sampled,
                                 trace_info.flags, name, WrappedRequestContext(context))

        for observer in self.observers:
            observer.on_server_span_created(context, server_span)
        return server_span


class Span(object):
    """A span represents a single RPC within a system."""

    def __init__(self, trace_id, parent_id, span_id, sampled, flags, name, context):
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.id = span_id
        self.sampled = sampled
        self.flags = flags
        self.name = name
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
    def __init__(self,
                 trace_id,
                 parent_id,
                 span_id,
                 sampled,
                 flags,
                 name,
                 context):
        super(LocalSpan, self).__init__(trace_id, parent_id, span_id, sampled,
                                        flags, name, context)
        self.context = context

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

        if local:
            context_copy = self.context.clone()
            span = LocalSpan(self.trace_id, self.id, span_id, self.sampled,
                             self.flags, name, context_copy)
            if component_name is None:
                raise ValueError("Cannot create local span without component name.")
            span.component_name = component_name
            context_copy.shadow_context_attr('trace', span)
        else:
            span = Span(self.trace_id, self.id, span_id, self.sampled, self.flags, name, self.context)
        for observer in self.observers:
            observer.on_child_span_created(span)
        return span


class ServerSpan(LocalSpan):
    """A server span represents a request this server is handling.

    The server span is available on the :term:`context object` during requests
    as the ``trace`` attribute.

    """
    pass
