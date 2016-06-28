from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import random

from ._utils import warn_deprecated


class BaseplateObserver(object):
    """Interface for an observer that watches Baseplate."""

    def on_root_span_created(self, context, root_span):  # pragma: nocover
        """Called when a root span is created.

        :py:class:`Baseplate` calls this when a new request begins.

        :param context: The :term:`context object` for this request.
        :param baseplate.core.RootSpan root_span: The root span representing
            this request.

        """
        raise NotImplementedError


class SpanObserver(object):  # pragma: nocover
    """Interface for an observer that watches a span."""

    def on_start(self):
        """Called when the observed span is started."""
        pass

    def on_annotate(self, key, value):
        """Called when an annotation is added to the observed span."""
        pass

    def on_stop(self, error):
        """Called when the observed span is stopped.

        :param error: If the span ended because of an exception, the instance
            raised. Otherwise, :py:data:`None`.

        """
        pass


class RootSpanObserver(SpanObserver):
    """Interface for an observer that watches the root span."""

    def on_child_span_created(self, span):  # pragma: nocover
        """Called when a child span is created.

        :py:class:`RootSpan` objects call this when a new child span is
        created.

        :param baseplate.core.Span span: The new child span.

        """
        pass


_TraceInfo = collections.namedtuple("_TraceInfo", "trace_id parent_id span_id")


class TraceInfo(_TraceInfo):
    """Trace context for a span.

    If this request was made at the behest of an upstream service, the upstream
    service should have passed along trace information. This class is used for
    collecting the trace context and passing it along to the root span.

    """
    @classmethod
    def new(cls):
        """Generate IDs for a new initial root span.

        This span has no parent and has a random ID. It cannot be correlated
        with any upstream requests.

        """
        trace_id = random.getrandbits(64)
        return cls(trace_id=trace_id, parent_id=None, span_id=trace_id)

    @classmethod
    def from_upstream(cls, trace_id, parent_id, span_id):
        """Build a TraceInfo from individual headers.

        :param int trace_id: The ID of the trace.
        :param int parent_id: The ID of the parent span.
        :param int span_id: The ID of this span within the tree.

        :raises: :py:exc:`ValueError` if any of the values are inappropriate.

        """
        if trace_id is None or not 0 <= trace_id < 2**64:
            raise ValueError("invalid trace_id")

        if span_id is None or not 0 <= span_id < 2**64:
            raise ValueError("invalid span_id")

        if parent_id is None or not 0 <= parent_id < 2**64:
            raise ValueError("invalid parent_id")

        return cls(trace_id, parent_id, span_id)


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

    def configure_logging(self):  # pragma: nocover
        """Add request context to the logging system."""
        from .diagnostics.logging import LoggingBaseplateObserver
        self.register(LoggingBaseplateObserver())

    def configure_metrics(self, metrics_client):  # pragma: nocover
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

    def add_to_context(self, name, context_factory):  # pragma: nocover
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

    def make_root_span(self, context, name, trace_info=None, **kwargs):
        """Return a root span representing the current request we are handling.

        In a server, a root span represents the time spent on a single incoming
        request. Any calls made to downstream services will be new child spans
        of the root span, and the root span will in turn be the child span of
        whatever upstream request it is part of.

        :param context: The :term:`context object` for this request.
        :param str name: A name to identify the type of this request, e.g.
            a route or RPC method name.
        :param baseplate.core.TraceInfo trace_info: The trace context of this
            request as passed in from upstream. If :py:data:`None`, a new trace
            context will be generated.

        """

        if "trace_id" in kwargs:
            warn_deprecated("Passing trace_id/parent_id/span_id to "
                            "make_root_span is deprecated in favor of passing "
                            "a single TraceInfo object as trace_info.")

            trace_info = TraceInfo(
                trace_id=kwargs["trace_id"],
                parent_id=kwargs["parent_id"],
                span_id=kwargs["span_id"],
            )
        elif trace_info is None:
            trace_info = TraceInfo.new()

        root_span = RootSpan(trace_info.trace_id, trace_info.parent_id,
                             trace_info.span_id, name)
        for observer in self.observers:
            observer.on_root_span_created(context, root_span)
        return root_span


class Span(object):
    """A span represents a single RPC within a system."""

    # pylint: disable=invalid-name
    def __init__(self, trace_id, parent_id, span_id, name):
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.id = span_id
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
        calls :py:meth:`stop`.

        .. _context manager protocol:
            https://docs.python.org/3/reference/datamodel.html#context-managers

        """
        for observer in self.observers:
            observer.on_start()

    def annotate(self, key, value):
        """Add an annotation to the span.

        Annotations are arbitrary key/value pairs that add context and meaning
        to the span, such as a hostname or query string. Observers may
        interpret or ignore annotations as they desire.

        :param str key: The name of the annotation.
        :param str value: The value of the annotation.

        """
        for observer in self.observers:
            observer.on_annotate(key, value)

    def stop(self, error=None):
        """Record the end of the span.

        :param error: If the span ended because of an exception, this should
            be that exception. The default is :py:data:`None` which indicates
            normal exit.

        """
        for observer in self.observers:
            observer.on_stop(error=error)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, value, traceback):
        self.stop(error=value)


class RootSpan(Span):
    """A root span represents a request this server is handling.

    The root span is available on the :term:`context object` during requests as
    the ``trace`` attribute.

    """

    def make_child(self, name):
        """Return a child span representing an outbound service call."""
        span_id = random.getrandbits(64)
        span = Span(self.trace_id, self.id, span_id, name)
        for observer in self.observers:
            observer.on_child_span_created(span)
        return span
