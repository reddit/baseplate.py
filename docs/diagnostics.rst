================================
Diagnostics: Spans and Observers
================================

Spans
-----

The heart of Baseplate is its diagnostics system.

At the root of each application is a single instance of
:py:class:`~baseplate.core.Baseplate`. Each time a new request comes in to be
served, the time taken to handle the request is represented by a new
:py:class:`~baseplate.core.RootSpan` instance. During the course of handling
that request, the local service might make calls to remote services or do
expensive calculations, the time spent in these actions is represented by child
:py:class:`~baseplate.core.Span` instances.

Spans have names and IDs and track their parent relationships. When calls are
made to remote services, the names, IDs, and other data are passed along as
possible so that requests can be correlated across the whole system.

Small bits of data, called annotations, can be attached to spans as well. This
could be the URL fetched, or how many items were sent in a batch, or whatever
else might be helpful.

Observers
---------

To actually do something with all these spans, Baseplate provides observer
interfaces which receive notification of events happening in the application
via calls to various methods.

The base type of observer is :py:class:`~baseplate.core.BaseplateObserver`
which can be registered with the root :py:class:`~baseplate.core.Baseplate`
instance using the :py:meth:`~baseplate.core.Baseplate.register` method.
Whenever a new root span is created in your application (i.e. a new request
comes in to be served) the observer has its
:py:meth:`~baseplate.core.BaseplateObserver.on_root_span_created` method called
with the relevant details. If this method returns an instance of
:py:class:`~baseplate.core.RootSpanObserver`, the returned observer will be
registered to receive events from the new root span.

Spans, and root spans, can be notified of three common events:
:py:meth:`~baseplate.core.SpanObserver.on_start`,
:py:meth:`~baseplate.core.SpanObserver.on_annotate`, and
:py:meth:`~baseplate.core.SpanObserver.on_stop`. These represent the span
starting, having a custom annotation added, and ending, respectively.

Additionally, :py:class:`~baseplate.core.RootSpanObserver` has one extra event,
:py:meth:`~baseplate.core.RootSpanObserver.on_child_span_created`. This method
is called when a new child span is created in the application for e.g. a call
to a remote service or an expensive computation. If this method returns an
instance of :py:class:`~baseplate.core.SpanObserver`, the returned observer
will be registered to receive events from the new child span.

It's up to the observers to attach meaning to these events. For example, the
metrics observer would start a timer
:py:meth:`~!baseplate.core.SpanObserver.on_start` and record the elapsed time to
statsd :py:meth:`~!baseplate.core.SpanObserver.on_stop`.


Convenience
-----------

Baseplate comes with some core monitoring observers built in and just requires
you to configure them. You can enable them by calling the relevant methods on
your application's :py:class:`baseplate.core.Baseplate` object.

- Logging: :py:meth:`~baseplate.core.Baseplate.configure_logging`
- Metrics (statsd): :py:meth:`~baseplate.core.Baseplate.configure_metrics`

Additionally, Baseplate provides helpers which can be attached to the
:term:`context object` in requests. These helpers make the passing of trace
information and collection of spans automatic and transparent. Because this
pattern is so common, Baseplate has a special kind of observer for it which can
be registered with :py:meth:`~baseplate.core.Baseplate.add_to_context`. See the
:py:mod:`baseplate.context` package for a list of helpers included.
