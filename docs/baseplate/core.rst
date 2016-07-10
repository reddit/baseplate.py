baseplate.core
==============

The heart of the Baseplate framework is its diagnostics system. Here's an
incomplete example of an application built on the framework::

   def do_something(request):
       request.some_redis_client.ping()

   def make_app(app_config):
       ... snip ...

       baseplate = Baseplate()
       baseplate.configure_metrics(metrics_client)
       baseplate.add_to_context(
           "some_redis_client", RedisContextFactory(redis_pool))

       ... snip ...

When a request is made which routes to the ``do_something`` handler, a
:py:class:`~baseplate.core.RootSpan` is automatically created. If the incoming
request has trace headers, we construct the root span to be identical to the
child span in the upstream service. When we call
``request.some_redis_client.ping()`` in the handler, Baseplate will create a
child :py:class:`~baseplate.core.Span` object to represent the time taken
talking to redis.

The creation of the root and child spans will trigger updates on all the
:py:class:`~baseplate.core.RootSpanObserver` and
:py:class:`~baseplate.core.SpanObserver` objects registered.  Because we called
``baseplate.configure_metrics`` in our setup, this means we have observers that
send statsd metrics so Baseplate will automatically send metrics on how long it
took our application to ``do_something`` and how long Redis took to respond to
our ``ping`` to statsd/Graphite without any extra code in our application.

.. note::

   The documentation below explains how all this works under the hood. If you
   just want to write an application, you can skip on to :doc:`how to integrate
   Baseplate with your application framework <integration/index>` or :doc:`how to
   add client libraries to the context object <context/index>`.

.. automodule:: baseplate.core

Baseplate
---------

At the root of each application is a single instance of
:py:class:`~baseplate.core.Baseplate`. This object can be integrated with
various other frameworks (e.g. Thrift, Pyramid, etc.) using one of :doc:`the
integrations <integration/index>`.

.. autoclass:: Baseplate
   :members:

.. autoclass:: TraceInfo
   :members: from_upstream

Spans
-----

Each time a new request comes in to be served, the time taken to handle the
request is represented by a new :py:class:`~baseplate.core.RootSpan` instance.
During the course of handling that request, our application might make calls to
remote services or do expensive calculations, the time spent can be represented
by child :py:class:`~baseplate.core.Span` instances.

Spans have names and IDs and track their parent relationships. When calls are
made to remote services, the information that identifies the local child span
representing that service call is passed along to the remote service and
becomes the root span in the remote service. This allows requests to be traced
across the infrastructure.

Small bits of data, called annotations, can be attached to spans as well. This
could be the URL fetched, or how many items were sent in a batch, or whatever
else might be helpful.

.. autoclass:: RootSpan
   :members:
   :inherited-members:

.. autoclass:: Span
   :members:

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

.. autoclass:: BaseplateObserver
   :members:

.. autoclass:: RootSpanObserver
   :members:
   :inherited-members:

.. autoclass:: SpanObserver
   :members:

Convenience
-----------

Baseplate comes with some core monitoring observers built in and just requires
you to configure them. You can enable them by calling the relevant methods on
your application's :py:class:`~baseplate.core.Baseplate` object.

- Logging: :py:meth:`~baseplate.core.Baseplate.configure_logging`
- Metrics (statsd): :py:meth:`~baseplate.core.Baseplate.configure_metrics`

Additionally, Baseplate provides helpers which can be attached to the
:term:`context object` in requests. These helpers make the passing of trace
information and collection of spans automatic and transparent. Because this
pattern is so common, Baseplate has a special kind of observer for it which can
be registered with :py:meth:`~baseplate.core.Baseplate.add_to_context`. See the
:py:mod:`baseplate.context` package for a list of helpers included.
