``baseplate.core``
==================

The heart of the Baseplate framework is its diagnostics system. Here's an
incomplete example of an application built with the framework::

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
:py:class:`~baseplate.core.ServerSpan` is automatically created to represent
the time spent processing the request in our application. If the incoming
request has trace headers, the constructed server span will have the same IDs
as the upstream service's child span.

When we call ``request.some_redis_client.ping()`` in the handler, Baseplate
will create a child :py:class:`~baseplate.core.Span` object to represent the
time taken talking to redis.

The creation of the server and child spans will trigger updates on all the
:py:class:`~baseplate.core.ServerSpanObserver` and
:py:class:`~baseplate.core.SpanObserver` objects registered.  Because we called
``baseplate.configure_metrics`` in our setup, this means we have observers that
send statsd metrics so Baseplate will automatically send metrics on how long it
took our application to ``do_something`` and how long Redis took to respond to
our ``ping`` to statsd/Graphite without any extra code in our application.

.. note::

   The documentation below explains how all this works under the hood. If you
   just want to write an application, you can skip on to :doc:`how to integrate
   Baseplate with your application framework <integration/index>` or :doc:`how
   to use client libraries with diagnostic instrumentation <context/index>`.

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
request is represented by a new :py:class:`~baseplate.core.ServerSpan` instance.
During the course of handling that request, our application might make calls to
remote services or do expensive calculations, the time spent can be represented
by child :py:class:`~baseplate.core.Span` instances.

Spans have names and IDs and track their parent relationships. When calls are
made to remote services, the information that identifies the local child span
representing that service call is passed along to the remote service and
becomes the server span in the remote service. This allows requests to be traced
across the infrastructure.

Small bits of data, called annotations, can be attached to spans as well. This
could be the URL fetched, or how many items were sent in a batch, or whatever
else might be helpful.

.. autoclass:: ServerSpan
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
Whenever a new server span is created in your application (i.e. a new request
comes in to be served) the observer has its
:py:meth:`~baseplate.core.BaseplateObserver.on_server_span_created` method
called with the relevant details. This method can register
:py:class:`~baseplate.core.ServerSpanObserver` instances with the new server
span to receive events as they happen.

Spans can be notified of five common events:

* :py:meth:`~baseplate.core.SpanObserver.on_start`, the span started.
* :py:meth:`~baseplate.core.SpanObserver.on_set_tag`, a tag was set on the span.
* :py:meth:`~baseplate.core.SpanObserver.on_log`, a log was entered on the span.
* :py:meth:`~baseplate.core.SpanObserver.on_finish`, the span finished.
* :py:meth:`~baseplate.core.SpanObserver.on_child_span_created`, a new child span was created.

New child spans are created in the application automatically by various client
library instrumentations e.g. for a call to a remote service or database, and
can also be created explicitly for local actions like expensive computations.
The handler can register new :py:class:`~baseplate.core.SpanObserver` instances
with the new child span to receive events as they happen.

It's up to the observers to attach meaning to these events. For example, the
metrics observer would start a timer
:py:meth:`~!baseplate.core.SpanObserver.on_start` and record the elapsed time to
statsd :py:meth:`~!baseplate.core.SpanObserver.on_finish`.

.. autoclass:: BaseplateObserver
   :members:

.. autoclass:: ServerSpanObserver
   :members:
   :inherited-members:

.. autoclass:: SpanObserver
   :members:


.. _convenience_methods:

Convenience Methods
-------------------

Baseplate comes with some core monitoring observers built in and just requires
you to configure them. You can enable them by calling the relevant methods on
your application's :py:class:`~baseplate.core.Baseplate` object.

- Logging: :py:meth:`~baseplate.core.Baseplate.configure_logging`
- Metrics (statsd): :py:meth:`~baseplate.core.Baseplate.configure_metrics`
- Tracing (Zipkin): :py:meth:`~baseplate.core.Baseplate.configure_tracing`
- Error Reporting (Sentry): :py:meth:`~baseplate.core.Baseplate.configure_error_reporting`

Additionally, Baseplate provides helpers which can be attached to the
:term:`context object` in requests. These helpers make the passing of trace
information and collection of spans automatic and transparent. Because this
pattern is so common, Baseplate has a special kind of observer for it which can
be registered with :py:meth:`~baseplate.core.Baseplate.add_to_context`. See the
:py:mod:`baseplate.context` package for a list of helpers included.
