``baseplate``
=============

The heart of the Baseplate framework is its telemetry system. Here's an
incomplete example of an application built with the framework::

   def do_something(request):
       result = request.db.execute("SELECT date('now');")

   def make_app(app_config):
       ... snip ...

       baseplate = Baseplate(app_config)
       baseplate.configure_observers()
       baseplate.configure_context({"db": SQLAlchemySession()})

       ... snip ...

When a request is made that routes to the ``do_something`` handler, a
:py:class:`~baseplate.ServerSpan` is automatically created to track the time
spent processing the request in our application. If the incoming request has
trace headers, the constructed server span will have the same IDs as the
upstream service's child span or else it will start a new trace with randomized
values.

When we call ``request.db.execute(...)`` in the handler, Baseplate creates a
child :py:class:`~baseplate.Span` object to represent the time taken running
the query.

The creation of the server and child spans trigger callbacks on all the
:py:class:`~baseplate.ServerSpanObserver` and
:py:class:`~baseplate.SpanObserver` objects registered.  Because we called
:py:meth:`~baseplate.Baseplate.configure_observers` in our setup, this means we
have observers that send telemetry about how our service is functioning.

.. automodule:: baseplate

Framework Configuration
-----------------------

At the root of each application is a single instance of
:py:class:`~baseplate.Baseplate`. This object can be integrated with
various other frameworks (e.g. Thrift, Pyramid, etc.) using one of :doc:`the
integrations <frameworks/index>`.

.. autoclass:: Baseplate
   :members: __init__, configure_observers, configure_context, add_to_context

Per-request Context
-------------------

Each time a new request comes in to be served, the time taken to handle the
request is represented by a new :py:class:`~baseplate.ServerSpan` instance.
During the course of handling that request, our application might make calls to
remote services or do expensive calculations, the time spent can be represented
by child :py:class:`~baseplate.Span` instances.

Spans have names and IDs and track their parent relationships. When calls are
made to remote services, the information that identifies the local child span
representing that service call is passed along to the remote service and
becomes the server span in the remote service. This allows requests to be traced
across the infrastructure.

Small bits of data, called annotations, can be attached to spans as well. This
could be the URL fetched, or how many items were sent in a batch, or whatever
else might be helpful.

.. autoclass:: RequestContext

.. automethod:: Baseplate.make_context_object
.. automethod:: Baseplate.make_server_span
.. automethod:: Baseplate.server_context

.. autoclass:: ServerSpan
   :members:
   :inherited-members:

.. autoclass:: Span
   :members:

.. autoclass:: TraceInfo
   :members:

Observers
---------

To actually do something with all these spans, Baseplate provides observer
interfaces which receive notification of events happening in the application
via calls to various methods.

The base type of observer is :py:class:`~baseplate.BaseplateObserver` which can
be registered with the root :py:class:`~baseplate.Baseplate` instance using the
:py:meth:`~baseplate.Baseplate.register` method.  Whenever a new server span is
created in your application (i.e. a new request comes in to be served) the
observer has its :py:meth:`~baseplate.BaseplateObserver.on_server_span_created`
method called with the relevant details. This method can register
:py:class:`~baseplate.ServerSpanObserver` instances with the new server span to
receive events as they happen.

Spans can be notified of five common events:

* :py:meth:`~baseplate.SpanObserver.on_start`, the span started.
* :py:meth:`~baseplate.SpanObserver.on_set_tag`, a tag was set on the span.
* :py:meth:`~baseplate.SpanObserver.on_log`, a log was entered on the span.
* :py:meth:`~baseplate.SpanObserver.on_finish`, the span finished.
* :py:meth:`~baseplate.SpanObserver.on_child_span_created`, a new child span was created.

New child spans are created in the application automatically by various client
library wrappers e.g. for a call to a remote service or database, and can also
be created explicitly for local actions like expensive computations.  The
handler can register new :py:class:`~baseplate.SpanObserver` instances with the
new child span to receive events as they happen.

It's up to the observers to attach meaning to these events. For example, the
metrics observer would start a timer
:py:meth:`~!baseplate.SpanObserver.on_start` and record the elapsed time to
StatsD :py:meth:`~!baseplate.SpanObserver.on_finish`.

.. automethod:: Baseplate.register

.. autoclass:: BaseplateObserver
   :members:

.. autoclass:: ServerSpanObserver
   :members:
   :inherited-members:

.. autoclass:: SpanObserver
   :members:

Legacy Methods
--------------

.. automethod:: Baseplate.configure_logging
.. automethod:: Baseplate.configure_metrics
.. automethod:: Baseplate.configure_tracing
.. automethod:: Baseplate.configure_error_reporting

