StatsD Metrics
==============

The metrics observer emits `StatsD`_-compatible time-series metrics about the
performance of your application. These metrics are useful to get a
cross-sectional view of how your application is performing in a broad sense.

.. _`StatsD`: https://github.com/statsd/statsd

Configuration
-------------

Make sure your service calls
:py:meth:`~baseplate.Baseplate.configure_observers` during application startup
and then add the following to your configuration file to enable and configure
the StatsD metrics observer.

.. code-block:: ini

   [app:main]

   ...


   # required: the prefix added to all metrics emitted.
   # if present, the observer is enabled.
   metrics.namespace = myservice

   # optional: an endpoint to send the metrics datagrams to.
   # if not specified, metrics will only be emitted to debug logs.
   metrics.endpoint = statsd.local:8125

   # optional: the percent of statsd metrics to sample
   # if not specified, it will default to 100% (all metrics sent)
   # config must be passed to the `Baseplate` constructor to use this option
   metrics_observer.sample_rate = 100%

   ...

Outputs
-------

For each span in the application, the metrics observer emits a
:py:class:`~baseplate.lib.metrics.Timer` tracking how long the span took and
increments a :py:class:`~baseplate.lib.metrics.Counter` for success or failure
of the span (failure being an unexpected exception).

For the :py:class:`~baseplate.ServerSpan` representing the request the server
is handling, the timer has a name like
``{namespace}.server.{route_or_method_name}`` and the counter looks like
``{namespace}.server.{route_or_method_name}.{success,failure}``. If the request
:doc:`timed out <timeout>` an additional counter will be emitted with path
``{namespace}.server.{route_or_method_name}.timed_out``.

For each span representing a call to a remote service or database, the timer
has a name like ``{namespace}.clients.{context_name}.{method}`` and the counter
``{namespace}.clients.{context_name}.{method}.{success,failure}`` where
``context_name`` is the name of the client in the context configuration.

Calls to :py:meth:`~baseplate.Span.incr_tag` will increment a counter like
``{namespace}.{tag_name}`` by the amount specified.

When using :program:`baseplate-serve`, various process-level runtime metrics
will also be emitted. These are not tied to individual requests but instead
give insight into how the whole application is functioning. See
:ref:`runtime-metrics` for more information.

Direct Use
----------

When enabled, the metrics observer also adds a
:py:class:`~baseplate.lib.metrics.Client` object as an attribute named
``metrics`` to the :py:class:`~baseplate.RequestContext`::

   def my_handler(request):
       request.metrics.counter("foo").increment()

To keep your application more generic, it's better to use local spans for
custom local timers and :py:meth:`~baseplate.Span.incr_tag` for custom
counters.
