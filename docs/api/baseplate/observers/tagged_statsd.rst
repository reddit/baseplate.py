StatsD Tagged Metrics
=====================

The tagged metrics observer emits `StatsD`_-compatible time-series metrics
about the performance of your application with tags in the InfluxStatsD format.
The tags added to the metrics are configurable: any tags that pass through the
:py:meth:`~baseplate.Span.set_tag` function are filtered through a
user-supplied allowlist in the configuration file.

.. _`StatsD`: https://github.com/statsd/statsd

Configuration
-------------

Make sure your service calls
:py:meth:`~baseplate.Baseplate.configure_observers` during application startup
and then add the following to your configuration file to enable and configure
the StatsD tagged metrics observer.

.. code-block:: ini

   [app:main]

   ...

   # required to enable observer
   metrics.tagging = true

   # optional: which span tags should be attached to metrics. see below.
   #
   # `endpoint` and `client` are always allowed
   metrics.allowlist = foo, bar, baz

   # optional: the percent of statsd metrics to sample.
   #
   # if not specified, it will default to 100% (all metrics sent)
   metrics_observer.sample_rate = 100%

   ...

Tag Allowlist
-------------

Wavefront supports a maximum of 20 tags per cluster and 1000 distinct time
series per metric. Baseplate integrations of frameworks come out of the box
with some default tags set via :py:meth:`~baseplate.Span.set_tag()`, but to
append them to the metrics they must be present in the configuration file via
``metrics.allowlist``.

In order to find these tags to put in the allowlist, look through the code base
for calls to :py:meth:`~baseplate.Span.set_tag()` or check a zipkin trace in
Wavefront to see all the tags on a span.

Outputs
-------

For each span in the application, the metrics observer emits a
:py:class:`~baseplate.lib.metrics.Timer` tracking how long the span took and
increments a :py:class:`~baseplate.lib.metrics.Counter` for success or failure
of the span (failure being an unexpected exception).

A key differentiation from the :doc:`untagged StatsD metrics observer <statsd>` is that the emitted
outputs from baseplate no longer contain a namespace prefix. Prepending the namespace must be configured
in Telegraf via the ``name_prefix`` input plugin configuration.

For the :py:class:`~baseplate.ServerSpan` representing the request the server
is handling, the timer has a name like
``baseplate.server.latency,endpoint={route_or_method_name}`` and the counter
looks like
``baseplate.server.rate,success={True,False},endpoint={route_or_method_name}``.

For each span representing a call to a remote service or database, the timer
has a name like ``baseplate.clients.latency,client={name},endpoint={method}``
and the counter
``baseplate.clients.rate,client={name},endpoint={method},success={True,False}``.

When using :program:`baseplate-serve`, various process-level runtime metrics
will also be emitted. These are not tied to individual requests but instead
give insight into how the whole application is functioning. See
:ref:`runtime-metrics` for more information.

Direct Use
----------

When enabled, the metrics observer also adds a
:py:class:`~baseplate.lib.metrics.Client` object as an attribute named
``metrics`` to the :py:class:`~baseplate.RequestContext` which can take an optional
tags parameter in the form of a ``dict``::

   def my_handler(request):
       request.metrics.counter("foo", {"bar": "baz"}).increment()

To keep your application more generic, it's better to use local spans for
custom local timers and :py:meth:`~baseplate.Span.incr_tag` for custom
counters.
