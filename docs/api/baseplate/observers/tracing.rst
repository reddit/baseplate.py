Tracing
=======

The tracing observer reports span information to a Zipkin-compatible
distributed trace aggregator. This can be used to build up cross-service views
of request processing across many services.

See `the OpenTracing overview`_ for more info on what tracing is and how it is
helpful to you.

.. _`the OpenTracing overview`: https://opentracing.io/docs/overview/

Configuration
-------------

Make sure your service calls
:py:meth:`~baseplate.Baseplate.configure_observers` during application startup
and then add the following to your configuration file to enable and configure
the tracing observer.

.. code-block:: ini

   [app:main]

   ...

   # required to enable observer
   # the name of the service reporting traces
   tracing.service_name = my_service

   # optional: traces won't be reported if not set
   # the name of the POSIX queue the trace publisher sidecar
   # is listening on
   tracing.queue_name = some-queue

   # optional: what percent of requests to report spans for
   # (defaults to 10%). note: this only kicks in if sampling
   # has not already been determined by an upstream service.
   tracing.sample_rate = 10%

   ...
