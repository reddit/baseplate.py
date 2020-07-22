Server Timeouts
===============

The timeout observer ends processing of requests in your service if they take
too long. This is particularly important when an upstream service times out on
its end and retries requests to your services which will cause a pileup.

This is entirely configured in-service at the moment and no headers from
upstream services are yet taken into account.

.. warning::

   The timeout mechanism is entirely cooperative. If request processing is
   taking a long time because it is doing compute-heavy actions and not
   yielding to the event loop it might go on longer than the allotted timeout.

.. versionadded:: 1.2

.. versionchanged:: 1.3.3

   The default timeout was changed from 10 seconds to no timeout.  Having a
   default timeout was confusing and broke jobs like crons.

Configuration
-------------

Make sure your service calls
:py:meth:`~baseplate.Baseplate.configure_observers` during application startup.
By default, requests will not time out unless you configure them. The following
configuration settings allow you to customize this.

.. code-block:: ini

   [app:main]

   ...

   # optional: defaults to no timeout if not specified. this timeout
   # is used for any endpoint not specified in the by_endpoint
   # section below.
   # note: leaving this unconfigured is deprecated.
   # can be set to 'infinite' to disable the timeout altogether.
   server_timeout.default = 200 milliseconds

   # optional: defaults to false. if enabled, tracebacks will be
   # printed to the logs when timeouts occur.
   server_timeout.debug = true

   # optional: timeout values for specific endpoints. the name
   # used must match the name of the server span generated.
   # this overrides the default timeout.
   # - thrift services: the name of the thrift RPC method
   # - pyramid services: the name of the route (config.add_route)
   # can be set to 'infinite' to disable the timeout altogether.
   server_timeout.by_endpoint.is_healthy = 300 milliseoncds
   server_timeout.by_endpoint.my_method = 12 seconds

   ...

Outputs
-------

When a request times out, Baseplate.py will end the greenlet processing that
request and emit some diagnostics:

* A log entry like ``Server timed out processing for 'is_healthy' after 0.30
  seconds``. If ``server_timeout.debug`` was configured to ``True``, the full
  stack trace of the place the greenlet timed out will also be included.
* A :doc:`counter metric <statsd>` named
  ``{namespace}.server.{route_or_method_name}.timed_out``.
* A tag on the :doc:`server span sent to distributed tracing <tracing>`
  indicating ``timed_out=True``.
