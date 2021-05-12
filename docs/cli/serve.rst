``baseplate-serve``
===================

Baseplate comes with a simple Gevent-based server for both Thrift and WSGI
applications called ``baseplate-serve``.

Configuration
-------------

There is one required parameter on the command line, the path to an INI-format
configuration file. There should be two sections in the file: the ``server``
section and the ``app`` section. The section headers look like ``server:main``
or ``app:main`` where the part before the ``:`` is the type of section and the
part after is the "name". Baseplate looks for sections named ``main`` by
default but can be overridden with the ``--server-name`` and ``--app-name``
options.

.. highlight:: ini

The Server
----------

Here's an example of a ``server`` section::

   [server:main]
   factory = baseplate.server.thrift
   stop_timeout = 30

The ``factory`` tells baseplate what code to use to run the server. Baseplate
comes with two servers built in:

``baseplate.server.thrift``
   A Gevent Thrift server.

``baseplate.server.wsgi``
   A Gevent WSGI server.

Both take two configuration values as well:

``max_concurrency``
   The maximum number of simultaneous clients the server will handle. Note that
   this is how many connections will be accepted, but some of those connections
   may be idle at any given time.

``stop_timeout``
   (Optional) How long, in seconds, to wait for active connections to finish up
   gracefully when shutting down. By default, the server will shut down
   immediately.

The WSGI server takes an additional optional parameter:

``handler``
   A full name of a class which subclasses
   ``gevent.pywsgi.WSGIHandler`` for extra functionality.

There are some additional configuration settings in this section that start
with a ``monitoring`` prefix. For more information on those, see `Process-level
metrics`_.


The Application
---------------

And now the real bread and butter, your ``app`` section::

   [app:main]
   factory = my_app.processor:make_processor
   foo = 3
   bar = 22
   noodles.blah = one, two, three

The ``app`` section also takes a ``factory``.  This should be the name of a
callable in your code which builds and returns your application. The part
before the ``:`` is a Python module. The part after the ``:`` is the name of a
callable object within that module.

The rest of the options in the ``app`` section of the configuration file get
passed as a dictionary to your application callable. You can parse these
options with :py:mod:`baseplate.lib.config`.

The application factory should return an appropriate object for your server:

Thrift
   A ``TProcessor``.

WSGI
   A WSGI callable.

.. _server-logging:

Logging
-------

The baseplate server provides a default configuration for the Python standard
``logging`` system. The root logger will print to ``stdout`` with a format that
includes trace information. The default log level is ``INFO`` or ``DEBUG`` if
the ``--debug`` flag is passed to ``baseplate-serve``.

If more complex logging configuration is necessary, the configuration file will
override the default setup. The :ref:`logging-config-fileformat` is documented
in the standard library.

Automatic reload on source changes
----------------------------------

In development, it's useful for the server to restart itself when you change
code.  You can do this by passing the ``--reload`` flag to ``baseplate-serve``.

This should not be used in production environments.

Einhorn
-------

``baseplate-serve`` can run as a worker in `Stripe's Einhorn socket manager`_.
This allows Einhorn to handle binding the socket, worker management, rolling
restarts, and worker health checks.

Baseplate supports Einhorn's "manual ACK" protocol. Once the application is
loaded and ready to serve, Baseplate notifies the Einhorn master process via
its command socket.

An example command line::

   einhorn -m manual -n 4 --bind localhost:9190 \
      baseplate-serve myapp.ini

.. _Stripe's Einhorn socket manager: https://github.com/stripe/einhorn

Graceful shutdown
-----------------

The flow of graceful shutdown while handling live traffic looks like this:

* The server receives a ``SIGTERM`` from the infrastructure.
* The server sets ``baseplate.server.SERVER_STATE.shutting_down`` to ``True``.
* If the ``drain_time`` setting is set in the server configuration, the server
  will wait the specified amount of time before continuing to the next step.
  This gives your application a chance to use the ``shutting_down`` flag in
  healthcheck responses.
* The server begins graceful shutdown. No new connections will be accepted. The
  server will continue processing the existing in-flight requests until they
  are all done or ``stop_timeout`` time has elapsed.
* The server exits and lets the infrastructure clean up.

During the period between receiving the ``SIGTERM`` and the server exiting, the
application may still be routed new requests. To ensure requests aren't lost
during the graceful shutdown (where they won't be listened for) your
application should set an appropriate ``drain_time`` and use the
``SERVER_STATE.shutting_down`` flag to fail ``READINESS`` healthchecks.

For example:

.. code-block:: py

   def is_healthy(self, context, healthcheck_request):
       if healthcheck_request.probe == IsHealthyProbe.LIVENESS:
           return True
       elif healthcheck_request.probe == IsHealthyProbe.READINESS:
           if SERVER_STATE.shutting_down:
               return False
           return True
       return True


Debug Signal
------------

Applications running under ``baseplate-serve`` will respond to ``SIGUSR1`` by
printing a stack trace to the logger. This can be useful for debugging
deadlocks and other issues.

Note that Einhorn will exit if you send it a ``SIGUSR1``. You can instead open up
``einhornsh`` and instruct the master to send the signal to all workers:

.. code-block:: console

   $ einhornsh
   > signal SIGUSR1
   Successfully sent USR1s to 4 processes: [...]

.. _runtime-metrics:

Process-level metrics
---------------------

If your application has registered a metrics client with
:py:meth:`~baseplate.Baseplate.configure_observers`, ``baseplate-serve``
will automatically send process-level metrics every 10 seconds. Which metrics
are sent depends on your server configuration, for example::

   [server:main]
   factory = baseplate.server.thrift

   monitoring.blocked_hub = 100 milliseconds
   monitoring.concurrency = true

will enable the ``blocked_hub`` reporter (configuring it to trigger at a 100ms
threshold) and the ``concurrency`` reporter (which has no special
configuration).

The following reporters are available:

``monitoring.blocked_hub``
   Enabled if a valid :py:func:`~baseplate.lib.config.Timespan` is set, defaults to disabled.

   This will turn on `Gevent's monitoring thread`_ and report events indicating
   that Gevent detects the main event loop was blocked by a greenlet for longer
   than the given time span. This can indicate excessive CPU usage causing
   event loop starvation or the use of non-patched blocking IO calls. More
   detailed information, including stack traces, is also printed to the logging
   system.

   Each instance of the hub being blocked will be reported as a
   :py:class:`~baseplate.lib.metrics.Timer` measuring the duration of the blockage.

   Note: the performance impact of this reporter is not currently understood.
   Watch your metrics closely if you turn this on.

``monitoring.concurrency``
   Enabled if ``true``, disabled if ``false``. Defaults to enabled.

   This will track the number of in-flight requests being processed
   concurrently by this server process.

   At each report interval, this will update two
   :py:class:`~baseplate.lib.metrics.Gauge` metrics with the current number of
   open connections (``open_connections``) and current number of in-flight
   requests being processed concurrently (``active_requests``).

``monitoring.connection_pool``
   Enabled if ``true``, disabled if ``false``. Defaults to disabled.

   This will track the usage of connection pools for various clients in the
   application. The metrics generated will depend on which clients are used.

``monitoring.gc.stats``
   Enabled if ``true``, disabled if ``false``. Defaults to enabled.

   This will report the Python garbage collector's statistics to the metrics system.

   At each report interval, this will update gauges with the current values
   returned by :py:func:`gc.get_stats()`.

``monitoring.gc.timing``
   Enabled if ``true``, disabled if ``false``. Defaults to disabled.

   This will track the duration of time taken by Python's garbage collector
   doing a collection sweep.

   The duration of each pass of the garbage collector will be reported as a
   timer.

   Note: the performance impact of this reporter is not currently understood.
   Watch your metrics closely if you turn this on.

``monitoring.gc.refcycle``
   Enabled if a path to a writable directory is set, defaults to disabled.

   **This should only be used in debugging, it will certainly have a negative
   performance impact.**

   This will turn off automatic garbage collection and instead run a sweep
   every reporting interval. Any objects found by the collector will be graphed
   using `objgraph`_ to help find reference cycles. The resulting graphs images
   will be written to the directory specified.

   The ``objgraph`` library and ``graphviz`` package must be installed for this
   to work properly.

All metrics generated by ``baseplate-serve`` are prefixed with ``runtime`` and
are tagged with ``hostname`` and ``PID``.

.. versionchanged:: 2.0

   The ``hostname`` and ``PID`` fields of the metric name were moved to tags.

.. _Gevent's monitoring thread: http://www.gevent.org/monitoring.html#the-monitor-thread
.. _objgraph: https://mg.pov.lt/objgraph/
