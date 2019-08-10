Logging
=======

The logging observer adds request-specific metadata to log lines coming out of
your application.

Configuration
-------------

No configuration is necessary, this observer is always enabled when you call
:py:meth:`~baseplate.Baseplate.configure_observers`.

If your application is run with :program:`baseplate-serve`, logging can be
controlled with :ref:`Python's standard logging configuration
<logging-config-fileformat>`. See :ref:`server-logging` for more information.

Outputs
-------

When used with :program:`baseplate-serve`, log lines look like::

   17905:7296338476964580186:baseplate.lib.metrics:DEBUG:Blah blah
   ^     ^                   ^                     ^     ^
   |     |                   |                     |     Log message
   |     |                   |                     Log level
   |     |                   Name of the logger
   |     Trace ID of the request
   Process ID

Direct Use
----------

Any log messages emitted with the Python standard :py:mod:`logging` interfaces
will be annotated by this observer.
