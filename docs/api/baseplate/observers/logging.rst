Logging
=======

The logging observer adds request-specific metadata to log lines coming out of
your application.

.. versionchanged:: 1.4
   Logs are now formatted as JSON objects.

Configuration
-------------

No configuration is necessary, this observer is always enabled when you call
:py:meth:`~baseplate.Baseplate.configure_observers`.

If your application is run with :program:`baseplate-serve`, logging can be
controlled with :ref:`Python's standard logging configuration
<logging-config-fileformat>`. See :ref:`server-logging` for more information.

Outputs
-------

The :program:`baseplate-serve` command will automatically set up log
formatting.

When directly used from a TTY, a simplified human-readable message format is
emitted. Only the level and message are included.

In production usage, log entries are formatted as JSON objects that can be
parsed automatically by log analysis systems. Log entry objects contain the
following keys:

``message``
   The message.

``level``
   The name of the log level at which the log entry was generated, e.g.
   ``INFO``, ``WARNING``, etc.

   Along with ``name``, this can be useful for :ref:`configuring logging to
   squelch noisy messages <server-logging>`.

``name``
   The name of the :py:class:`~logging.Logger` used.

   Along with ``level``, this can be useful for :ref:`configuring logging to
   squelch noisy messages <server-logging>`.

``traceID``
   The Trace ID of the request within context of which the log entry was
   generated. This can be used to correlate log entries from the same root
   request within and across services.

   Only present if the log entry was generated during a request. Otherwise see
   ``thread``.

``pathname``
   The path to the Python source for the module that generated the log entry.

``module``
   The name of the module in which the log entry was generated.

``funcName``
   The name of the function that generated the log entry.

``lineno``
   The line number on which the log entry was generated.

``process``
   The OS-level Process ID of the process that generated the log entry.

``processName``
   The name of the process that generated the log entry (as set on
   :py:attr:`multiprocessing.current_process().name
   <multiprocessing.Process.name>`).

``thread``
   The name of the thread that generated the log entry (as set on
   :py:attr:`threading.current_thread().name <threading.Thread.name>`).

   This may be absent if the log entry was generated from within processing of
   a request, in which case ``traceID`` will be included instead.

Older logging
-------------

Before v1.4, log entries were written in a custom format::


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
