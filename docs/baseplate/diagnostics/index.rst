``baseplate.diagnostics``
=========================

.. automodule:: baseplate.diagnostics

Observers
---------

Observers watch Baseplate for events that happen during requests, such as
requests starting and ending and service calls being made. Observers can also
add attributes to the :term:`context object` for your application to use during
the request. Under the hood, the context factories
(:py:mod:`baseplate.context`) are implemented as observers. All of the
following observers can be configured with :ref:`convenience_methods` on your
application's :py:class:`~baseplate.core.Baseplate` object.

.. autoclass:: baseplate.diagnostics.logging.LoggingBaseplateObserver

.. autoclass:: baseplate.diagnostics.metrics.MetricsBaseplateObserver

.. autoclass:: baseplate.diagnostics.sentry.SentryBaseplateObserver

.. autoclass:: baseplate.diagnostics.tracing.TraceBaseplateObserver
