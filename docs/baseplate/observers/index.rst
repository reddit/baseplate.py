``baseplate.observers``
=======================

.. automodule:: baseplate.observers

Observers
---------

Observers watch Baseplate for events that happen during requests, such as
requests starting and ending and service calls being made. Observers can also
add attributes to the :term:`context object` for your application to use during
the request. Under the hood, the context factories
(:py:mod:`baseplate.clients`) are implemented as observers. All of the
following observers can be configured with :ref:`convenience_methods` on your
application's :py:class:`~baseplate.Baseplate` object.

.. autoclass:: baseplate.observers.logging.LoggingBaseplateObserver

.. autoclass:: baseplate.observers.metrics.MetricsBaseplateObserver

.. autoclass:: baseplate.observers.sentry.SentryBaseplateObserver

.. autoclass:: baseplate.observers.tracing.TraceBaseplateObserver
