``baseplate.observers``
=======================

.. automodule:: baseplate.observers

Observers watch Baseplate for events that happen during requests, such as
requests starting and ending and service calls being made. Observers can also
add attributes to the :py:class:`~baseplate.RequestContext` for your
application to use during the request.

To enable observers, call :py:meth:`~baseplate.Baseplate.configure_observers`
on your :py:class:`~baseplate.Baseplate` object during application startup and
supply the application configuration. See each observer below for what
configuration options are available.

.. code-block:: python

   def make_wsgi_app(app_config):
       baseplate = Baseplate(app_config)
       baseplate.configure_observers()

       ...

.. toctree::

   logging
   statsd
   tagged_statsd
   sentry
   tracing
   timeout
