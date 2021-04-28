Sentry (Crash Reporting)
========================

The Sentry observer integrates `sentry-sdk`_ with your application to record
tracebacks for crashes to `Sentry`_.

.. versionchanged:: 2.0

   The underlying library for communicating with sentry was changed from Raven
   to sentry-sdk.

.. _sentry-sdk: https://docs.sentry.io/platforms/python/
.. _Sentry: https://sentry.io/welcome/

Configuration
-------------

Make sure your service calls
:py:meth:`~baseplate.Baseplate.configure_observers` during application startup
and then add the following to your configuration file to enable and configure
the Sentry observer.

.. code-block:: ini

   [app:main]

   ...

   # required to enable the observer
   # the DSN provided by Sentry for your project
   sentry.dsn = https://decaf:face@sentry.local/123

   # optional: the environment this application is running in
   sentry.environment = staging

   # optional: percent chance that a given error will be reported
   # (defaults to 100%)
   sentry.sample_rate = 37%

   # optional: comma-delimited list of fully qualified names of exception
   # classes to not report.
   sentry.ignore_errors = my_service.UninterestingException

   ...


Outputs
-------

Any unexpected exceptions that cause the request to crash (including outside
request context) will be reported to Sentry. The Trace ID of the current
request will be included in the context reported to Sentry.

Direct Use
----------

When enabled, the error reporting observer also adds a :py:class:`sentry_sdk.Hub`
object as an attribute named ``sentry`` to the
:py:class:`~baseplate.RequestContext`::

   def my_handler(request):
       try:
           ...
       except Exception:
           request.sentry.capture_exception()
