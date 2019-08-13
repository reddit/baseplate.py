Sentry (Crash Reporting)
========================

The Sentry observer integrates `Raven`_ with your application to record
tracebacks for crashes to `Sentry`_.

.. _Raven: https://docs.sentry.io/clients/python/
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

   # optional: string to identify this client installation
   sentry.site = my site

   # optional: the environment this application is running in
   sentry.environment = staging

   # optional: comma-delimited list of module prefixes to ignore when
   # determining where an error came from
   sentry.exclude_paths = foo, bar

   # optional: comma-delimited list of module prefixes to include for
   # consideration when drilling down into an exception
   sentry.include_paths = foo, bar

   # optional: comma-delimited list of fully qualified names of exception
   # classes (potentially with * globs) to not report.
   sentry.ignore_exceptions = my_service.UninterestingException

   # optional: percent chance that a given error will be reported
   # (defaults to 100%)
   sentry.sample_rate = 37%

   # optional: comma-delimited list of fully qualified names of processor
   # classes to apply to events before sending to Sentry. defaults to
   # raven.processors.SanitizePasswordsProcessor
   sentry.processors = my_service.SanitizeTokenProcessor

   ...


Outputs
-------

Any unexpected exceptions that cause the request to crash (including outside
request context) will be reported to Sentry. The Trace ID of the current
request will be included in the context reported to Sentry.

Direct Use
----------

When enabled, the error reporting observer also adds a :py:class:`raven.Client`
object as an attribute named ``sentry`` to the
:py:class:`~baseplate.RequestContext`::

   def my_handler(request):
       try:
           ...
       except Exception:
           request.sentry.captureException()
