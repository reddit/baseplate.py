``baseplate.clients.thrift``
============================

`Thrift`_ is a cross-language framework for cross-service communication.
Developers write a language-independent definition of a service's API (the
"IDL") and Thrift's code generator makes server and client libraries for that
API.

.. _`Thrift`: https://thrift.apache.org/

.. automodule:: baseplate.clients.thrift

Example
-------

To add a Thrift client to your application, ensure that your service has
Baseplate.py's Thrift build step integrated in its root ``setup.py``::

   from baseplate.frameworks.thrift.command import ThriftBuildPyCommand

   ...

   setup(
      ...

      cmdclass={
         "build_py": ThriftBuildPyCommand,
      },
   )

Then add the downstream service's IDL to your application directory:

.. code-block:: console

   $ cp ~/src/other/other/other.thrift myservice/

Baseplate.py will automatically run the Thrift compiler on this when building
your application and put the output into a Python package within your
application like ``yourservice.other_thrift``. Import the client class and add
it to your context object::

   from .other_thrift import OtherService

   ...

   def make_wsgi_app(app_config):
      ...

      baseplate.configure_context(
         app_config,
         {
            ...
            "foo": ThriftClient(OtherService.Client),
            ...
         }
      )

then configure it in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # required: the host:port to find the service at
   foo.endpoint = localhost:9999

   # optional: the size of the connection pool (default 10)
   foo.size = 10

   # optional: how long a connection can be alive before we
   # recycle it (default 1 minute)
   foo.max_age = 1 minute

   # optional: how long before we time out when connecting
   # or doing an RPC (default 1 second)
   foo.timeout = 1 second

   # optional: how many times we'll retry connecting (default 3)
   foo.max_retries = 3

   ...


and finally use the attached client in request::

   def my_method(request):
       request.foo.is_healthy(
           request=IsHealthyRequest(probe=IsHealthyProbe.READINESS),
       )

Classes
-------

.. autoclass:: ThriftClient

.. autoclass:: ThriftContextFactory

Runtime Metrics
---------------

In addition to request-level metrics reported through spans, this wrapper
reports connection pool statistics periodically via the :ref:`runtime-metrics`
system.  All metrics are prefixed as follows:

.. code-block:: none

   {namespace}.runtime.{hostname}.PID{pid}.clients.{name}

where ``namespace`` is the application's namespace, ``hostname`` and ``pid``
come from the operating system, and ``name`` is the name given to
:py:meth:`~baseplate.Baseplate.add_to_context` when registering this
context factory.

The following metrics are reported:

``pool.size``
   The size limit for the connection pool.
``pool.in_use``
   How many connections have been established and are currently checked out and
   being used.
