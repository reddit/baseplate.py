``baseplate.clients.thrift``
============================

.. automodule:: baseplate.clients.thrift

.. autoclass:: ThriftClient

.. autoclass:: ThriftContextFactory

Runtime Metrics
---------------

In addition to request-level metrics reported through spans, this wrapper
reports connection pool statistics periodically via the :ref:`runtime-metrics`
system.  All metrics are prefixed as follows:

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
