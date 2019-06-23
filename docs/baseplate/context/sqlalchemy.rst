``baseplate.context.sqlalchemy``
================================

.. automodule:: baseplate.context.sqlalchemy

Configuration
-------------

.. autoclass:: baseplate.context.sqlalchemy.SQLAlchemySession

.. autofunction:: baseplate.context.sqlalchemy.engine_from_config

Classes
-------

.. autoclass:: baseplate.context.sqlalchemy.SQLAlchemyEngineContextFactory

.. autoclass:: baseplate.context.sqlalchemy.SQLAlchemySessionContextFactory

Runtime Metrics
---------------

In addition to request-level metrics reported through spans, this wrapper
reports connection pool statistics periodically via the :ref:`runtime-metrics`
system.  All metrics are prefixed as follows:

   {namespace}.runtime.{hostname}.PID{pid}.clients.{name}

where ``namespace`` is the application's namespace, ``hostname`` and ``pid``
come from the operating system, and ``name`` is the name given to
:py:meth:`~baseplate.core.Baseplate.add_to_context` when registering this
context factory.

The following metrics are reported:

``pool.size``
   The size limit for the connection pool.
``pool.open_and_available``
   How many connections have been established but are sitting available for use
   in the connection pool.
``pool.in_use``
   How many connections have been established and are currently checked out and
   being used.
``pool.overflow``
   How many connections beyond the pool size are currently being used. See
   :py:class:`sqlalchemy.pool.QueuePool` for more information.
