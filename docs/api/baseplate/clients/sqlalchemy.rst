``baseplate.clients.sqlalchemy``
================================

`SQLAlchemy`_ is an ORM and general-purpose SQL engine for Python. It can work
with many different SQL database backends. Reddit generally uses it to talk to
`PostgreSQL`_.

.. _`SQLAlchemy`: https://www.sqlalchemy.org/
.. _`PostgreSQL`: https://www.postgresql.org/

.. automodule:: baseplate.clients.sqlalchemy

Example
-------

To integrate SQLAlchemy with your application, add the appropriate client
declaration to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": SQLAlchemySession(),
         ...
      }
   )

configure it in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # required: sqlalchemy URL describing a database to connect to
   foo.url = postgresql://postgres.local:6543/bar

   # optional: the name of a CredentialSecret holding credentials for
   # authenticating to the database
   foo.credentials_secret = secret/my_service/db-foo

   ...


and then use the attached :py:class:`~sqlalchemy.orm.session.Session` object in
request::

   def my_method(request):
       request.foo.query(MyModel).filter_by(...).all()

Configuration
-------------

.. autoclass:: SQLAlchemySession

.. autofunction:: engine_from_config

Classes
-------

.. autoclass:: SQLAlchemyEngineContextFactory

.. autoclass:: SQLAlchemySessionContextFactory

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
``pool.open_and_available``
   How many connections have been established but are sitting available for use
   in the connection pool.
``pool.in_use``
   How many connections have been established and are currently checked out and
   being used.
``pool.overflow``
   How many connections beyond the pool size are currently being used. See
   :py:class:`sqlalchemy.pool.QueuePool` for more information.
