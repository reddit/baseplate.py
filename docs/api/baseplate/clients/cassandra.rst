``baseplate.clients.cassandra``
===============================

.. automodule:: baseplate.clients.cassandra

This integration supports both the base Python Cassandra driver and the
Cassandra ORM, CQLMapper.

To integrate it with your application, add the appropriate client declaration
to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": CassandraClient(),
         ...
      }
   )

configure it in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # required: a comma-delimited list of hosts to contact to find the ring
   foo.contact_points = cassandra-01.local, cassandra-02.local

   # optional: the port to connect to on each cassandra server
   # (default: 9042)
   foo.port = 9999

   # optional: the name of a CredentialSecret holding credentials for
   # authenticating to cassandra
   foo.credential_secret = secret/my_service/cassandra-foo

   ...


and then use it in request::

   def my_method(request):
       request.foo.execute("SELECT 1;")


Configuration
-------------

.. autoclass:: CassandraClient

.. autoclass:: CQLMapperClient

.. autofunction:: cluster_from_config

Classes
-------

.. autoclass:: CassandraContextFactory

.. autoclass:: CQLMapperContextFactory
