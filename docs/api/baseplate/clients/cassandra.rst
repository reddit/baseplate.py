``baseplate.clients.cassandra``
===============================

.. automodule:: baseplate.clients.cassandra

Cassandra_ is a database designed for high-availability, high write throughput,
and eventual consistency.

Baseplate.py supports both the base :doc:`Python Cassandra driver
<cassandra:index>` and the Cassandra ORM, `CQLMapper`_.

.. _`Cassandra`: https://cassandra.apache.org/

.. _`CQLMapper`: https://github.com/reddit/cqlmapper

Example
-------

To integrate the Cassandra driver with your application, add the appropriate
client declaration to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": CassandraClient("mykeyspace"),
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


and then use the attached :py:class:`~cassandra.cluster.Session`-like object in
request::

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
