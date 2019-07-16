``baseplate.clients``
=====================

.. automodule:: baseplate.clients

Instrumented Client Libraries
-----------------------------

.. toctree::
   :titlesonly:

   baseplate.clients.cassandra: Cassandra CQL Client <cassandra>
   baseplate.clients.hvac: Client for using Vault's advanced features <hvac>
   baseplate.clients.kombu: Client for publishing to queues <kombu>
   baseplate.clients.memcache: Memcached Client <memcache>
   baseplate.clients.redis: Redis Client <redis>
   baseplate.clients.sqlalchemy: SQL Client for relational databases (e.g. PostgreSQL) <sqlalchemy>
   baseplate.clients.thrift: Thrift client for RPC to other backend services <thrift>

DIY: The Factory
----------------

If a library you want isn't supported here, it can be added to your own
application by implementing :py:class:`ContextFactory`.

.. autoclass:: ContextFactory
   :members:
