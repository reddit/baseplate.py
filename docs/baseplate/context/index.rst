``baseplate.context``
=====================

.. automodule:: baseplate.context

Instrumented Client Libraries
-----------------------------

.. toctree::
   :titlesonly:

   baseplate.context.cassandra: Cassandra CQL Client <cassandra>
   baseplate.context.hvac: Client for using Vault's advanced features <hvac>
   baseplate.context.memcache: Memcached Client <memcache>
   baseplate.context.redis: Redis Client <redis>
   baseplate.context.sqlalchemy: SQL Client for relational databases (e.g. PostgreSQL) <sqlalchemy>
   baseplate.context.thrift: Thrift client for RPC to other backend services <thrift>

DIY: The Factory
----------------

If a library you want isn't supported here, it can be added to your own
application by implementing :py:class:`~baseplate.context.ContextFactory`.

.. autoclass:: baseplate.context.ContextFactory
   :members:
