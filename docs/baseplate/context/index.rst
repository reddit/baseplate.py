baseplate.context
=================

.. automodule:: baseplate.context


Services
--------

.. autoclass:: baseplate.context.thrift.ThriftContextFactory


Cassandra
---------

.. autofunction:: baseplate.context.cassandra.cluster_from_config

.. autoclass:: baseplate.context.cassandra.CassandraContextFactory


Redis
-----

.. autofunction:: baseplate.context.redis.pool_from_config

.. autoclass:: baseplate.context.redis.RedisContextFactory
   :members:

.. autoclass:: baseplate.context.redis.MonitoredRedisConnection
   :members:


Memcache
--------

.. autofunction:: baseplate.context.memcache.pool_from_config

.. autoclass:: baseplate.context.memcache.MemcacheContextFactory
   :members:

.. autoclass:: baseplate.context.memcache.MonitoredMemcacheConnection
   :members:


SQLAlchemy
----------

.. autoclass:: baseplate.context.sqlalchemy.SQLAlchemyEngineContextFactory

.. autoclass:: baseplate.context.sqlalchemy.SQLAlchemySessionContextFactory


DIY: The Factory
----------------

.. note::

   Stuff beyond this point is only useful if you are implementing your own
   context helpers. You can stop reading here if the stuff above already has
   you covered!


.. autoclass:: baseplate.context.ContextFactory
   :members:
