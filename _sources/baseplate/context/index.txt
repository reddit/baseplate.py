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

.. autoclass:: baseplate.context.cassandra.CQLMapperContextFactory


Memcache
--------

.. autofunction:: baseplate.context.memcache.pool_from_config

.. autoclass:: baseplate.context.memcache.MemcacheContextFactory
   :members:

.. autoclass:: baseplate.context.memcache.MonitoredMemcacheConnection
   :members:

.. autofunction:: baseplate.context.memcache.lib.decompress_and_load

.. autofunction:: baseplate.context.memcache.lib.make_dump_and_compress_fn

.. autofunction:: baseplate.context.memcache.lib.decompress_and_unpickle

.. autofunction:: baseplate.context.memcache.lib.make_pickle_and_compress_fn


Redis
-----

.. autofunction:: baseplate.context.redis.pool_from_config

.. autoclass:: baseplate.context.redis.RedisContextFactory
   :members:

.. autoclass:: baseplate.context.redis.MonitoredRedisConnection
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
