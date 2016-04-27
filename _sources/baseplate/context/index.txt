baseplate.context
=================

.. automodule:: baseplate.context


Services
--------

.. autoclass:: baseplate.context.thrift.ThriftContextFactory


Cassandra
---------

.. autoclass:: baseplate.context.cassandra.CassandraContextFactory


Redis
-----

.. autoclass:: baseplate.context.redis.RedisContextFactory
   :members:

.. autoclass:: baseplate.context.redis.MonitoredRedisConnection
   :members:


DIY: The Factory
----------------

.. note::

   Stuff beyond this point is only useful if you are implementing your own
   context helpers. You can stop reading here if the stuff above already has
   you covered!


.. autoclass:: baseplate.context.ContextFactory
   :members:
