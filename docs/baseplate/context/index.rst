baseplate.context
=================

.. automodule:: baseplate.context


Services
--------

.. autoclass:: baseplate.context.thrift.ThriftContextFactory


Data Stores
-----------

.. autoclass:: baseplate.context.cassandra.CassandraContextFactory

.. autoclass:: baseplate.context.redis.RedisContextFactory


DIY: The Factory
----------------

.. note::

   Stuff beyond this point is only useful if you are implementing your own
   context helpers. You can stop reading here if the stuff above already has
   you covered!


.. autoclass:: baseplate.context.ContextFactory
   :members:
