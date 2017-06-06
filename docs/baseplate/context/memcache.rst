``baseplate.context.memcache``
==============================

.. automodule:: baseplate.context.memcache

Configuration Parsing
---------------------

.. autofunction:: baseplate.context.memcache.pool_from_config

Classes
-------

.. autoclass:: baseplate.context.memcache.MemcacheContextFactory
   :members:

.. autoclass:: baseplate.context.memcache.MonitoredMemcacheConnection
   :members:

Serialization/deserialization helpers
-------------------------------------

.. autofunction:: baseplate.context.memcache.lib.decompress_and_load

.. autofunction:: baseplate.context.memcache.lib.make_dump_and_compress_fn

.. autofunction:: baseplate.context.memcache.lib.decompress_and_unpickle

.. autofunction:: baseplate.context.memcache.lib.make_pickle_and_compress_fn
