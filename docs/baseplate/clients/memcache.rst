``baseplate.clients.memcache``
==============================

.. automodule:: baseplate.clients.memcache

Configuration
-------------

.. autoclass:: MemcacheClient

.. autofunction:: pool_from_config

Classes
-------

.. autoclass:: MemcacheContextFactory
   :members:

.. autoclass:: MonitoredMemcacheConnection
   :members:

Serialization/deserialization helpers
-------------------------------------

.. automodule:: baseplate.clients.memcache.lib

.. autofunction:: decompress_and_load

.. autofunction:: make_dump_and_compress_fn

.. autofunction:: decompress_and_unpickle

.. autofunction:: make_pickle_and_compress_fn
