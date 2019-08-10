``baseplate.clients.memcache``
==============================

.. automodule:: baseplate.clients.memcache

To integrate pymemcache with your application, add the appropriate client
declaration to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": MemcacheClient(),
         ...
      }
   )

configure it in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # required: the host and port to connect to
   foo.endpoint = localhost:11211

   # optional: the maximum size of the connection pool (default 2147483648)
   foo.max_pool_size = 99

   # optional: how long to wait (in seconds) for connections to establish
   foo.connect_timeout = .5

   # optional: how long to wait (in seconds) for a memcached command
   foo.timeout = .1

   ...


and then use it in request::

   def my_method(request):
       request.foo.incr("bar")

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
