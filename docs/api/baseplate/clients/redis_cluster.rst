``baseplate.clients.redis_cluster``
===================================

`Redis`_ is an in-memory data structure store used where speed is necessary but
complexity is beyond simple key-value operations. (If you're just doing
caching, prefer :doc:`memcached <memcache>`). `Redis-py-cluster`_ is a Python
client library that supports interacting with Redis when operating in cluster mode.

.. _`Redis`: https://redis.io/
.. _`redis-py-cluster`: https://github.com/Grokzen/redis-py

.. automodule:: baseplate.clients.redis_cluster

Example
-------

To integrate redis-py-cluster with your application, add the appropriate client
declaration to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": ClusterRedisClient(),
         ...
      }
   )

configure it in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...


   # required: what redis instance to connect to
   foo.url = redis://localhost:6379/0

   # optional: the maximum size of the connection pool
   foo.max_connections = 99

   # optional: how long to wait for a connection to establish
   foo.timeout = 3 seconds

   # optional: Whether read requests should be directed to replicas as well
   #  instead of just the primary
   foo.read_from_replicas = true
   ...


and then use the attached :py:class:`~redis.Redis`-like object in
request::

   def my_method(request):
       request.foo.ping()

Configuration
-------------

.. autoclass:: ClusterRedisClient

.. autofunction:: cluster_pool_from_config

Classes
-------

.. autoclass:: ClusterRedisContextFactory
   :members:

.. autoclass:: MonitoredClusterRedisConnection
   :members:

Runtime Metrics
---------------

In addition to request-level metrics reported through spans, this wrapper
reports connection pool statistics periodically via the :ref:`runtime-metrics`
system.  All metrics are tagged with ``client``, the name given to
:py:meth:`~baseplate.Baseplate.configure_context` when registering this context
factory.

The following metrics are reported:

``runtime.pool.size``
   The size limit for the connection pool.
``runtime.pool.in_use``
   How many connections have been established and are currently checked out and
   being used.

.. versionadded:: 2.1
