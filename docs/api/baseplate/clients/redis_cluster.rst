``baseplate.clients.redis_cluster``
===================================

`Redis`_ is an in-memory data structure store used where speed is necessary but
complexity is beyond simple key-value operations. (If you're just doing
caching, prefer :doc:`memcached <memcache>`). `Redis-py-cluster`_ is a Python
client library that supports interacting with Redis when operating in cluster mode.

.. _`Redis`: https://redis.io/
.. _`redis-py-cluster`: https://github.com/Grokzen/redis-py-cluster

.. automodule:: baseplate.clients.redis_cluster

.. versionadded:: 2.1

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

   # optional: Whether read requests should be directed to replicas
   # as well instead of just the primary
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


Hot Key Tracking
----------------

Optionally, the client can help track key usage across the Redis cluster to
help you identify if you have "hot" keys (keys that are read from or
written to much more frequently than other keys). This is particularly useful
in clusters with ``noeviction`` set as the eviction policy, since Redis
lacks a built-in mechanism to help you track hot keys in this case.

Since tracking every single key used is expensive, the tracker works by
tracking a small percentage or reads and/or writes, which can be configured
on your client:

.. code-block:: ini

   [app:main]

   ...
   # Note that by default the sample rate will be zero for both reads and writes

   # optional: Sample keys for 1% of read operations
   foo.track_key_reads_sample_rate = 0.01

   # optional: Sample keys for 10% of write operations
   foo.track_key_writes_sample_rate = 0.01

   ...

The keys tracked will be written to a sorted set in the Redis cluster itself,
which we can query at any time to see what keys are read from or written to
more often than others. Keys used for writes will be stored in
`baseplate-hot-key-tracker-writes` and keys used for reads will be stored in
`baseplate-hot-key-tracker-reads`. Here's an example of how you can query the
top 10 keys on each categories with their associated scores:

.. code-block:: console

   > ZREVRANGEBYSCORE baseplate-hot-key-tracker-reads +inf 0 WITHSCORES LIMIT 0 10

   > ZREVRANGEBYSCORE baseplate-hot-key-tracker-writes +inf 0 WITHSCORES LIMIT 0 10


Note that due to how the sampling works the scores are only meaningful in a
relative sense (by comparing one key's access frequency to others in the list)
but can't be used to make any inferences about key access rate or anything like
that.

Both tracker sets have a default TTL of 24 hours, so once they stop being
written to (for instance, if key tracking is disabled) they will clean up
after themselves in 24 hours, allowing us to start fresh the next time we
want to enable key tracking.
