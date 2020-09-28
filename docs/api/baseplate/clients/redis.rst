``baseplate.clients.redis``
===========================

`Redis`_ is an in-memory data structure store used where speed is necessary but
complexity is beyond simple key-value operations. (If you're just doing
caching, prefer :doc:`memcached <memcache>`). `Redis-py`_ is a Python
client library for Redis.

.. _`Redis`: https://redis.io/
.. _`redis-py`: https://github.com/andymccurdy/redis-py

.. automodule:: baseplate.clients.redis

Example
-------

To integrate redis-py with your application, add the appropriate client
declaration to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": RedisClient(),
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
   foo.socket_connect_timeout = 3 seconds

   # optional: how long to wait for a command to execute
   foo.socket_timeout = 200 milliseconds

   ...


and then use the attached :py:class:`~redis.Redis`-like object in
request::

   def my_method(request):
       request.foo.ping()

Configuration
-------------

.. autoclass:: RedisClient

.. autofunction:: pool_from_config

Classes
-------

.. autoclass:: RedisContextFactory
   :members:

.. autoclass:: MonitoredRedisConnection
   :members:

.. autoclass:: MessageQueue
   :members:

Runtime Metrics
---------------

In addition to request-level metrics reported through spans, this wrapper
reports connection pool statistics periodically via the :ref:`runtime-metrics`
system.  All metrics are prefixed as follows:

.. code-block:: none

   {namespace}.runtime.{hostname}.PID{pid}.clients.{name}

where ``namespace`` is the application's namespace, ``hostname`` and ``pid``
come from the operating system, and ``name`` is the name given to
:py:meth:`~baseplate.Baseplate.add_to_context` when registering this
context factory.

The following metrics are reported:

``pool.size``
   The size limit for the connection pool.
``pool.in_use``
   How many connections have been established and are currently checked out and
   being used.

.. versionadded:: 1.5
