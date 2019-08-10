``baseplate.clients.redis``
===========================

.. automodule:: baseplate.clients.redis

To integrate `redis-py`_ with your application, add the appropriate client
declaration to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": RedisClient(),
         ...
      }
   )

.. _`redis-py`: https://github.com/andymccurdy/redis-py

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


and then use it in request::

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
