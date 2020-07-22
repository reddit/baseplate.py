``baseplate.clients``
=====================

.. automodule:: baseplate.clients

Instrumented Client Libraries
-----------------------------

.. toctree::
   :titlesonly:

   baseplate.clients.cassandra: Cassandra CQL Client <cassandra>
   baseplate.clients.hvac: Client for using Vault's advanced features <hvac>
   baseplate.clients.kombu: Client for publishing to queues <kombu>
   baseplate.clients.memcache: Memcached Client <memcache>
   baseplate.clients.redis: Redis Client <redis>
   baseplate.clients.requests: Requests (HTTP) Client <requests>
   baseplate.clients.sqlalchemy: SQL Client for relational databases (e.g. PostgreSQL) <sqlalchemy>
   baseplate.clients.thrift: Thrift client for RPC to other backend services <thrift>

DIY: The Factory
----------------

If a library you want is not supported here, it can be added to your own
application by implementing :py:class:`ContextFactory`.

.. autoclass:: ContextFactory
   :members:

To integrate with :py:meth:`~baseplate.Baseplate.configure_context` for maximum
convenience, make a parser that implements
:py:class:`baseplate.lib.config.Parser` and returns your
:py:class:`ContextFactory`.

.. code-block:: python

   class MyClient(config.Parser):
      def parse(
          self, key_path: str, raw_config: config.RawConfig
      ) -> "MyContextFactory":
         parser = config.SpecParser(
            {
               "foo": config.Integer(),
               "bar": config.Boolean(),
            }
         )
         result = parser.parse(key_path, raw_config)
         return MyContextFactory(foo=result.foo, bar=result.bar)
