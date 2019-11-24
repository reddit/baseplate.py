``baseplate.frameworks.queue_consumer.kombu``
=============================================

:doc:`Kombu <kombu:index>` is a library for interacting with queue brokers.

This module provides a :py:class:`~baseplate.server.queue_consumer.QueueConsumerFactory`
that allows you to run a :py:class:`~baseplate.server.queue_consumer.QueueConsumerServer`
that integrates Baseplate's facilities with Kombu.

An abbreviated example of it in use::

    import kombu
    from baseplate import RequestContext
    from typing import Any

    def process_links(
        context: RequestContext,
        body: Any,
        message: kombu.Message,
    ):
        print(f"processing {body}")

    def make_consumer_factory(app_config):
        baseplate = Baseplate(app_config)
        exchange = Exchange("reddit_exchange", "direct")
        connection = Connection(
          hostname="amqp://guest:guest@reddit.local:5672",
          virtual_host="/",
        )
        queue_name = "process_links_q"
        routing_keys = ["link_created"]
        return KombuQueueConsumerFactory.new(
            baseplate=baseplate,
            exchange=exchange,
            connection=connection,
            queue_name=queue_name,
            routing_keys=routing_keys,
            handler_fn=process_links,
        )


This will create a queue named ``'process_links_q'`` and bind the routing key
``'link_created'``. It will then register a consumer for ``'process_links_q'``
to read messages and feed them to ``process_links``.

.. automodule:: baseplate.frameworks.queue_consumer.kombu


Factory
-------

.. autoclass:: KombuQueueConsumerFactory

   .. automethod:: new
   .. automethod:: __init__


Errors
------

.. autoclass:: FatalMessageHandlerError
