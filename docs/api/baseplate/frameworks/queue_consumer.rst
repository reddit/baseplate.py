``baseplate.frameworks.queue_consumer``
=======================================

.. automodule:: baseplate.frameworks.queue_consumer

Create a long-running process to consume from a queue. For example::

    from kombu import Connection, Exchange
    from baseplate import queue_consumer

    def process_links(context, msg_body, msg):
        print('processing %s' % msg_body)

    queue_consumer.consume(
        baseplate=make_baseplate(cfg, app_config),
        exchange=Exchange('reddit_exchange', 'direct'),
        connection=Connection(
          hostname='amqp://guest:guest@reddit.local:5672',
          virtual_host='/',
        ),
        queue_name='process_links_q',
        routing_keys=[
            'link_created',
            'link_deleted',
            'link_updated',
        ],
        handler=process_links,
    )


This will create a queue named ``'process_links_q'`` and bind the routing keys
``'link_created'``, ``'link_deleted'``, and ``'link_updated'``. It will then
register a consumer for ``'process_links_q'`` to read messages and feed them to
``process_links``.

Register and run a queue consumer
---------------------------------

.. autofunction:: consume

.. autoclass:: KombuConsumer
   :members:

If you require more direct control
----------------------------------

.. autoclass:: BaseKombuConsumer
   :members:
