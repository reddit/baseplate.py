``baseplate.frameworks.queue_consumer.deprecated``
==================================================

.. deprecated:: 1.1
    This way of creating a Baseplate queue consumer is deprecated in favor of
    using a `QueueConsumerServer` and will be removed in the next major release.
    Instructions for ugrading are included below.

Upgrading to a QueueConsumerServer
----------------------------------

To start, you will be running your queue consumer as a "server" now, so it will
use `baseplate-serve` rather than `baseplate-script` as the entrypoint.

.. code-block:: diff

    - baseplate-script run.ini consumer:run
    + baseplate-serve run.ini --bind 0.0.0.0:8080

This also means that you will need to update your config file with similar
sections to what you would have for an HTTP or Thrift service.

.. code-block:: diff

    [DEFAULT]
    rabbitmq.hostname = amqp://rabbit.local:5672
    rabbitmq.exchange_name = my-exchange
    rabbitmq.exchange_type = direct

    + [app:main]
    + factory = my_service:make_consumer_factory
    +
    + [server:main]
    + factory = baseplate.server.queue_consumer
    + max_concurrency = 1

You will also need to change your code to create a
:py:class:`~baseplate.frameworks.queue_consumer.komub.KombuQueueConsumerFactory`
with a `make_consumer_factory` function rather than using `queue_consumer.consume`
as you did for this.

.. code-block:: diff

    from kombu import Connection, Exchange
    -
    - from baseplate import queue_consumer
    -
    - def process_links(context, msg_body, msg):
    -     print('processing %s' % msg_body)
    -
    - def run():
    -     queue_consumer.consume(
    -         baseplate=make_baseplate(cfg, app_config),
    -         exchange=Exchange('reddit_exchange', 'direct'),
    -         connection=Connection(
    -         hostname='amqp://guest:guest@reddit.local:5672',
    -         virtual_host='/',
    -         ),
    -         queue_name='process_links_q',
    -         routing_keys=[
    -             'link_created',
    -             'link_deleted',
    -             'link_updated',
    -         ],
    -         handler=process_links,
    -     )
    +
    + from baseplate import Baseplate
    + from baseplate.frameworks.queue_consumer.kombu import (
    +     KombuQueueConsumerFactory,
    + )
    +
    + def process_links(context, message):
    +     body = message.decode()
    +     print('processing %s' % body)
    +
    + def make_consumer_factory(app_config):
    +     baseplate = Baseplate(app_config)
    +     exchange = Exchange('reddit_exchange', 'direct')
    +     connection = Connection(
    +       hostname='amqp://guest:guest@reddit.local:5672',
    +       virtual_host='/',
    +     )
    +     queue_name = 'process_links_q'
    +     routing_keys = ['link_created', 'link_deleted', 'link_updated']
    +     return KombuQueueConsumerFactory.new(
    +         baseplate=baseplate,
    +         exchange=exchange,
    +         connection=connection,
    +         queue_name=queue_name,
    +         routing_keys=routing_keys,
    +         handler_fn=process_links,
    +     )


Original docs
-------------

.. automodule:: baseplate.frameworks.queue_consumer.deprecated

To create a long-running process to consume from a queue::

    from kombu import Connection, Exchange
    from baseplate import queue_consumer

    def process_links(context, msg_body, msg):
        print('processing %s' % msg_body)

    def run():
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
