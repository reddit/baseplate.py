``baseplate.frameworks.queue_consumer.kafka``
=============================================

This module provides a :py:class:`~baseplate.server.queue_consumer.QueueConsumerFactory`
that allows you to run a :py:class:`~baseplate.server.queue_consumer.QueueConsumerServer`
that integrates Baseplate's facilities with Kafka.

An abbreviated example of it in use::

    import confluent_kafka
    from baseplate import RequestContext
    from typing import Any
    from typing import Dict

    def process_links(
        context: RequestContext,
        data: Dict[str, Any],
        message: confluent_kafka.Message,
    ):
        print(f"processing {data}")

    def make_consumer_factory(app_config):
        baseplate = Baseplate(app_config)
        return InOrderConsumerFactory.new(
            name="kafka_consumer.link_consumer_v0",
            baseplate=baseplate,
            bootstrap_servers="127.0.0.1:9092",
            group_id="service.link_consumer",
            topics=["new_links", "edited_links"],
            handler_fn=process_links,
        )


This will create a Kafka consumer group named ``'service.link_consumer'`` that
consumes from the topics ``'new_links'`` and ``'edited_links'``. Messages read
from those topics will be fed to ``process_links``.

.. automodule:: baseplate.frameworks.queue_consumer.kafka


Factory
-------

.. autoclass:: InOrderConsumerFactory

   .. automethod:: new
   .. automethod:: __init__


.. autoclass:: FastConsumerFactory

   .. automethod:: new
   .. automethod:: __init__
