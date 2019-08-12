``baseplate.clients.kombu``
===========================

This integration adds support for sending messages to queue brokers (like
`RabbitMQ`_) via :doc:`Kombu <kombu:index>`.  If you are looking to consume
messages, check out the :py:mod:`baseplate.frameworks.queue_consumer` framework
integration instead.

.. _`RabbitMQ`: https://www.rabbitmq.com/

.. automodule:: baseplate.clients.kombu

Example
-------

To integrate it with your application, add the appropriate client declaration
to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": KombuProducer(),
         ...
      }
   )

configure it in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # required: where to find the queue broker
   foo.hostname = rabbit.local

   # optional: the rabbitmq virtual host to use
   foo.virtual_host = /

   # required: which type of exchange to use
   foo.exchange_type = topic

   # optional: the name of the exchange to use (default is no name)
   foo.exchange_name = bar

   ...


and then use the attached :py:class:`~kombu.Producer`-like object in request::

   def my_method(request):
       request.foo.publish("boo!", routing_key="route_me")

Configuration
-------------

.. autoclass:: KombuProducer

.. autofunction:: connection_from_config

.. autofunction:: exchange_from_config


Classes
-------

.. autoclass:: KombuProducerContextFactory
