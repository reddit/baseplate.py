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

Serialization
-------------

This integration also supports adding custom serializers to
:doc:`Kombu <kombu:index>` via the :py:class:`baseplate.clients.kombu.KombuSerializer`
interface and the :py:class:`baseplate.clients.kombu.register_serializer`
function.  This serializer can be passed to the
:py:class:`baseplate.clients.kombu.KombuProducerContextFactory` for use by the
:py:class:`baseplate.clients.kombu.KombuProducer` to allow for automatic
serialization when publishing.

In order to use a custom serializer, you must first register it with Kombu using
the provided :py:class:`baseplate.clients.kombu.register_serializer` function.

In-addition to the base interface, we also provide a serializer for Thrift
objects: :py:class:`baseplate.clients.kombu.KombuThriftSerializer`.

Example
^^^^^^^

.. code-block:: python

   serializer = KombuThriftSerializer[ThriftStruct](ThriftStruct)
   register_serializer(serializer)


Interface
^^^^^^^^^

.. autoclass:: KombuSerializer

.. autofunction:: register_serializer

Serializers
^^^^^^^^^^^

.. autoclass:: KombuThriftSerializer


Configuration
-------------

.. autoclass:: KombuProducer

.. autofunction:: connection_from_config

.. autofunction:: exchange_from_config


Classes
-------

.. autoclass:: KombuProducerContextFactory
