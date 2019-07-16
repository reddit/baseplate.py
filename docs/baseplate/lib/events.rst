``baseplate.lib.events``
========================

.. automodule:: baseplate.lib.events

Building Events
---------------

Thrift Schema v2 Events
~~~~~~~~~~~~~~~~~~~~~~~

For modern Thrift-based events: import the event schemas into your project,
instantiate and fill out an event object, and pass it into the queue::

   import time
   import uuid

   from baseplate.lib.events import EventQueue, serialize_v2_event

   from event_schemas.event.ttypes import Event


   def make_wsgi_app(app_config):
      ...

      queue = EventQueue("v2", event_serializer=serialize_v2_event)
      baseplate.add_to_context("events_v2", queue)

      ...


   def my_handler(request):
      event = Event(
         source="baseplate",
         action="test",
         noun="baseplate",
         client_timestamp=time.time() * 1000,
         uuid=str(uuid.uuid4()),
      )
      request.events_v2.put(ev2)


Legacy schemaless events
~~~~~~~~~~~~~~~~~~~~~~~~

For legacy schemaless events, you can use these helper objects to build
payloads:

.. autoclass:: FieldKind
   :members:

.. autoclass:: Event
   :members:


Queing Events
-------------

.. autoclass:: EventQueue
   :members: put


The ``EventQueue`` also implements
:py:class:`~baseplate.clients.ContextFactory` so it can be used with
:py:meth:`~baseplate.Baseplate.add_to_context`::

   event_queue = EventQueue("production")
   baseplate.add_to_context("events_production", event_queue)

It can then be used from the :term:`context object` during requests::

   def some_service_method(self, context):
       event = Event(...)
       context.events_production.put(event)

Serializers
~~~~~~~~~~~

The ``event_serializer`` parameter to :py:class:`EventQueue` is a callable
which serializes a given event object. The default is the original schemaless
format. This can be overridden by passing in a different serializer.  Baseplate
comes with a serializer for the new Thrift schema based V2 event system as
well:

.. autofunction:: serialize_v1_event

.. autofunction:: serialize_v2_event


Exceptions
~~~~~~~~~~

.. autoexception:: EventError

.. autoexception:: EventTooLargeError

.. autoexception:: EventQueueFullError


Publishing Events
-----------------

Events that are put onto an :py:class:`EventQueue` are consumed by a separate
process and published to the remote event collector service. The publisher is
in baseplate and can be run as follows::

    python -m baseplate.sidecars.event_publisher --queue-name something config_file.ini

The publisher will look at the specified INI file to find its configuration.
Given a queue name of ``something`` (as in the example above), it will expect a
section in the INI file called ``[event-publisher:something]`` with content
like below::

   [event-publisher:something]
   collector.hostname = some-domain.example.com

   key.name = NameOfASecretKey
   key.secret = Base64-encoded-blob-of-randomness

   metrics.namespace = a.name.to.put.metrics.under
   metrics.endpoint = the-statsd-host:1234
