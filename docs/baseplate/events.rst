baseplate.events
================

.. automodule:: baseplate.events

Building Events
---------------

.. autoclass:: FieldKind
   :members:

.. autoclass:: Event
   :members:


Queing Events
-------------

.. autoclass:: EventQueue
   :members:


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

    python -m baseplate.events.publisher --queue-name something config_file.ini

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
