Baseplate
=========

Baseplate is a framework to build services on and a library of common and
well-tested code to share. Its goal is to provide all the common things a
service needs with as few surprises as possible.

Introduction
------------

.. toctree::
   :titlesonly:

   quickstart

The CLI Toolkit
---------------

These are helper tools for running and supporting applications in production
and development.

.. toctree::
   :titlesonly:

   baseplate-healthcheck: Is your service alive? <cli/healthcheck>
   baseplate-serve: The application server <cli/serve>
   baseplate-script: Run backend scripts <cli/script>

The Framework
-------------

Baseplate provides an opinionated and integrated framework for making server
applications.  This framework provides monitoring, logging, and tracing out of
the box.

.. toctree::
   :titlesonly:

   baseplate.core: The guts of the diagnostics framework <baseplate/core>
   baseplate.context: Integration with client libraries <baseplate/context/index>
   baseplate.integration: Integration with application frameworks <baseplate/integration/index>
   baseplate.diagnostics: Diagnostics observers <baseplate/diagnostics/index>

The Library
-----------

These modules are relatively disconnected and provide various pieces of
functionality that are commonly needed in production applications. They can
be used without the framework.

.. toctree::
   :titlesonly:

   baseplate: General purpose helpers <baseplate/index>
   baseplate.config: Configuration parsing <baseplate/config>
   baseplate.crypto: Cryptographic Primitives <baseplate/crypto>
   baseplate.events: Events for the data pipeline <baseplate/events>
   baseplate.message_queue: POSIX IPC Message Queues <baseplate/message_queue>
   baseplate.metrics: Counters and timers for statsd <baseplate/metrics>
   baseplate.random: Extensions to the standard library's random module <baseplate/random>
   baseplate.retry: Policies for retrying operations <baseplate/retry>
   baseplate.thrift_pool: A Thrift client connection pool <baseplate/thrift_pool>
   baseplate.service_discovery: Integration with Synapse service discovery <baseplate/service_discovery>

Other
-----

* :ref:`genindex`
* :ref:`modindex`
* :doc:`glossary`

.. toctree::
   :titlesonly:
   :hidden:

   glossary
