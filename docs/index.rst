Baseplate
=========

Baseplate is a framework to build services on and a library of common and
well-tested code to share. Its goal is to provide all the common things a
service needs with as few surprises as possible. It can be divided up into
three distinct categories of components, listed below.

The Instrumentation Framework
-----------------------------

Baseplate provides an instrumentation framework that integrates with popular
application frameworks to provide automatic diagnostics to services.

.. toctree::
   :titlesonly:

   baseplate.core: The skeleton of the instrumentation framework <baseplate/core>
   baseplate.context: Integration with client libraries <baseplate/context/index>
   baseplate.integration: Integration with application frameworks <baseplate/integration/index>
   baseplate.diagnostics: Diagnostics observers <baseplate/diagnostics/index>

The Library
-----------

Baseplate also provides a collection of "extra batteries". These independent
modules provide commonly needed functionality to applications. They can be used
separately from the rest of Baseplate.

.. toctree::
   :titlesonly:

   baseplate: General purpose helpers <baseplate/index>
   baseplate.config: Configuration parsing <baseplate/config>
   baseplate.crypto: Cryptographic Primitives <baseplate/crypto>
   baseplate.events: Events for the data pipeline <baseplate/events>
   baseplate.experiments: Experiments framework <baseplate/experiments/index>
   baseplate.file_watcher: Read files from disk as they change <baseplate/file_watcher>
   baseplate.live_data: Tools for centralized data that updates near instantly <baseplate/live_data>
   baseplate.message_queue: POSIX IPC Message Queues <baseplate/message_queue>
   baseplate.metrics: Counters, timers, gauges, and histograms for statsd <baseplate/metrics>
   baseplate.queue_consumer: Consume messages from a queue <baseplate/queue_consumer>
   baseplate.random: Extensions to the standard library's random module <baseplate/random>
   baseplate.ratelimit: Ratelimit counters in memcached or redis <baseplate/ratelimit>
   baseplate.retry: Policies for retrying operations <baseplate/retry>
   baseplate.secrets: Secure storage and access to secret tokens and credentials <baseplate/secrets>
   baseplate.thing_id: Thing ID prefixes and validator <baseplate/thing_id>
   baseplate.thrift_pool: A Thrift client connection pool <baseplate/thrift_pool>
   baseplate.service_discovery: Integration with Synapse service discovery <baseplate/service_discovery>

The CLI Toolkit
---------------

Baseplate provides command line tools which are useful for running applications
in production and development.

.. toctree::
   :titlesonly:

   baseplate-healthcheck: Is your service alive? <cli/healthcheck>
   baseplate-serve: The application server <cli/serve>
   baseplate-script: Run backend scripts <cli/script>
   baseplate-tshell: Begin an interactive shell for a Thrift service <cli/tshell>

HTTP services can use Pyramid's pshell_ in order to get an interactive shell.

.. _pshell: https://docs.pylonsproject.org/projects/pyramid/en/latest/pscripts/pshell.html

Appendix
--------

* :ref:`genindex`
* :ref:`modindex`
* :doc:`glossary`

.. toctree::
   :hidden:

   glossary
