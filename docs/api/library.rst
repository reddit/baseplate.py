The Library
===========

Baseplate also provides a collection of "extra batteries". These independent
modules provide commonly needed functionality to applications. They can be used
separately from the rest of Baseplate.

.. toctree::
   :titlesonly:

   baseplate.lib.config: Configuration parsing <baseplate/lib/config>
   baseplate.lib.crypto: Cryptographic Primitives <baseplate/lib/crypto>
   baseplate.lib.datetime: Extensions to the standard library's datetime module <baseplate/lib/datetime>
   baseplate.lib.edge_context: Information about the original request from the client <baseplate/lib/edge_context>
   baseplate.lib.events: Events for the data pipeline <baseplate/lib/events>
   baseplate.lib.experiments: Experiments framework <baseplate/lib/experiments>
   baseplate.lib.file_watcher: Read files from disk as they change <baseplate/lib/file_watcher>
   baseplate.lib.live_data: Tools for centralized data that updates near instantly <baseplate/lib/live_data>
   baseplate.lib.message_queue: POSIX IPC Message Queues <baseplate/lib/message_queue>
   baseplate.lib.metrics: Counters, timers, gauges, and histograms for statsd <baseplate/lib/metrics>
   baseplate.lib.random: Extensions to the standard library's random module <baseplate/lib/random>
   baseplate.lib.ratelimit: Ratelimit counters in memcached or redis <baseplate/lib/ratelimit>
   baseplate.lib.retry: Policies for retrying operations <baseplate/lib/retry>
   baseplate.lib.secrets: Secure storage and access to secret tokens and credentials <baseplate/lib/secrets>
   baseplate.lib.thrift_pool: A Thrift client connection pool <baseplate/lib/thrift_pool>
   baseplate.lib.service_discovery: Integration with Synapse service discovery <baseplate/lib/service_discovery>
