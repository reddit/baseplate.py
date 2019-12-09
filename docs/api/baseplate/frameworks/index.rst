``baseplate.frameworks``
========================

.. automodule:: baseplate.frameworks

Baseplate.py provides integrations with common Python application frameworks.
These integrations automatically manage the :py:class:`~baseplate.ServerSpan`
lifecycle for each unit of work the framework processes (requests or messages).

.. toctree::
   :maxdepth: 1
   :titlesonly:

   baseplate.frameworks.thrift: Thrift RPC <thrift>
   baseplate.frameworks.pyramid: Pyramid Web Framework <pyramid>
   baseplate.frameworks.queue_consumer: Kombu Queue Consumer <queue_consumer/index>
