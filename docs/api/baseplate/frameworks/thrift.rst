``baseplate.frameworks.thrift``
===============================

`Thrift`_ is a cross-language framework for cross-service communication.
Developers write a language-independent definition of a service's API (the
"IDL") and Thrift's code generator makes server and client libraries for that
API.

This module provides a wrapper for a :py:class:`TProcessor` which integrates
Baseplate's facilities into the Thrift request lifecycle.

An abbreviated example of it in use::

    logger = logging.getLogger(__name__)


    def make_processor(app_config):
        baseplate = Baseplate(app_config)

        handler = MyHandler()
        processor = my_thrift.MyService.Processor(handler)
        return baseplateify_processor(processor, logger, baseplate)

.. _`Thrift`: https://thrift.apache.org/

.. automodule:: baseplate.frameworks.thrift

.. autofunction:: baseplateify_processor
