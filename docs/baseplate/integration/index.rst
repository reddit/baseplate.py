``baseplate.integration``
=========================

.. automodule:: baseplate.integration

See one of the submodules below for your framework of choice.


Thrift
------

.. automodule:: baseplate.integration.thrift

.. autoclass:: baseplate.integration.thrift.BaseplateProcessorEventHandler


Pyramid
-------

.. automodule:: baseplate.integration.pyramid

.. autoclass:: baseplate.integration.pyramid.BaseplateConfigurator

Events
~~~~~~

Within its Pyramid integration, Baseplate will emit events at various stages
of the request lifecycle that services can hook into.

.. autoclass:: baseplate.integration.pyramid.ServerSpanInitialized
