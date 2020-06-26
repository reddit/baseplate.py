``baseplate.frameworks.pyramid``
================================

`Pyramid`_ is a mature web framework for Python that we build HTTP services
with.

.. _`Pyramid`: https://trypyramid.com/

This module provides a configuration extension for Pyramid which integrates
Baseplate's facilities into the Pyramid WSGI request lifecycle.

An abbreviated example of it in use::

    def make_app(app_config):
        configurator = Configurator()

        baseplate = Baseplate(app_config)
        baseplate_config = BaseplateConfigurator(
            baseplate,
            trust_trace_headers=True,
        )
        configurator.include(baseplate_config.includeme)

        return configurator.make_wsgi_app()

.. warning::

    Because of how Baseplate instruments Pyramid, you should not make an
    :ref:`exception view <exception_views>` prevent Baseplate from seeing the
    unhandled error and reporting it appropriately.

.. automodule:: baseplate.frameworks.pyramid

.. autoclass:: BaseplateConfigurator

.. autoclass:: HeaderTrustHandler
    :members:

.. autoclass:: StaticTrustHandler
    :members:

Events
------

Within its Pyramid integration, Baseplate will emit events at various stages
of the request lifecycle that services can hook into.


.. autoclass:: ServerSpanInitialized

Health Checker Helper
---------------------

This module also provides a helper function to extract the health check probe
used by the builtin healthchecker out of the request.


.. autofunction:: get_is_healthy_probe
