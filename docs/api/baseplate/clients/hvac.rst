``baseplate.clients.hvac``
==========================

.. automodule:: baseplate.clients.hvac

To integrate HVAC with your application, add the appropriate client declaration
to your context configuration::

   baseplate.configure_context(
      app_config,
      {
         ...
         "foo": HvacClient(),
         ...
      }
   )

configure it in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # optional: how long to wait for calls to vault
   foo.timeout = 300 milliseconds

   ...

and then use it in request::

   def my_method(request):
       request.foo.is_initialized()

Configuration
-------------

.. autoclass:: HvacClient

.. autofunction:: hvac_factory_from_config

Classes
-------

.. autoclass:: HvacContextFactory
   :members:
