``baseplate.clients.hvac``
==========================

`Vault`_ is a high-security store for secret tokens, credentials, and other
sensitive information. `HVAC`_ is a Python client library for Vault.

.. note:: The :py:class:`~baseplate.lib.secrets.SecretsStore` handles the most
    common use case of Vault in a Baseplate application: secure retrieval of
    secret tokens. This client is only necessary when taking advantage of more
    advanced features of Vault such as the `Transit backend`_ or `Cubbyholes`_.
    If these don't sound familiar, check out the secrets store before digging
    in here.

.. _`Vault`: https://www.vaultproject.io/
.. _`HVAC`: https://github.com/hvac/hvac/
.. _Transit backend: https://www.vaultproject.io/docs/secrets/transit/
.. _Cubbyholes: https://www.vaultproject.io/docs/secrets/cubbyhole/index.html

.. automodule:: baseplate.clients.hvac

Example
-------

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

See `HVAC's README`_ for documentation on the methods available from its
client.

.. _HVAC's README: https://github.com/hvac/hvac/blob/master/README.md

Configuration
-------------

.. autoclass:: HvacClient

.. autofunction:: hvac_factory_from_config

Classes
-------

.. autoclass:: HvacContextFactory
   :members:
