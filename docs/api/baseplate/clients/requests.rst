``baseplate.clients.requests``
==============================

:doc:`Requests <requests:user/quickstart>` is a library for making HTTP
requests. Baseplate provides two wrappers for Requests: the "external" client
is suitable for communication with third party, potentially untrusted,
services; the "internal" client is suitable for talking to first-party services
and automatically includes trace and edge context data in requests. Baseplate
uses `Advocate`_ to prevent the external client from talking to internal
services and vice versa.

.. _`Advocate`: https://pypi.org/project/advocate/

.. automodule:: baseplate.clients.requests

.. versionadded:: 1.4

Example
-------

To integrate ``requests`` with your application, add the appropriate client
declaration to your context configuration::

   baseplate.configure_context(
      {
         ...
         # see above for when to use which of these
         "foo": ExternalRequestsClient(),
         "bar": InternalRequestsClient(),
         ...
      }
   )

configure it in your application's configuration file:

.. code-block:: ini

   [app:main]

   ...

   # optional: the number of connections to cache
   foo.pool_connections = 10

   # optional: the maximum number of connections to keep in the pool
   foo.pool_maxsize = 10

   # optional: how many times to retry DNS/connection attempts
   # (not data requests)
   foo.max_retries = 0

   # optional: whether or not to block waiting for connections
   # from the pool
   foo.pool_block = false

   # optional: address filter configuration, see
   # http_adapter_from_config for all options
   foo.filter.ip_allowlist = 1.2.3.0/24

   ...


and then use the attached :py:class:`~requests.Session`-like object in
request::

   def my_method(request):
       request.foo.get("http://html5zombo.com")

Configuration
-------------

.. autoclass:: ExternalRequestsClient

.. autoclass:: InternalRequestsClient

.. autofunction:: http_adapter_from_config

Classes
-------

.. autoclass:: BaseplateSession
   :members:

.. autoclass:: RequestsContextFactory
   :members:
