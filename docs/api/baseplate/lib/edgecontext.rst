``baseplate.lib.edgecontext``
------------------------------

.. automodule:: baseplate.lib.edgecontext

Services deep within the backend often need to know information about the
client that originated the request, such as what user is authenticated or what
country they're in. Baseplate services can get this information from the edge
context which is automatically propagated along with calls between services.

.. versionchanged:: 2.0

   The implementation built into Baseplate.py was extracted into its own
   library. See <https://reddit-edgecontext.readthedocs.io/en/latest/> for an
   example implementation.

.. autoclass:: EdgeContextFactory
   :members:
