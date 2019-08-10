``baseplate.lib.edge_context``
------------------------------

.. automodule:: baseplate.lib.edge_context

The :py:class:`EdgeRequestContext` provides an interface into both
authentication and context information about the original request from a user.
For edge services, it provides helpers to create the initial object and
serialize the context information into the appropriate headers.  Once this
object is created and attached to the context, Baseplate will automatically
forward the headers to downstream services so they can access the
authentication and context data as well.

.. autoclass:: EdgeRequestContextFactory
   :members:

.. autoclass:: EdgeRequestContext
   :members:

.. autoclass:: User
   :members:

.. autoclass:: OAuthClient
   :members:

.. autoclass:: Session
   :members:

.. autoclass:: Service
   :members:

.. autoclass:: AuthenticationToken
   :members:

.. autoexception:: NoAuthenticationError

