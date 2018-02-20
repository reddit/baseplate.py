``baseplate.lock``
=================================

.. automodule:: baseplate.lock

Assuming your project uses MemcacheContextFactory to attach a pymemcache client
to the context object (or the request object for HTTP projects), you can
acquire a lock like::

    from baseplate.lock import make_lock

    class Handler:
        def do_something(self, context):
            with make_lock(context.cache, 'do_something') as lock:
                # we got the lock

Acquiring A Lock
----------------

.. autofunction:: make_lock
.. autoclass:: baseplate.lock.CacheLock
   :members:

Exceptions
----------

.. autoexception:: TimeoutExpired
