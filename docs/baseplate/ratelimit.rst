``baseplate.ratelimit``
=======================

.. automodule:: baseplate.ratelimit

Assuming your project uses MemcacheContextFactory or RedisContextFactor to
attach a cache client to the context object (or the request object for HTTP
projects), you can increment a rate limit counter like::

    from baseplate import ratelimit

    class Handler:
        def do_something(self, context):
            count = ratelimit.incr_and_get(context.cache, 'key')
            if count >= 20:
                raise ValueError('rate limit exceeded, slow down!')

Incrementing a rate limit counter
---------------------------------

.. autofunction:: incr_and_get
.. autofunction:: get_current_interval
