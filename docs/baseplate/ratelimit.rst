``baseplate.ratelimit``
=======================

.. automodule:: baseplate.ratelimit

Assuming your project uses RedisContextFactory to attach a cache client to the
context object (or the request object for HTTP projects), you can create a
rate limiter with::

    from baseplate import ratelimit

    cache = ratelimit.RedisRateLimitCache(context.cache)
    ratelimiter = ratelimit.RateLimiter(cache, allowance=1000, interval=60)

    try:
        ratelimiter.consume(context.request_context.user.id)
        print('Ratelimit passed')
    except ratelimit.RateLimitExceededException:
        print('Too many requests')


Classes
-------

.. autoclass:: baseplate.ratelimit.RateLimiter
   :members:

.. autoclass:: baseplate.ratelimit.RedisRateLimitCache
  :members:
