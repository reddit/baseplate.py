``baseplate.ratelimit``
=======================

.. automodule:: baseplate.ratelimit

Configuring a rate limiter for your request context requires a context factory for the backend
and a factory for the rate limiter itself::

    redis_pool = pool_from_config(app_config)
    backend_factory = RedisRateLimitBackendContextFactory(redis_pool)
    ratelimiter_factory = RateLimiterContextFactory(backend_factory, allowance, interval)
    baseplate.add_to_context('ratelimiter', ratelimiter_factory)

The rate limiter can then be used during a request with::

    try:
        context.ratelimiter.consume(context.request_context.user.id)
        print('Ratelimit passed')
    except RateLimitExceededException:
        print('Too many requests')


Classes
-------

.. autoclass:: baseplate.ratelimit.RateLimiter
  :members:

.. autoclass:: baseplate.ratelimit.RateLimiterContextFactory
  :members:

.. autoclass:: baseplate.ratelimit.RateLimitExceededException
  :members:

.. autoclass:: baseplate.ratelimit.RateLimitBackend
  :members:

.. autoclass:: baseplate.ratelimit.MemcacheRateLimitBackendContextFactory
  :members:

.. autoclass:: baseplate.ratelimit.MemcacheRateLimitBackend
  :members:

.. autoclass:: baseplate.ratelimit.RedisRateLimitBackendContextFactory
  :members:

.. autoclass:: baseplate.ratelimit.RedisRateLimitBackend
  :members:
