``baseplate.lib.ratelimit``
===========================

.. automodule:: baseplate.lib.ratelimit

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

.. autoclass:: RateLimiter
  :members:

.. autoclass:: RateLimiterContextFactory
  :members:

.. autoclass:: RateLimitExceededException
  :members:


Backends
--------

.. autoclass:: baseplate.lib.ratelimit.backends.RateLimitBackend
  :members:


Memcache
^^^^^^^^

.. automodule:: baseplate.lib.ratelimit.backends.memcache

.. autoclass:: MemcacheRateLimitBackendContextFactory
  :members:

.. autoclass:: MemcacheRateLimitBackend
  :members:


Redis
^^^^^

.. automodule:: baseplate.lib.ratelimit.backends.redis

.. autoclass:: RedisRateLimitBackendContextFactory
  :members:

.. autoclass:: RedisRateLimitBackend
  :members:
