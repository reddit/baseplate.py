from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from .backends.base import RateLimitBackend
from ..context import ContextFactory


class RateLimitExceededException(Exception):
    """This exception gets raised whenever a rate limit is exceeded.
    """
    pass


class RateLimiterContextFactory(ContextFactory):
    """RateLimiter context factory

    :param backend_factory: An instance of
        :py:class:`baseplate.context.ContextFactory`. The context factory must
        return an instance of :py:class:`baseplate.ratelimit.backends.RateLimitBackend`
    :param int allowance: The maximum allowance allowed per key.
    :param int interval: The interval (in seconds) to reset allowances.
    :param str key_prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, backend_factory, allowance, interval, key_prefix=''):
        if allowance < 1:
            raise ValueError('minimum allowance is 1')
        if interval < 1:
            raise ValueError('minimum interval is 1')
        if not isinstance(backend_factory, ContextFactory):
            raise TypeError('backend_factory must be an instance of ContextFactory')

        self.backend_factory = backend_factory
        self.allowance = allowance
        self.interval = interval
        self.key_prefix = key_prefix

    def make_object_for_context(self, name, server_span):
        backend = self.backend_factory.make_object_for_context(name, server_span)
        return RateLimiter(backend, self.allowance, self.interval,
                           key_prefix=self.key_prefix)


class RateLimiter(object):
    """A class for rate limiting actions.

    :param `RateLimitBackend` backend: The backend to use for storing rate
        limit counters.
    :param int allowance: The maximum allowance allowed per key.
    :param int interval: The interval (in seconds) to reset allowances.
    :param str key_prefix: A prefix to add to keys during rate limiting.
        This is useful if you will have two different rate limiters that will
        receive the same keys.

    """

    def __init__(self, backend, allowance, interval, key_prefix='rl:'):
        if allowance < 1:
            raise ValueError('minimum allowance is 1')
        if interval < 1:
            raise ValueError('minimum interval is 1')
        if not isinstance(backend, RateLimitBackend):
            raise TypeError('backend must be an instance of RateLimitBackend')

        self.backend = backend
        self.key_prefix = key_prefix
        self.allowance = allowance
        self.interval = interval

    def consume(self, key, amount=1):
        """Consume the given `amount` from the allowance for the given `key`.

        This will rate
        :py:class:`baseplate.ratelimit.RateLimitExceededException` if the
        allowance for `key` is exhausted.

        :param str key: The name of the rate limit bucket to consume from.
        :param int amount: The amount to consume from the rate limit bucket.

        """
        key = self.key_prefix + key
        if not self.backend.consume(key, amount, self.allowance, self.interval):
            raise RateLimitExceededException('Rate limit exceeded.')
