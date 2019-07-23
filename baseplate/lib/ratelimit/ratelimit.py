from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib.ratelimit.backends import RateLimitBackend


class RateLimitExceededException(Exception):
    """This exception gets raised whenever a rate limit is exceeded."""


class RateLimiterContextFactory(ContextFactory):
    """RateLimiter context factory.

    :param backend_factory: An instance of
        :py:class:`baseplate.clients.ContextFactory`. The context factory must
        return an instance of :py:class:`baseplate.lib.ratelimit.backends.RateLimitBackend`
    :param allowance: The maximum allowance allowed per key.
    :param interval: The interval (in seconds) to reset allowances.

    """

    def __init__(self, backend_factory: ContextFactory, allowance: int, interval: int):
        if allowance < 1:
            raise ValueError("minimum allowance is 1")
        if interval < 1:
            raise ValueError("minimum interval is 1")
        if not isinstance(backend_factory, ContextFactory):
            raise TypeError("backend_factory must be an instance of ContextFactory")

        self.backend_factory = backend_factory
        self.allowance = allowance
        self.interval = interval

    def make_object_for_context(self, name: str, span: Span) -> "RateLimiter":
        backend = self.backend_factory.make_object_for_context(name, span)
        return RateLimiter(backend, self.allowance, self.interval)


class RateLimiter:
    """A class for rate limiting actions.

    :param backend: The backend to use for storing rate limit counters.
    :param allowance: The maximum allowance allowed per key.
    :param interval: The interval (in seconds) to reset allowances.

    """

    def __init__(self, backend: RateLimitBackend, allowance: int, interval: int):
        if allowance < 1:
            raise ValueError("minimum allowance is 1")
        if interval < 1:
            raise ValueError("minimum interval is 1")
        if not isinstance(backend, RateLimitBackend):
            raise TypeError("backend must be an instance of RateLimitBackend")

        self.backend = backend
        self.allowance = allowance
        self.interval = interval

    def consume(self, key: str, amount: int = 1) -> None:
        """Consume the given `amount` from the allowance for the given `key`.

        This will raise
        :py:class:`baseplate.lib.ratelimit.RateLimitExceededException` if the
        allowance for `key` is exhausted.

        :param key: The name of the rate limit bucket to consume from.
        :param amount: The amount to consume from the rate limit bucket.

        """
        if not self.backend.consume(key, amount, self.allowance, self.interval):
            raise RateLimitExceededException("Rate limit exceeded.")
