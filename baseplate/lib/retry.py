"""Policies for retrying an operation safely."""
import time

from typing import Iterator
from typing import Optional


class RetryPolicy:
    """A policy for retrying operations.

    Policies are meant to be used as an iterable::

        for time_remaining in RetryPolicy.new(attempts=3):
            try:
                some_operation.do(timeout=time_remaining)
                break
            except SomeError:
                pass
        else:
            raise MaxRetriesError

    """

    def yield_attempts(self) -> Iterator[Optional[float]]:
        """Return an iterator which controls attempts.

        On each iteration, the iterator will yield the number of seconds left
        to retry, this should be used to set the timeout on the operation
        being carried out. If there is no maximum time remaining,
        :py:data:`None` is yielded instead.

        The iterable will raise :py:exc:`StopIteration` once the operation
        should not be retried any further.

        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[Optional[float]]:
        """Return the result of :py:meth:`yield_attempts`.

        This allows policies to be directly iterated over.

        """
        return self.yield_attempts()

    @staticmethod
    def new(
        attempts: Optional[int] = None,
        budget: Optional[float] = None,
        backoff: Optional[float] = None,
    ) -> "RetryPolicy":
        """Create a new retry policy with the given constraints.

        :param attempts: The maximum number of times the operation can be
            attempted.
        :param budget: The maximum amount of time, in seconds, that the
            local service will wait for the operation to succeed.
        :param backoff: The base amount of time, in seconds, for
            exponential back-off between attempts. ``N`` in (``N *
            2**attempts``).

        """
        policy: RetryPolicy = IndefiniteRetryPolicy()

        if attempts is not None:
            policy = MaximumAttemptsRetryPolicy(policy, attempts)

        if budget is not None:
            policy = TimeBudgetRetryPolicy(policy, budget)

        if backoff is not None:
            policy = ExponentialBackoffRetryPolicy(policy, backoff)

        return policy


class IndefiniteRetryPolicy(RetryPolicy):  # pragma: noqa
    """Retry immediately forever."""

    def yield_attempts(self) -> Iterator[Optional[float]]:
        while True:
            yield None


class MaximumAttemptsRetryPolicy(RetryPolicy):
    """Constrain the total number of attempts."""

    def __init__(self, policy: RetryPolicy, attempts: int):
        self.subpolicy = policy
        self.attempts = attempts

    def yield_attempts(self) -> Iterator[Optional[float]]:
        for attempt_number, time_remaining in enumerate(self.subpolicy):
            if attempt_number == self.attempts:
                break
            yield time_remaining


class TimeBudgetRetryPolicy(RetryPolicy):
    """Constrain attempts to an overall time budget."""

    def __init__(self, policy: RetryPolicy, budget: float):
        assert budget >= 0, "The time budget must not be negative."
        self.subpolicy = policy
        self.budget = budget

    def yield_attempts(self) -> Iterator[Optional[float]]:
        start_time = time.time()

        yield self.budget

        for _ in self.subpolicy:
            elapsed = time.time() - start_time
            time_remaining = self.budget - elapsed
            if time_remaining <= 0:
                break
            yield time_remaining


class ExponentialBackoffRetryPolicy(RetryPolicy):
    """Sleep exponentially longer between attempts."""

    def __init__(self, policy: RetryPolicy, base: float):
        self.subpolicy = policy
        self.base = base

    def yield_attempts(self) -> Iterator[Optional[float]]:
        for attempt, time_remaining in enumerate(self.subpolicy):
            if attempt > 0:
                delay = self.base * 2.0 ** (attempt - 1.0)
                if time_remaining:
                    delay = min(delay, time_remaining)
                    time_remaining -= delay

                time.sleep(delay)

            yield time_remaining
