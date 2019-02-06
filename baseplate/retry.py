"""Policies for retrying an operation safely."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import time


logger = logging.getLogger(__name__)


class RetryPolicy(object):
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

    def yield_attempts(self):
        """Return an iterator which controls attempts.

        On each iteration, the iterator will yield the number of seconds left
        to retry, this should be used to set the timeout on the operation
        being carried out. If there is no maximum time remaining,
        :py:data:`None` is yielded instead.

        The iterable will raise :py:exc:`StopIteration` once the operation
        should not be retried any further.

        """

        raise NotImplementedError

    def __iter__(self):
        """Convenience alias for :py:meth:`yield_attempts`.

        This allows policies to be directly iterated over.

        """
        return self.yield_attempts()

    @staticmethod
    def new(attempts=None, budget=None, backoff=None):
        """Create a new retry policy with the given constraints.

        :param int attempts: The maximum number of times the operation can be
            attempted.
        :param float budget: The maximum amount of time, in seconds, that the
            local service will wait for the operation to succeed.
        :param float backoff: The base amount of time, in seconds, for
            exponential backoff between attempts. ``N`` in (``N *
            2**attempts``).

        """
        policy = IndefiniteRetryPolicy()

        if attempts is not None:
            policy = MaximumAttemptsRetryPolicy(policy, attempts)

        if budget is not None:
            policy = TimeBudgetRetryPolicy(policy, budget)

        if backoff is not None:
            policy = ExponentialBackoffRetryPolicy(policy, backoff)

        return policy


class IndefiniteRetryPolicy(RetryPolicy):  # pragma: noqa
    """Retry immediately forever."""
    def yield_attempts(self):
        while True:
            yield None


class MaximumAttemptsRetryPolicy(RetryPolicy):
    """Constrain the total number of attempts."""
    def __init__(self, policy, attempts):
        self.subpolicy = policy
        self.attempts = attempts

    def yield_attempts(self):
        for i, remaining in enumerate(self.subpolicy):
            if i == self.attempts:
                logger.error("Retry attempt budget of %d exhausted", self.attempts)
                break

            if i > 0:
                logger.info("Attempting retry %d of %d", i, self.attempts)

            yield remaining


class TimeBudgetRetryPolicy(RetryPolicy):
    """Constrain attempts to an overall time budget."""
    def __init__(self, policy, budget):
        assert budget >= 0, "The time budget must not be negative."
        self.subpolicy = policy
        self.budget = budget

    def yield_attempts(self):
        start_time = time.time()

        yield self.budget

        # starting enumeration with 1 because of yield above
        for retry_attempt, _ in enumerate(self.subpolicy, start=1):
            elapsed = time.time() - start_time
            time_remaining = self.budget - elapsed
            if time_remaining <= 0:
                logger.error("Retried %d times. Budget of %6.2f seconds exceeded",
                             retry_attempt, self.budget)
                break
            logger.warn("%d retry attempt(s). %6.2f seconds remaining in budget",
                        retry_attempt, time_remaining)
            yield time_remaining


class ExponentialBackoffRetryPolicy(RetryPolicy):
    """Sleep exponentially longer between attempts."""
    def __init__(self, policy, base):
        self.subpolicy = policy
        self.base = base

    def yield_attempts(self):
        for attempt, time_remaining in enumerate(self.subpolicy):
            if attempt > 0:
                delay = self.base * 2**(attempt-1)
                if time_remaining:
                    delay = min(delay, time_remaining)
                    time_remaining -= delay

                logger.warn("Retry attempt %d, waiting %6.2f seconds", attempt, delay)
                time.sleep(delay)

            yield time_remaining
