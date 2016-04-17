from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import itertools
import unittest

from baseplate.retry import (
    ExponentialBackoffRetryPolicy,
    IndefiniteRetryPolicy,
    MaximumAttemptsRetryPolicy,
    TimeBudgetRetryPolicy,
)

from .. import mock


class RetryPolicyTests(unittest.TestCase):
    def test_maximum_attempts(self):
        base_policy = mock.MagicMock()
        base_policy.__iter__.return_value = itertools.repeat(1.2)
        policy = MaximumAttemptsRetryPolicy(base_policy, attempts=3)

        retries = iter(policy)
        self.assertEqual(next(retries), 1.2)
        self.assertEqual(next(retries), 1.2)
        self.assertEqual(next(retries), 1.2)
        with self.assertRaises(StopIteration):
            next(retries)

    @mock.patch("time.time", autospec=True)
    def test_time_budget(self, time):
        policy = TimeBudgetRetryPolicy(IndefiniteRetryPolicy(), budget=5)

        time.return_value = 0
        retries = iter(policy)
        self.assertEqual(next(retries), 5)

        time.return_value = 3
        for _ in range(100):
            self.assertEqual(next(retries), 2)

        time.return_value = 7
        with self.assertRaises(StopIteration):
            next(retries)

    @mock.patch("time.sleep", autospec=True)
    def test_exponential_backoff(self, sleep):
        base_policy = mock.MagicMock()
        base_policy.__iter__.return_value = itertools.repeat(.9)
        policy = ExponentialBackoffRetryPolicy(base_policy, base=.1)

        retries = iter(policy)
        next(retries)
        self.assertEqual(sleep.call_count, 0)

        next(retries)
        sleep.assert_called_with(.1)

        next(retries)
        sleep.assert_called_with(.2)

        next(retries)
        sleep.assert_called_with(.4)

        next(retries)
        sleep.assert_called_with(.8)

        # the base policy's lower time remaining takes over here
        next(retries)
        sleep.assert_called_with(.9)
