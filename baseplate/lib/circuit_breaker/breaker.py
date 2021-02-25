from collections import deque
from datetime import datetime
from datetime import timedelta
from enum import Enum
from math import ceil
from random import random
from typing import Deque


class BreakerState(Enum):
    WORKING = "working"
    TRIPPED = "tripped"
    # trip immediately after failure
    TESTING = "testing"


class Breaker:
    failures: int
    _state: BreakerState = BreakerState.WORKING
    _is_bucket_full: bool = False

    def __init__(
        self,
        name: str,
        samples: int = 20,
        trip_failure_ratio: float = 0.5,
        trip_for: timedelta = timedelta(minutes=1),
        fuzz_ratio: float = 0.1,
    ):
        """Circuit breaker.

        * name: str - full name/path of the circuit breaker
        * samples: int - number of previous results used to calculate the trip failure ratio
        * trip_failure_percent: float - the minimum ratio of sampled failed results to trip the breaker
        * trip_for: timedelta - how long to remain tripped before resetting the breaker
        * fuzz_ratio: float - how much to randomly add/subtract to the trip_for time
        """
        self.name = name
        self.samples = samples
        self.results_bucket: Deque = deque([], self.samples)
        self.tripped_until: datetime = datetime.utcnow()
        self.trip_threshold = ceil(trip_failure_ratio * samples)
        self.trip_for = trip_for
        self.fuzz_ratio = fuzz_ratio
        self.reset()

    @property
    def state(self) -> BreakerState:
        if self._state == BreakerState.TRIPPED and (datetime.utcnow() >= self.tripped_until):
            self.set_state(BreakerState.TESTING)

        return self._state

    def register_attempt(self, success: bool) -> None:
        # This breaker has already tripped, so ignore the "late" registrations
        if self.state == BreakerState.TRIPPED:
            return

        if not success:
            self.failures += 1

        # When a deque is full, elements are dropped from the left. We need to record it's value before we do so as not
        # to "leak" failures.
        if self._is_bucket_full and not self.results_bucket[0]:
            self.failures -= 1

        self.results_bucket.append(success)

        if not self._is_bucket_full and (len(self.results_bucket) == self.samples):
            self._is_bucket_full = True

        if success and (self.state == BreakerState.TESTING):
            self.reset()
            return

        if self.state == BreakerState.TESTING:
            # failure in the TESTING state trips the breaker immediately
            self.trip()
            return

        if not self._is_bucket_full:
            # no need to check anything if we haven't recorded enough samples
            return

        # check for trip condition
        if self.failures >= self.trip_threshold:
            self.trip()

    def set_state(self, state: BreakerState) -> None:
        self._state = state

    def trip(self) -> None:
        if self.fuzz_ratio > 0.0:
            fuzz_ratio = ((2 * random()) - 1.0) * self.fuzz_ratio
            fuzz_ratio = 1 + fuzz_ratio
        else:
            fuzz_ratio = 1.0

        self.tripped_until = datetime.utcnow() + (self.trip_for * fuzz_ratio)
        self.set_state(BreakerState.TRIPPED)

    def reset(self) -> None:
        self.results_bucket.clear()
        self.failures = 0
        self._is_bucket_full = False
        self.tripped_until = datetime.utcnow()
        self.set_state(BreakerState.WORKING)
