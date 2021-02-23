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
    """Circuit breaker.

    The circuit breaker has 3 states:
    * WORKING (closed)
    * TRIPPED (open)
    * TESTING (half open)

    During normal operation the circuit breaker is in the WORKING state.

    When the number of failures exceeds the threshold the breaker moves to the TRIPPED state. It
    stays in this state for the timeout period.

    After the timeout period passes the breaker moves to the TESTING state. If the next attempt
    is successful the breaker moves to the WORKING state. If the next attempt is a failure the
    breaker moves back to the TRIPPED state.

    :param name: full name/path of the circuit breaker
    :param samples: number of previous results used to calculate the trip failure ratio
    :param trip_failure_percent: the minimum ratio of sampled failed results to trip the breaker
    :param trip_for: how long to remain tripped before resetting the breaker
    :param fuzz_ratio: how much to randomly add/subtract to the trip_for time
    """

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
        """Register a success or failure.

        This may cause the state to change.

        :param success: Whether the attempt was a success (not a failure).
        """
        # This breaker has already tripped, so ignore the "late" registrations
        if self.state == BreakerState.TRIPPED:
            return

        if not success:
            self.failures += 1

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
        """Change state to TRIPPED and set the timeout after which state will change to TESTING."""
        if self.fuzz_ratio > 0.0:
            fuzz_ratio = ((2 * random()) - 1.0) * self.fuzz_ratio
            fuzz_ratio = 1 + fuzz_ratio
        else:
            fuzz_ratio = 1.0

        self.tripped_until = datetime.utcnow() + (self.trip_for * fuzz_ratio)
        self.set_state(BreakerState.TRIPPED)

    def reset(self) -> None:
        """Reset to freshly initialized WORKING state."""
        self.results_bucket.clear()
        self.failures = 0
        self._is_bucket_full = False
        self.tripped_until = datetime.utcnow()
        self.set_state(BreakerState.WORKING)
