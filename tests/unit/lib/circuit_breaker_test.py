from datetime import datetime
from datetime import timedelta

import pytest

from baseplate.lib.circuit_breaker.breaker import Breaker
from baseplate.lib.circuit_breaker.breaker import BreakerState

SAMPLES=4


@pytest.fixture
def breaker():
    return Breaker(
        name="test",
        samples=SAMPLES,
        trip_failure_ratio=0.5,
        trip_for=timedelta(seconds=60),
        fuzz_ratio=0.1,
    )


@pytest.fixture
def tripped_breaker(breaker):
    for attempt in [True, True, False, False]:
        breaker.register_attempt(attempt)
    return breaker


@pytest.fixture
def tripped_exact_breaker(breaker):
    breaker.fuzz_ratio = 0.0
    for attempt in [True, True, False, False]:
        breaker.register_attempt(attempt)
    return breaker


@pytest.fixture
def testing_breaker(tripped_breaker):
    tripped_breaker.tripped_until = datetime.utcnow()
    return tripped_breaker


@pytest.mark.parametrize(
    "attempts,expected_state",
    [
        ([], BreakerState.WORKING),
        ([True], BreakerState.WORKING),
        ([False], BreakerState.WORKING),
        ([True, True], BreakerState.WORKING),
        ([False, False], BreakerState.WORKING),
        ([True, True, True], BreakerState.WORKING),
        ([False, False, False], BreakerState.WORKING),
        ([True, True, True, True], BreakerState.WORKING),
        ([True, True, True, False], BreakerState.WORKING),
        ([True, True, False, True], BreakerState.WORKING),
        ([True, False, True, True], BreakerState.WORKING),
        ([False, True, True, True], BreakerState.WORKING),
        ([False, False, False, False], BreakerState.TRIPPED),
        ([True, True, False, False], BreakerState.TRIPPED),
        ([False, False, True, True], BreakerState.TRIPPED),
        ([True, False, True, False], BreakerState.TRIPPED),
        ([False, True, False, True], BreakerState.TRIPPED),
        (2*SAMPLES*[True] + 2*SAMPLES*[False], BreakerState.TRIPPED),
    ],
)
def test_breaker_state(breaker, attempts, expected_state):
    for attempt in attempts:
        breaker.register_attempt(attempt)
    assert breaker.state == expected_state
    # Check for leaks
    assert len(breaker.results_bucket) <= SAMPLES
    assert 0 <= breaker.failures <= SAMPLES


def test_testing_state(testing_breaker):
    assert testing_breaker.state == BreakerState.TESTING


def test_trip_after_successful_test(testing_breaker):
    testing_breaker.register_attempt(True)
    assert testing_breaker.state == BreakerState.WORKING


def test_trip_after_failed_test(testing_breaker):
    testing_breaker.register_attempt(False)
    assert testing_breaker.state == BreakerState.TRIPPED


def test_late_register_success(tripped_breaker):
    tripped_breaker.register_attempt(True)
    assert tripped_breaker.state == BreakerState.TRIPPED
    assert tripped_breaker.failures == 2


def test_late_register_failure(tripped_breaker):
    tripped_breaker.register_attempt(False)
    assert tripped_breaker.state == BreakerState.TRIPPED
    assert tripped_breaker.failures == 2


def test_trip_for_exact(tripped_exact_breaker):
    assert tripped_exact_breaker.fuzz_ratio == 0.0
    expected_tripped_until = datetime.utcnow() + timedelta(seconds=60)
    assert tripped_exact_breaker.tripped_until <= expected_tripped_until


def test_trip_for_fuzzing(tripped_breaker):
    assert tripped_breaker.fuzz_ratio == 0.1
    expected_tripped_until = datetime.utcnow() + timedelta(seconds=60)
    delta = abs(tripped_breaker.tripped_until - expected_tripped_until)
    assert delta <= timedelta(seconds=6, milliseconds=1)
