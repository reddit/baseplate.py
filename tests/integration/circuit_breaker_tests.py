import unittest

from collections import deque
from datetime import datetime
from datetime import timedelta
from unittest import mock

import pytest

from pytz import UTC

from baseplate import Baseplate
from baseplate.clients import ContextFactory
from baseplate.lib.circuit_breaker import breaker_box_from_config
from baseplate.lib.circuit_breaker import CircuitBreakerClientWrapperFactory
from baseplate.lib.circuit_breaker.breaker import BreakerState
from baseplate.lib.circuit_breaker.errors import BreakerTrippedError

from . import TestBaseplateObserver


class TestClientFactory(ContextFactory):
    def __init__(self, client):
        self.client = client

    def make_object_for_context(self, name, span):
        return self.client


class CircuitBreakerTests(unittest.TestCase):
    def setUp(self):
        self.breaker_box = breaker_box_from_config(
            app_config={
                "brkr.samples": "4",
                "brkr.trip_failure_ratio": "0.75",
                "brkr.trip_for": "1 minute",
                "brkr.fuzz_ratio": "0.1",
            },
            name="test_breaker",
            prefix="brkr.",
        )

        self.client = mock.Mock()
        client_factory = TestClientFactory(self.client)

        wrapped_client_factory = CircuitBreakerClientWrapperFactory(
            client_factory, self.breaker_box
        )

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("wrapped_client", wrapped_client_factory)

        self.context = baseplate.make_context_object()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_breaker_box(self):
        breaker_box = self.context.wrapped_client.breaker_box
        assert breaker_box.name == "test_breaker"
        assert breaker_box.samples == 4
        assert breaker_box.trip_failure_ratio == 0.75
        assert breaker_box.trip_for == timedelta(seconds=60)
        assert breaker_box.fuzz_ratio == 0.1

    @mock.patch("baseplate.lib.circuit_breaker.breaker.random")
    @mock.patch("baseplate.lib.circuit_breaker.breaker.datetime")
    def test_breaker_context(self, datetime_mock, random_mock):
        self.client.get_something.side_effect = [None, AttributeError, KeyError, ValueError]

        datetime_mock.utcnow.side_effect = [
            datetime(2021, 2, 25, 0, 0, 1, tzinfo=UTC),
            datetime(2021, 2, 25, 0, 0, 2, tzinfo=UTC),
        ]

        random_mock.return_value = 0.1

        def make_call(_server_span, _context, *args):
            with _server_span:
                breaker_context = _context.wrapped_client.breaker_context(
                    operation="get_something", breakable_exceptions=(KeyError, ValueError),
                )

                with breaker_context as client:
                    client.get_something(*args)

        # test a few calls
        make_call(self.server_span, self.context, "a")

        with pytest.raises(AttributeError):
            # note that this is not in `breakable_exceptions` so
            # counts as a success
            make_call(self.server_span, self.context, "b")

        with pytest.raises(KeyError):
            make_call(self.server_span, self.context, "c")

        with pytest.raises(ValueError):
            make_call(self.server_span, self.context, "d")

        self.client.get_something.assert_has_calls(
            [mock.call("a"), mock.call("b"), mock.call("c"), mock.call("d")]
        )

        breaker = self.breaker_box.breaker_box["get_something"]
        assert breaker.name == "test_breaker.get_something"
        assert breaker.results_bucket == deque([True, True, False, False])
        assert breaker.state == BreakerState.WORKING
        assert breaker.tripped_until == datetime(2021, 2, 25, 0, 0, 2, tzinfo=UTC)

        # push into failed state
        datetime_mock.utcnow.side_effect = [
            datetime(2021, 2, 25, 0, 0, 3, tzinfo=UTC),
            datetime(2021, 2, 25, 0, 0, 4, tzinfo=UTC),
            datetime(2021, 2, 25, 0, 0, 5, tzinfo=UTC),
        ]

        self.client.get_something.reset_mock()
        self.client.get_something.side_effect = [ValueError]

        with pytest.raises(ValueError):
            make_call(self.server_span, self.context, "e")

        self.client.get_something.assert_called_once_with("e")

        breaker = self.breaker_box.breaker_box["get_something"]
        assert breaker.name == "test_breaker.get_something"
        assert breaker.results_bucket == deque([True, False, False, False])
        assert breaker.state == BreakerState.TRIPPED
        assert breaker.tripped_until == datetime(2021, 2, 25, 0, 0, 58, 200000, tzinfo=UTC)

        # call while in failed state
        datetime_mock.utcnow.side_effect = [
            datetime(2021, 2, 25, 0, 0, 6, tzinfo=UTC),
            datetime(2021, 2, 25, 0, 0, 7, tzinfo=UTC),
        ]

        self.client.get_something.reset_mock()
        self.client.get_something.return_value = None

        with pytest.raises(BreakerTrippedError):
            make_call(self.server_span, self.context, "f")

        self.client.get_something.assert_not_called()

        breaker = self.breaker_box.breaker_box["get_something"]
        assert breaker.name == "test_breaker.get_something"
        assert breaker.results_bucket == deque([True, False, False, False])
        assert breaker.state == BreakerState.TRIPPED
        assert breaker.tripped_until == datetime(2021, 2, 25, 0, 0, 58, 200000, tzinfo=UTC)

        # call (and fail) while in testing state
        datetime_mock.utcnow.side_effect = [
            datetime(2021, 2, 25, 0, 1, 0, tzinfo=UTC),
            datetime(2021, 2, 25, 0, 1, 1, tzinfo=UTC),
            datetime(2021, 2, 25, 0, 1, 2, tzinfo=UTC),
            datetime(2021, 2, 25, 0, 1, 3, tzinfo=UTC),
        ]

        self.client.get_something.reset_mock()
        self.client.get_something.side_effect = [ValueError]

        assert breaker.state == BreakerState.TESTING

        with pytest.raises(ValueError):
            make_call(self.server_span, self.context, "g")

        self.client.get_something.assert_called_once_with("g")

        assert breaker.results_bucket == deque([False, False, False, False])
        assert breaker.state == BreakerState.TRIPPED
        assert breaker.tripped_until == datetime(2021, 2, 25, 0, 1, 56, 200000, tzinfo=UTC)

        # call (and succeed) while in testing state
        datetime_mock.utcnow.side_effect = [
            datetime(2021, 2, 25, 0, 2, 0, tzinfo=UTC),
            datetime(2021, 2, 25, 0, 2, 1, tzinfo=UTC),
        ]

        self.client.get_something.reset_mock()
        self.client.get_something.side_effect = [None]

        assert breaker.state == BreakerState.TESTING

        make_call(self.server_span, self.context, "h")

        self.client.get_something.assert_called_once_with("h")

        assert breaker.results_bucket == deque([])
        assert breaker.state == BreakerState.WORKING
        assert breaker.tripped_until == datetime(2021, 2, 25, 0, 2, 1, tzinfo=UTC)
