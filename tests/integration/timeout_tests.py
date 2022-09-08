import gevent.monkey
import pytest

from baseplate import Baseplate
from baseplate.observers.timeout import ServerTimeout
from baseplate.observers.timeout import TimeoutBaseplateObserver


def _create_baseplate_object(timeout: str):
    baseplate = Baseplate()

    observer = TimeoutBaseplateObserver.from_config({"server_timeout.default": timeout})
    baseplate.register(observer)
    return baseplate


def test_default_timeout():
    baseplate = _create_baseplate_object("50 milliseconds")
    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        with pytest.raises(ServerTimeout):
            gevent.sleep(1)

    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        gevent.sleep(0)  # shouldn't time out since it's so fast!


def test_route_specific_timeout():
    baseplate = _create_baseplate_object("5 milliseconds")
    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        with pytest.raises(ServerTimeout):
            gevent.sleep(1)

    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        gevent.sleep(0)


def test_timeout_from_context():
    baseplate = _create_baseplate_object("1 hour")
    baseplate.add_to_context("deadline_budget", 0.01)

    # tests short deadline_budget causes timeout
    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        with pytest.raises(ServerTimeout):
            gevent.sleep(1)

    # tests long deadline_budget and server timeout doesn't timeout
    context = baseplate.make_context_object()
    baseplate.add_to_context("deadline_budget", 1000)
    with baseplate.make_server_span(context, "test"):
        gevent.sleep(0.01)  # shouldn't time out since timeouts are so long


def test_pool_timeout_with_long_deadline_budget():
    baseplate = _create_baseplate_object("5 milliseconds")
    baseplate.add_to_context("deadline_budget", 1000)

    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        with pytest.raises(ServerTimeout):
            gevent.sleep(0.01)
