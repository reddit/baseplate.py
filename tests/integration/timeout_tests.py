import gevent.monkey
import pytest

from baseplate import Baseplate
from baseplate.observers.timeout import ServerTimeout
from baseplate.observers.timeout import TimeoutBaseplateObserver


def test_default_timeout():
    baseplate = Baseplate()

    observer = TimeoutBaseplateObserver.from_config({"server_timeout.default": "50 milliseconds"})
    baseplate.register(observer)

    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        with pytest.raises(ServerTimeout):
            gevent.sleep(1)

    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        gevent.sleep(0)  # shouldn't time out since it's so fast!


def test_route_specific_timeout():
    baseplate = Baseplate()

    observer = TimeoutBaseplateObserver.from_config(
        {"server_timeout.default": "1 hour", "server_timeout.by_endpoint.test": "5 milliseconds"}
    )
    baseplate.register(observer)

    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        with pytest.raises(ServerTimeout):
            gevent.sleep(1)

    context = baseplate.make_context_object()
    with baseplate.make_server_span(context, "test"):
        gevent.sleep(0)  # shouldn't time out since it's so fast!
