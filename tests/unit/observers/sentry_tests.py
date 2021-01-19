from typing import Any
from typing import Dict

import gevent
import pytest
import sentry_sdk

from baseplate import Baseplate
from baseplate.observers.sentry import _SentryUnhandledErrorReporter
from baseplate.observers.sentry import init_sentry_client_from_config
from baseplate.observers.sentry import SentryBaseplateObserver


class FakeTransport:
    def __init__(self):
        self.events = []

    def __call__(self, event: Dict[str, Any]) -> None:
        self.events.append(event)


@pytest.fixture
def sentry_transport():
    return FakeTransport()


@pytest.fixture(autouse=True)
def init_sentry_client(sentry_transport):
    try:
        init_sentry_client_from_config({"sentry.dsn": "foo"}, transport=sentry_transport)
        yield
    finally:
        sentry_sdk.init()  # shut everything down


@pytest.fixture
def baseplate_app():
    baseplate = Baseplate()
    baseplate.register(SentryBaseplateObserver())
    return baseplate


def test_no_event_when_nothing_wrong(baseplate_app, sentry_transport):
    with baseplate_app.server_context("test"):
        pass

    assert not sentry_transport.events


def test_event_when_exception(baseplate_app, sentry_transport):
    with pytest.raises(ValueError):
        with baseplate_app.server_context("test"):
            raise ValueError("oops")

    assert len(sentry_transport.events) == 1
    event = sentry_transport.events[0]
    assert event["exception"]["values"][0]["type"] == "ValueError"


def test_tags(baseplate_app, sentry_transport):
    with pytest.raises(ValueError):
        with baseplate_app.server_context("test") as context:
            context.span.set_tag("foo", "bar")
            raise ValueError("oops")

    assert len(sentry_transport.events) == 1
    event = sentry_transport.events[0]
    assert event["exception"]["values"][0]["type"] == "ValueError"
    assert event["tags"]["foo"] == "bar"

    with pytest.raises(ValueError):
        with baseplate_app.server_context("test") as context:
            context.span.set_tag("different-tag", "foo")
            raise ValueError("oops")

    assert len(sentry_transport.events) == 2
    event = sentry_transport.events[1]
    assert event["exception"]["values"][0]["type"] == "ValueError"
    assert event["tags"]["different-tag"] == "foo"
    assert "foo" not in event["tags"]


def test_logs(baseplate_app, sentry_transport):
    with pytest.raises(ValueError):
        with baseplate_app.server_context("test") as context:
            context.span.log("foo-category", "bar-log-entry")
            raise ValueError("oops")

    assert len(sentry_transport.events) == 1
    event = sentry_transport.events[0]
    assert event["exception"]["values"][0]["type"] == "ValueError"

    last_breadcrumb = event["breadcrumbs"]["values"][-1]
    assert last_breadcrumb["category"] == "foo-category"
    assert last_breadcrumb["message"] == "bar-log-entry"


def test_ignored_exception_ignored(baseplate_app, sentry_transport):
    with pytest.raises(ConnectionError):
        with baseplate_app.server_context("test"):
            raise ConnectionError("oops")

    assert not sentry_transport.events


def test_unhandled_error_reporter(sentry_transport):
    def raise_unhandled_error():
        raise KeyError("foo")

    try:
        _SentryUnhandledErrorReporter.install()

        greenlet = gevent.spawn(raise_unhandled_error)
        greenlet.join()
    finally:
        _SentryUnhandledErrorReporter.uninstall()

    assert len(sentry_transport.events) == 1
    event = sentry_transport.events[0]
    assert event["exception"]["values"][0]["type"] == "KeyError"


def test_unhandled_error_reporter_server_timeout(sentry_transport):
    def raise_unhandled_error():
        from baseplate.observers.timeout import ServerTimeout

        raise ServerTimeout("blah", 1.0, debug=False)

    try:
        _SentryUnhandledErrorReporter.install()

        greenlet = gevent.spawn(raise_unhandled_error)
        greenlet.join()
    finally:
        _SentryUnhandledErrorReporter.uninstall()

    assert not sentry_transport.events
