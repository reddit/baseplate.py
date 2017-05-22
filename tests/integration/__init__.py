import socket
import unittest

from baseplate.core import BaseplateObserver, SpanObserver


def skip_if_server_unavailable(name, port):
    """Raise SkipTest if the given server is not available.

    This is useful for running tests in environments where we can't launch
    servers.

    """

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(.1)
        sock.connect(("localhost", port))
    except socket.error:
        raise unittest.SkipTest("local %s does not appear available" % name)
    else:
        sock.close()


class TestSpanObserver(SpanObserver):
    def __init__(self, span):
        self.span = span
        self.on_start_called = False
        self.on_finish_called = False
        self.on_finish_exc_info = None
        self.tags = {}
        self.logs = []
        self.children = []

    def on_start(self):
        """Called when the observed span is started."""
        assert not self.on_start_called, "start was already called on this span"
        self.on_start_called = True

    def on_set_tag(self, key, value):
        """Called when a tag is set on the observed span."""
        self.tags[key] = value

    def assert_tag(self, key, value):
        assert key in self.tags, "{!r} not found in tags ({!r})".format(
            key, list(self.tags.keys()))
        assert self.tags[key] == value, "tag {!r}: expected value {!r} but found {!r}".format(
            key, value, self.tags[key])

    def on_log(self, name, payload):
        """Called when a log entry is added to the span."""
        self.logs.append((name, payload))

    def on_finish(self, exc_info):
        """Called when the observed span is finished.

        :param exc_info: If the span ended because of an exception, the
            exception info. Otherwise, :py:data:`None`.

        """
        assert not self.on_finish_called, "finish was already called on this span"
        self.on_finish_called = True
        self.on_finish_exc_info = exc_info

    def on_child_span_created(self, span):  # pragma: nocover
        child = TestSpanObserver(span)
        self.children.append(child)
        span.register(child)

    def get_only_child(self):
        assert len(self.children) == 1, "observer has wrong number of children"
        return self.children[0]


class TestBaseplateObserver(BaseplateObserver):
    def __init__(self):
        self.children = []

    def get_only_child(self):
        assert len(self.children) == 1, "observer has wrong number of children"
        return self.children[0]

    def on_server_span_created(self, context, server_span):
        child = TestSpanObserver(server_span)
        self.children.append(child)
        server_span.register(child)
