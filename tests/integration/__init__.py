from baseplate.core import BaseplateObserver, SpanObserver


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
        assert self.tags.get(key) == value

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
