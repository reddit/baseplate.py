from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.core import (
    Baseplate,
    BaseplateObserver,
    ServerSpan,
    ServerSpanObserver,
    Span,
    SpanObserver,
    TraceInfo,
)

from .. import mock


class BaseplateTests(unittest.TestCase):
    def test_server_observer_made(self):
        mock_context = mock.Mock()
        mock_observer = mock.Mock(spec=BaseplateObserver)

        baseplate = Baseplate()
        baseplate.register(mock_observer)
        server_span = baseplate.make_server_span(mock_context, "name", TraceInfo(1, 2, 3))

        self.assertEqual(baseplate.observers, [mock_observer])
        self.assertEqual(mock_observer.on_server_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_server_span_created.call_args,
            mock.call(mock_context, server_span))

    def test_null_server_observer(self):
        mock_context = mock.Mock()
        mock_observer = mock.Mock(spec=BaseplateObserver)
        mock_observer.on_server_span_created.return_value = None

        baseplate = Baseplate()
        baseplate.register(mock_observer)
        server_span = baseplate.make_server_span(mock_context, "name", TraceInfo(1, 2, 3))

        self.assertEqual(server_span.observers, [])


class SpanTests(unittest.TestCase):
    def test_events(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = Span(1, 2, 3, "name")
        span.register(mock_observer)

        span.start()
        self.assertEqual(mock_observer.on_start.call_count, 1)

        span.set_tag("key", "value")
        mock_observer.on_set_tag("key", "value")

        span.log("name", "payload")
        mock_observer.on_log("name", "payload")

        span.finish()
        mock_observer.on_finish(exc_info=None)

    def test_context(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = Span(1, 2, 3, "name")
        span.register(mock_observer)

        with span:
            self.assertEqual(mock_observer.on_start.call_count, 1)
        self.assertEqual(mock_observer.on_finish.call_count, 1)

    def test_context_with_exception(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = Span(1, 2, 3, "name")
        span.register(mock_observer)

        class TestException(Exception):
            pass

        exc = TestException()
        with self.assertRaises(TestException):
            with span:
                raise exc
        self.assertEqual(mock_observer.on_finish.call_count, 1)
        _, captured_exc, _ = mock_observer.on_finish.call_args[0][0]
        self.assertEqual(captured_exc, exc)


class ServerSpanTests(unittest.TestCase):
    @mock.patch("random.getrandbits", autospec=True)
    def test_make_child(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE

        mock_observer = mock.Mock(spec=ServerSpanObserver)

        server_span = ServerSpan("trace", "parent", "id", "name")
        server_span.register(mock_observer)
        child_span = server_span.make_child("child_name")

        self.assertEqual(child_span.name, "child_name")
        self.assertEqual(child_span.id, 0xCAFE)
        self.assertEqual(child_span.trace_id, "trace")
        self.assertEqual(child_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args,
            mock.call(child_span))

    def test_null_child(self):
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_observer.on_child_span_created.return_value = None

        server_span = ServerSpan("trace", "parent", "id", "name")
        server_span.register(mock_observer)
        child_span = server_span.make_child("child_name")

        self.assertEqual(child_span.observers, [])
