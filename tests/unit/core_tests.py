from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.core import (
    Baseplate,
    BaseplateObserver,
    RootSpan,
    RootSpanObserver,
    Span,
    SpanObserver,
    TraceInfo,
)

from .. import mock


class BaseplateTests(unittest.TestCase):
    def test_root_observer_made(self):
        mock_context = mock.Mock()
        mock_observer = mock.Mock(spec=BaseplateObserver)

        baseplate = Baseplate()
        baseplate.register(mock_observer)
        root_span = baseplate.make_root_span(mock_context, "name", TraceInfo(1, 2, 3))

        self.assertEqual(baseplate.observers, [mock_observer])
        self.assertEqual(mock_observer.on_root_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_root_span_created.call_args,
            mock.call(mock_context, root_span))

    def test_null_root_observer(self):
        mock_context = mock.Mock()
        mock_observer = mock.Mock(spec=BaseplateObserver)
        mock_observer.on_root_span_created.return_value = None

        baseplate = Baseplate()
        baseplate.register(mock_observer)
        root_span = baseplate.make_root_span(mock_context, "name", TraceInfo(1, 2, 3))

        self.assertEqual(root_span.observers, [])


class SpanTests(unittest.TestCase):
    def test_events(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = Span(1, 2, 3, "name")
        span.register(mock_observer)

        span.start()
        self.assertEqual(mock_observer.on_start.call_count, 1)

        span.annotate("key", "value")
        mock_observer.on_annotate("key", "value")

        span.stop()
        mock_observer.on_stop(error=None)

    def test_context(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = Span(1, 2, 3, "name")
        span.register(mock_observer)

        with span:
            self.assertEqual(mock_observer.on_start.call_count, 1)
        self.assertEqual(mock_observer.on_stop.call_count, 1)

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
        self.assertEqual(mock_observer.on_stop.call_count, 1)
        self.assertEqual(mock_observer.on_stop.call_args, mock.call(error=exc))


class RootSpanTests(unittest.TestCase):
    @mock.patch("random.getrandbits", autospec=True)
    def test_make_child(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE

        mock_observer = mock.Mock(spec=RootSpanObserver)

        root_span = RootSpan("trace", "parent", "id", "name")
        root_span.register(mock_observer)
        child_span = root_span.make_child("child_name")

        self.assertEqual(child_span.name, "child_name")
        self.assertEqual(child_span.id, 0xCAFE)
        self.assertEqual(child_span.trace_id, "trace")
        self.assertEqual(child_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args,
            mock.call(child_span))

    def test_null_child(self):
        mock_observer = mock.Mock(spec=RootSpanObserver)
        mock_observer.on_child_span_created.return_value = None

        root_span = RootSpan("trace", "parent", "id", "name")
        root_span.register(mock_observer)
        child_span = root_span.make_child("child_name")

        self.assertEqual(child_span.observers, [])
