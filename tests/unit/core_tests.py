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
)

from .. import mock


class BaseplateTests(unittest.TestCase):
    def test_root_observer_made(self):
        mock_context = mock.Mock()
        mock_observer = mock.Mock(spec=BaseplateObserver)

        baseplate = Baseplate()
        baseplate.register(mock_observer)
        root_span = baseplate.make_root_span(mock_context, 1, 2, "name", 3)

        self.assertEqual(baseplate.observers, [mock_observer])
        self.assertEqual(mock_observer.make_root_observer.call_count, 1)
        self.assertEqual(mock_observer.make_root_observer.call_args,
            mock.call(mock_context, root_span))
        self.assertEqual(root_span.observers,
            [mock_observer.make_root_observer.return_value])

    def test_null_root_observer(self):
        mock_context = mock.Mock()
        mock_observer = mock.Mock(spec=BaseplateObserver)
        mock_observer.make_root_observer.return_value = None

        baseplate = Baseplate()
        baseplate.register(mock_observer)
        root_span = baseplate.make_root_span(mock_context, 1, 2, "name", 3)

        self.assertEqual(root_span.observers, [])


class SpanTests(unittest.TestCase):
    def test_events(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = Span(1, 2, 3, "name")
        span._register(mock_observer)

        span.start()
        self.assertEqual(mock_observer.on_start.call_count, 1)

        span.annotate("key", "value")
        mock_observer.on_annotate("key", "value")

        span.stop()
        mock_observer.on_stop()

    def test_context(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = Span(1, 2, 3, "name")
        span._register(mock_observer)

        with span:
            self.assertEqual(mock_observer.on_start.call_count, 1)
        self.assertEqual(mock_observer.on_stop.call_count, 1)


class RootSpanTests(unittest.TestCase):
    @mock.patch("random.getrandbits", autospec=True)
    def test_make_child(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE

        mock_child = mock.Mock(spec=Span)

        mock_observer = mock.Mock(spec=RootSpanObserver)
        mock_child = mock_observer.make_child_observer.return_value

        root_span = RootSpan("trace", "parent", "id", "name")
        root_span._register(mock_observer)
        child_span = root_span.make_child("child_name")

        self.assertEqual(child_span.name, "child_name")
        self.assertEqual(child_span.id, 0xCAFE)
        self.assertEqual(child_span.trace_id, "trace")
        self.assertEqual(child_span.parent_id, "id")
        self.assertEqual(child_span.observers, [mock_child])
        self.assertEqual(mock_observer.make_child_observer.call_count, 1)
        self.assertEqual(mock_observer.make_child_observer.call_args,
            mock.call(child_span))

    def test_null_child(self):
        mock_observer = mock.Mock(spec=RootSpanObserver)
        mock_observer.make_child_observer.return_value = None

        root_span = RootSpan("trace", "parent", "id", "name")
        root_span._register(mock_observer)
        child_span = root_span.make_child("child_name")

        self.assertEqual(child_span.observers, [])
