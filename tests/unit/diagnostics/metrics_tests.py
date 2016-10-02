from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.core import RootSpan
from baseplate.metrics import Client, Batch
from baseplate.diagnostics.metrics import (
    MetricsBaseplateObserver,
    MetricsRootSpanObserver,
    MetricsSpanObserver,
)

from ... import mock


class ObserverTests(unittest.TestCase):
    def test_add_to_context(self):
        mock_client = mock.Mock(spec=Client)
        mock_batch = mock_client.batch.return_value
        mock_context = mock.Mock()
        mock_root_span = mock.Mock(spec=RootSpan)
        mock_root_span.name = "name"

        observer = MetricsBaseplateObserver(mock_client)
        observer.on_root_span_created(mock_context, mock_root_span)
        self.assertEqual(mock_batch.timer.call_args, mock.call("server.name"))

        self.assertEqual(mock_context.metrics, mock_batch)
        self.assertEqual(mock_root_span.register.call_count, 1)


class RootSpanObserverTests(unittest.TestCase):
    def test_root_span_events(self):
        mock_batch = mock.Mock(spec=Batch)
        mock_timer = mock_batch.timer.return_value

        observer = MetricsRootSpanObserver(mock_batch, "request_name")

        observer.on_start()
        self.assertEqual(mock_timer.start.call_count, 1)

        observer.on_stop(exc_info=None)
        self.assertEqual(mock_timer.stop.call_count, 1)
        self.assertEqual(mock_batch.flush.call_count, 1)


class SpanObserverTests(unittest.TestCase):
    def test_timer(self):
        mock_batch = mock.Mock(spec=Batch)
        mock_timer = mock_batch.timer.return_value

        observer = MetricsSpanObserver(mock_batch, "example")
        self.assertEqual(mock_batch.timer.call_count, 1)
        self.assertEqual(mock_batch.timer.call_args, mock.call("example"))

        observer.on_start()
        self.assertEqual(mock_timer.start.call_count, 1)

        observer.on_stop(exc_info=None)
        self.assertEqual(mock_timer.stop.call_count, 1)
