import unittest

from unittest import mock

from baseplate import LocalSpan
from baseplate import ServerSpan
from baseplate import Span
from baseplate.lib.metrics import Batch
from baseplate.lib.metrics import Client
from baseplate.lib.metrics import Counter
from baseplate.lib.metrics import Timer
from baseplate.observers.metrics import MetricsBaseplateObserver
from baseplate.observers.metrics import MetricsClientSpanObserver
from baseplate.observers.metrics import MetricsLocalSpanObserver
from baseplate.observers.metrics import MetricsServerSpanObserver


class TestException(Exception):
    pass


class ObserverTests(unittest.TestCase):
    def test_add_to_context(self):
        mock_client = mock.Mock(spec=Client)
        mock_batch = mock_client.batch.return_value
        mock_context = mock.Mock()
        mock_server_span = mock.Mock(spec=ServerSpan)
        mock_server_span.name = "name"

        observer = MetricsBaseplateObserver(mock_client)
        observer.on_server_span_created(mock_context, mock_server_span)

        self.assertEqual(mock_context.metrics, mock_batch)
        self.assertEqual(mock_server_span.register.call_count, 1)


class ServerSpanObserverTests(unittest.TestCase):
    def test_server_span_events(self):
        mock_batch = mock.Mock(spec=Batch)
        mock_timer = mock.Mock(spec=Timer)
        mock_counter = mock.Mock(spec=Counter)
        mock_batch.timer.return_value = mock_timer
        mock_batch.counter.return_value = mock_counter

        mock_server_span = mock.Mock(spec=ServerSpan)
        mock_server_span.name = "request_name"

        observer = MetricsServerSpanObserver(mock_batch, mock_server_span)

        observer.on_start()
        self.assertEqual(mock_timer.start.call_count, 1)

        observer.on_incr_tag("test", delta=1)
        self.assertEqual(mock_counter.increment.call_count, 1)

        mock_child_span = mock.Mock()
        mock_child_span.name = "example"
        observer.on_child_span_created(mock_child_span)
        self.assertEqual(mock_child_span.register.call_count, 1)

        observer.on_finish(exc_info=None)
        self.assertEqual(mock_timer.stop.call_count, 1)
        self.assertEqual(mock_batch.flush.call_count, 1)


class ClientSpanObserverTests(unittest.TestCase):
    def test_metrics(self):
        mock_timer = mock.Mock(spec=Timer)
        mock_counter = mock.Mock(spec=Counter)
        mock_batch = mock.Mock(spec=Batch)
        mock_batch.timer.return_value = mock_timer
        mock_batch.counter.return_value = mock_counter

        mock_client_span = mock.Mock(spec=Span)
        mock_client_span.name = "example"

        observer = MetricsClientSpanObserver(mock_batch, mock_client_span)
        self.assertEqual(mock_batch.timer.call_count, 1)
        self.assertEqual(mock_batch.timer.call_args, mock.call("clients.example"))

        observer.on_start()
        self.assertEqual(mock_timer.start.call_count, 1)

        observer.on_incr_tag("test", delta=1)
        mock_counter.increment.assert_called()
        mock_counter.reset_mock()

        observer.on_finish(exc_info=None)
        self.assertEqual(mock_timer.stop.call_count, 1)
        self.assertEqual(mock_counter.increment.call_count, 1)

        mock_counter.reset_mock()
        observer.on_log(name="error.object", payload=TestException())
        self.assertEqual(mock_counter.increment.call_count, 1)
        self.assertEqual(mock_batch.counter.call_args, mock.call("errors.TestException"))


class LocalSpanObserverTests(unittest.TestCase):
    def test_metrics(self):
        mock_timer = mock.Mock(spec=Timer)
        mock_counter = mock.Mock(spec=Counter)
        mock_batch = mock.Mock(spec=Batch)
        mock_batch.timer.return_value = mock_timer
        mock_batch.counter.return_value = mock_counter

        mock_local_span = mock.Mock(spec=LocalSpan)
        mock_local_span.name = "example"
        mock_local_span.component_name = "some_component"

        observer = MetricsLocalSpanObserver(mock_batch, mock_local_span)
        self.assertEqual(mock_batch.timer.call_count, 1)
        self.assertEqual(mock_batch.timer.call_args, mock.call("some_component.example"))

        observer.on_start()
        self.assertEqual(mock_timer.start.call_count, 1)

        observer.on_incr_tag("test", delta=1)
        mock_counter.increment.assert_called()
        mock_counter.reset_mock()

        observer.on_finish(exc_info=None)
        self.assertEqual(mock_timer.stop.call_count, 1)

    def test_spans_under_local_spans(self):
        mock_batch = mock.Mock(spec=Batch)

        mock_local_span = mock.Mock(spec=LocalSpan)
        mock_local_span.name = "example"
        mock_local_span.component_name = "some_component"

        mock_nested_span = mock.Mock(spec=LocalSpan)
        mock_nested_span.name = "nested"
        mock_nested_span.component_name = "some_component2"

        observer = MetricsLocalSpanObserver(mock_batch, mock_local_span)
        observer.on_child_span_created(mock_nested_span)

        self.assertEqual(mock_nested_span.register.call_count, 1)
