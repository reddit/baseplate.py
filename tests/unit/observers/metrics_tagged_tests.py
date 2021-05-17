import unittest

from unittest import mock

from baseplate import LocalSpan
from baseplate import ServerSpan
from baseplate import Span
from baseplate.lib.metrics import Batch
from baseplate.lib.metrics import Client
from baseplate.lib.metrics import Counter
from baseplate.lib.metrics import Timer
from baseplate.observers.metrics_tagged import Errors
from baseplate.observers.metrics_tagged import TaggedMetricsBaseplateObserver
from baseplate.observers.metrics_tagged import TaggedMetricsClientSpanObserver
from baseplate.observers.metrics_tagged import TaggedMetricsLocalSpanObserver
from baseplate.observers.metrics_tagged import TaggedMetricsServerSpanObserver
from baseplate.observers.timeout import ServerTimeout


class TestException(Exception):
    pass


class ObserverTests(unittest.TestCase):
    def test_add_to_context(self):
        mock_client = mock.Mock(spec=Client)
        mock_batch = mock_client.batch.return_value
        mock_context = mock.Mock()
        mock_server_span = mock.Mock(spec=ServerSpan)
        mock_server_span.name = "name"
        mock_whitelist = ["endpoint", "success", "error", "client"]
        mock_sample_rate = 1.0

        observer = TaggedMetricsBaseplateObserver(mock_client, mock_whitelist, mock_sample_rate)
        observer.on_server_span_created(mock_context, mock_server_span)

        self.assertEqual(mock_context.metrics, mock_batch)
        self.assertEqual(mock_server_span.register.call_count, 1)


class ServerSpanObserverTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.mock_batch = mock.Mock(spec=Batch)
        self.mock_timer = mock.Mock(spec=Timer)
        self.mock_counter = mock.Mock(spec=Counter)
        self.mock_batch.timer.return_value = self.mock_timer
        self.mock_batch.counter.return_value = self.mock_counter
        mock_whitelist = ["endpoint", "success", "error", "incr"]
        mock_whitelist_empty = []

        mock_server_span = mock.Mock(spec=ServerSpan)
        mock_server_span.name = "request_name"

        self.observer = TaggedMetricsServerSpanObserver(
            self.mock_batch, mock_server_span, mock_whitelist
        )
        self.observer_empty_whitelist = TaggedMetricsServerSpanObserver(
            self.mock_batch, mock_server_span, mock_whitelist_empty
        )

    def test_on_start(self):
        self.observer.on_start()
        self.assertEqual(self.mock_timer.start.call_count, 1)

    def test_on_incr_tag(self):
        self.observer.on_incr_tag("incr", delta=1)
        self.observer_empty_whitelist.on_incr_tag("incr", delta=1)

        self.assertEqual(self.mock_counter.increment.call_count, 0)

        self.assertTrue("incr" in self.observer.counters)

        self.observer.on_finish(exc_info=None)
        self.observer_empty_whitelist.on_finish(exc_info=None)

        self.assertEqual(self.mock_counter.increment.call_count, 2)

    def test_on_set_tag(self):
        self.observer.on_set_tag("test", "value")
        self.observer_empty_whitelist.on_set_tag("error", Errors.EXCEPTION)

        self.observer.on_set_tag("error", "error")
        self.assertFalse("error" in self.observer.tags)

        self.observer.on_set_tag("error", Errors.EXCEPTION)
        self.assertTrue("error" in self.observer.tags)

    def test_on_child_span_created(self):
        mock_child_span = mock.Mock()
        mock_child_span.name = "example"
        self.observer.on_child_span_created(mock_child_span)
        self.assertEqual(mock_child_span.register.call_count, 1)

    def test_on_finish(self):
        self.observer.on_start()
        self.observer_empty_whitelist.on_start()

        self.observer.on_set_tag("test", "value")
        self.observer.on_set_tag("error", Errors.EXCEPTION)
        self.observer_empty_whitelist.on_set_tag("test", "value")
        self.observer_empty_whitelist.on_set_tag("error", Errors.EXCEPTION)

        self.observer.on_finish(exc_info=None)
        self.observer_empty_whitelist.on_finish(exc_info=None)
        self.assertEqual(self.mock_timer.stop.call_count, 2)
        self.assertEqual(self.mock_batch.flush.call_count, 2)

        self.assertFalse("test" in self.observer.tags)
        self.assertEqual(self.observer.tags["error"], "internal_server_error")

        self.assertFalse("error" in self.observer_empty_whitelist.tags)
        self.assertFalse("test" in self.observer_empty_whitelist.tags)

        self.observer.on_finish(exc_info=(ServerTimeout, ServerTimeout("timeout", 3.0, False)))
        self.assertFalse(self.observer.tags["success"])
        self.assertFalse("timed_out" in self.observer.tags)


class LocalSpanObserverTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.mock_batch = mock.Mock(spec=Batch)
        self.mock_timer = mock.Mock(spec=Timer)
        self.mock_counter = mock.Mock(spec=Counter)
        self.mock_batch.timer.return_value = self.mock_timer
        self.mock_batch.counter.return_value = self.mock_counter
        mock_whitelist = ["endpoint", "success", "error", "incr"]
        mock_whitelist_empty = []

        mock_server_span = mock.Mock(spec=ServerSpan)
        mock_server_span.name = "request_name"

        self.observer = TaggedMetricsServerSpanObserver(
            self.mock_batch, mock_server_span, mock_whitelist
        )
        self.observer_empty_whitelist = TaggedMetricsServerSpanObserver(
            self.mock_batch, mock_server_span, mock_whitelist_empty
        )
        self.mock_timer = mock.Mock(spec=Timer)
        self.mock_counter = mock.Mock(spec=Counter)
        self.mock_batch = mock.Mock(spec=Batch)
        self.mock_batch.timer.return_value = self.mock_timer
        self.mock_batch.counter.return_value = self.mock_counter
        mock_whitelist = ["endpoint", "success", "error"]
        mock_whitelist_empty = []

        mock_local_span = mock.Mock(spec=LocalSpan)
        mock_local_span.name = "example"
        mock_local_span.component_name = "some_component"

        self.observer = TaggedMetricsLocalSpanObserver(
            self.mock_batch, mock_local_span, mock_whitelist
        )
        self.observer_empty_whitelist = TaggedMetricsLocalSpanObserver(
            self.mock_batch, mock_local_span, mock_whitelist_empty
        )

    def test_on_start(self):
        self.observer.on_start()
        self.assertEqual(self.mock_timer.start.call_count, 1)

    def test_on_incr_tag(self):
        self.observer.on_incr_tag("test", delta=1)
        self.assertEqual(self.mock_counter.increment.call_count, 0)
        self.assertTrue("test" in self.observer.counters)
        self.observer.on_finish(exc_info=None)
        self.assertEqual(self.mock_counter.increment.call_count, 1)

    def test_on_set_tag(self):
        self.observer.on_set_tag("test", "value")
        self.observer_empty_whitelist.on_set_tag("error", Errors.EXCEPTION)

        self.observer.on_set_tag("error", "error")
        self.assertFalse("error" in self.observer.tags)

        self.observer.on_set_tag("error", Errors.EXCEPTION)
        self.assertTrue("error" in self.observer.tags)

    def test_on_finish(self):
        self.observer.on_start()
        self.observer_empty_whitelist.on_start()

        self.observer.on_set_tag("test", "value")
        self.observer.on_set_tag("error", Errors.EXCEPTION)
        self.observer_empty_whitelist.on_set_tag("test", "value")
        self.observer_empty_whitelist.on_set_tag("error", Errors.EXCEPTION)

        self.observer.on_finish(exc_info=None)
        self.observer_empty_whitelist.on_finish(exc_info=None)
        self.assertEqual(self.mock_timer.stop.call_count, 2)
        self.assertEqual(self.mock_batch.flush.call_count, 2)

        self.assertFalse("test" in self.observer.tags)
        self.assertEqual(self.observer.tags["error"], "internal_server_error")

        self.assertFalse("error" in self.observer_empty_whitelist.tags)
        self.assertFalse("test" in self.observer_empty_whitelist.tags)

    def test_on_child_span_created(self):
        mock_child_span = mock.Mock()
        mock_child_span.name = "example"
        self.observer.on_child_span_created(mock_child_span)
        self.assertEqual(mock_child_span.register.call_count, 1)


class ClientSpanObserverTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.mock_timer = mock.Mock(spec=Timer)
        self.mock_counter = mock.Mock(spec=Counter)
        self.mock_batch = mock.Mock(spec=Batch)
        self.mock_batch.timer.return_value = self.mock_timer
        self.mock_batch.counter.return_value = self.mock_counter
        mock_whitelist = ["endpoint", "success", "error"]
        mock_whitelist_empty = []

        mock_client_span = mock.Mock(spec=Span)
        mock_client_span.name = "client.endpoint"

        self.observer = TaggedMetricsClientSpanObserver(
            self.mock_batch, mock_client_span, mock_whitelist
        )
        self.observer_empty_whitelist = TaggedMetricsClientSpanObserver(
            self.mock_batch, mock_client_span, mock_whitelist_empty
        )

    def test_on_start(self):
        self.observer.on_start()
        self.assertEqual(self.mock_timer.start.call_count, 1)
        self.assertEqual(self.observer.tags["client"], "client")
        self.assertEqual(self.observer.tags["endpoint"], "endpoint")

    def test_incr_tag(self):
        self.observer.on_incr_tag("test", delta=1)
        self.mock_counter.increment.assert_not_called()
        self.assertTrue("test" in self.observer.counters)
        self.observer.on_finish(exc_info=None)
        self.mock_counter.increment.assert_called()

    def test_on_set_tag(self):
        self.observer.on_set_tag("test", "value")
        self.observer_empty_whitelist.on_set_tag("error", Errors.EXCEPTION)

        self.observer.on_set_tag("error", "error")
        self.assertFalse("error" in self.observer.tags)

        self.observer.on_set_tag("error", Errors.EXCEPTION)
        self.assertTrue("error" in self.observer.tags)

    def test_on_log(self):
        self.observer.on_log("any", {})
        self.assertFalse("error" in self.observer.tags)
        self.observer.on_log("error.object", {})
        self.assertEqual(self.observer.tags["error"], "internal_server_error")

    def test_on_finish(self):
        self.observer.on_start()
        self.observer_empty_whitelist.on_start()

        self.observer.on_set_tag("test", "value")
        self.observer.on_set_tag("error", Errors.EXCEPTION)
        self.observer_empty_whitelist.on_set_tag("test", "value")
        self.observer_empty_whitelist.on_set_tag("error", Errors.EXCEPTION)

        self.observer.on_finish(exc_info=None)
        self.observer_empty_whitelist.on_finish(exc_info=None)
        self.assertEqual(self.mock_timer.stop.call_count, 2)
        self.assertEqual(self.mock_batch.flush.call_count, 2)

        self.assertFalse("test" in self.observer.tags)
        self.assertEqual(self.observer.tags["error"], "internal_server_error")

        self.assertFalse("error" in self.observer_empty_whitelist.tags)
        self.assertFalse("test" in self.observer_empty_whitelist.tags)

        self.observer.on_finish(exc_info=(ServerTimeout, ServerTimeout("timeout", 3.0, False)))
        self.assertFalse(self.observer.tags["success"])
        self.assertFalse("timed_out" in self.observer.tags)
