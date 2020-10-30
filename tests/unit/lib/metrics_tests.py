# coding=utf8
import socket
import unittest

from unittest import mock

from baseplate.lib import config
from baseplate.lib import metrics


EXAMPLE_ENDPOINT = config.EndpointConfiguration(socket.AF_INET, ("127.0.0.1", 1234))


class MetricJoinTests(unittest.TestCase):
    def test_single_node(self):
        joined = metrics._metric_join(b"single")
        self.assertEqual(joined, b"single")

    def test_two_nodes(self):
        joined = metrics._metric_join(b"first", b"second")
        self.assertEqual(joined, b"first.second")

    def test_subpaths(self):
        joined = metrics._metric_join(b"first.second", b"third.fourth")
        self.assertEqual(joined, b"first.second.third.fourth")

    def test_subpaths_with_trailing_dot(self):
        joined = metrics._metric_join(b"first.", b"second")
        self.assertEqual(joined, b"first.second")

    def test_join_null_nodes(self):
        joined = metrics._metric_join(b"first.", None, b"second", None)
        self.assertEqual(joined, b"first.second")


class FormatTagsTest(unittest.TestCase):
    def test_no_tags(self):
        formatted_tags = metrics._format_tags(None)
        self.assertIsNone(formatted_tags)

    def test_tags(self):
        tags = {"success": True, "error": False}
        formatted_tags = metrics._format_tags(tags)
        self.assertEqual(formatted_tags, b",success=True,error=False")


class NullTransportTests(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_nothing_sent(self, mock_make_socket):
        transport = metrics.NullTransport(log_if_unconfigured=False)
        transport.send(b"metric")
        self.assertEqual(mock_make_socket.call_count, 0)


class RawTransportTests(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_sent_immediately(self, mock_make_socket):
        mocket = mock_make_socket.return_value
        transport = metrics.RawTransport(EXAMPLE_ENDPOINT)

        self.assertEqual(mocket.connect.call_args, mock.call(("127.0.0.1", 1234)))

        transport.send(b"metric")

        self.assertEqual(mocket.sendall.call_count, 1)
        self.assertEqual(mocket.sendall.call_args, mock.call(b"metric"))


class BufferedTransportTests(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_buffered(self, mock_make_socket):
        mocket = mock_make_socket.return_value
        raw_transport = metrics.RawTransport(EXAMPLE_ENDPOINT)
        transport = metrics.BufferedTransport(raw_transport)
        transport.send(b"a")
        transport.send(b"b")
        transport.send(b"c")
        self.assertEqual(mocket.sendall.call_count, 0)
        transport.flush()

        self.assertEqual(mocket.sendall.call_count, 1)
        self.assertEqual(mocket.sendall.call_args, mock.call(b"a\nb\nc"))

    def test_buffered_exception_is_caught(self):
        raw_transport = metrics.RawTransport(EXAMPLE_ENDPOINT)
        transport = metrics.BufferedTransport(raw_transport)
        transport.send(b"x" * 65536)

        with self.assertRaises(metrics.MessageTooBigTransportError):
            transport.flush()


class BaseClientTests(unittest.TestCase):
    def test_encode_namespace(self):
        transport = mock.Mock(spec=metrics.NullTransport)

        client = metrics.BaseClient(transport, "name")
        self.assertEqual(client.namespace, b"name")

        with self.assertRaises(UnicodeEncodeError):
            metrics.BaseClient(transport, "☃")


class BaseClientFactoriesTests(unittest.TestCase):
    def setUp(self):
        transport = mock.Mock(spec=metrics.NullTransport)
        self.client = metrics.BaseClient(transport, "namespace")

    def test_make_timer(self):
        timer = self.client.timer("some_timer")
        self.assertIsInstance(timer, metrics.Timer)
        self.assertEqual(timer.name, b"namespace.some_timer")

        with self.assertRaises(UnicodeEncodeError):
            self.client.timer("☃")

    def test_make_counter(self):
        counter = self.client.counter("some_counter")
        self.assertIsInstance(counter, metrics.Counter)
        self.assertEqual(counter.name, b"namespace.some_counter")

        with self.assertRaises(UnicodeEncodeError):
            self.client.counter("☃")

    def test_make_gauge(self):
        gauge = self.client.gauge("some_gauge")
        self.assertIsInstance(gauge, metrics.Gauge)
        self.assertEqual(gauge.name, b"namespace.some_gauge")

        with self.assertRaises(UnicodeEncodeError):
            self.client.gauge("☃")

    def test_make_histogram(self):
        histogram = self.client.histogram("some_histogram")
        self.assertIsInstance(histogram, metrics.Histogram)
        self.assertEqual(histogram.name, b"namespace.some_histogram")

        with self.assertRaises(UnicodeEncodeError):
            self.client.gauge("☃")


class ClientTests(unittest.TestCase):
    def test_make_batch(self):
        transport = mock.Mock(spec=metrics.NullTransport)
        client = metrics.Client(transport, "namespace")
        batch = client.batch()

        self.assertIsInstance(batch, metrics.Batch)
        self.assertEqual(batch.namespace, b"namespace")


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.patcher = mock.patch("baseplate.lib.metrics.BufferedTransport", autospec=True)
        self.mock_buffer = self.patcher.start().return_value
        self.mock_transport = mock.Mock(spec=metrics.NullTransport)

        # encode is called here since metrics.Batch is designed to be instantiated
        # by an instance of metrics.Client which encodes the namespace arg
        self.batch = metrics.Batch(self.mock_transport, b"namespace")

    def test_context(self):
        with self.batch as b:
            self.assertEqual(b, self.batch)
        self.assertTrue(self.mock_buffer.flush.called)

    def test_make_counter(self):
        batch_counter = self.batch.counter("some_counter")
        self.assertIsInstance(batch_counter, metrics.BatchCounter)
        expected_counter_name = b"namespace.some_counter"
        self.assertEqual(batch_counter.name, expected_counter_name)
        self.assertEqual(len(self.batch.counters), 1)
        self.assertTrue(expected_counter_name in self.batch.counters)

    def test_get_counter_twice(self):
        counter_name = "some_counter"
        batch_counter = self.batch.counter(counter_name)
        self.assertIsInstance(batch_counter, metrics.BatchCounter)
        expected_counter_name = b"namespace.some_counter"
        self.assertEqual(batch_counter.name, expected_counter_name)

        refetched_batch_counter = self.batch.counter(counter_name)
        self.assertEqual(len(self.batch.counters), 1)
        self.assertTrue(expected_counter_name in self.batch.counters)
        self.assertEqual(refetched_batch_counter, batch_counter)

    @mock.patch("baseplate.lib.metrics.BatchCounter", autospec=True)
    def test_counter_flush(self, MockBatchCounter):
        with self.batch as b:
            batch_counter = b.counter("some_counter")
            batch_counter.increment()
        self.assertTrue(batch_counter.flush.called)

    def test_counters_cleared_after_flush(self):
        self.batch.counter("some_counter").increment()
        self.batch.flush()
        self.mock_buffer.send.assert_called()
        self.mock_buffer.reset_mock()
        self.batch.flush()
        self.mock_buffer.send.assert_not_called()

    def tearDown(self):
        self.patcher.stop()


class TimerTests(unittest.TestCase):
    def setUp(self):
        self.transport = mock.Mock(spec=metrics.NullTransport)

    def test_init_with_tags(self):
        tags = {"test": "true"}
        timer = metrics.Timer(self.transport, b"example", tags)
        self.assertEqual(timer.tags, tags)

    @mock.patch("time.time", autospec=True)
    def test_basic_operation(self, mock_time):
        timer = metrics.Timer(self.transport, b"example")
        self.assertEqual(timer.tags, {})

        with self.assertRaises(Exception):
            timer.stop()

        mock_time.return_value = 1000
        timer.start()
        with self.assertRaises(Exception):
            timer.start()
        self.assertEqual(self.transport.send.call_count, 0)

        mock_time.return_value = 1004
        timer.stop()
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:4000|ms"))

        with self.assertRaises(Exception):
            timer.start()
        with self.assertRaises(Exception):
            timer.stop()

    @mock.patch("time.time", autospec=True)
    def test_context_manager(self, mock_time):
        timer = metrics.Timer(self.transport, b"example")

        mock_time.return_value = 1000
        with timer:
            mock_time.return_value = 1003

        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:3000|ms"))

    def test_send(self):
        timer = metrics.Timer(self.transport, b"example")
        timer.send(3.14)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:3140|ms"))

    def test_send_tagged(self):
        tags = {"test": "true"}
        timer = metrics.Timer(self.transport, b"example", tags)
        timer.send(3.14)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example,test=true:3140|ms"))

    def test_update_tags(self):
        tags = {"test": "true"}
        timer = metrics.Timer(self.transport, b"example", tags)
        new_tags = {"test2": "false"}
        timer.update_tags(new_tags)
        self.assertNotEqual(timer.tags, new_tags)


class CounterTests(unittest.TestCase):
    def setUp(self):
        self.transport = mock.Mock(spec=metrics.NullTransport)

    def test_incr(self):
        counter = metrics.Counter(self.transport, b"example")
        self.assertIsNone(counter.tags)

        counter.increment()
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:1|c"))

        counter.increment(delta=10)
        self.assertEqual(self.transport.send.call_count, 2)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:10|c"))

        counter.increment(delta=-20)
        self.assertEqual(self.transport.send.call_count, 3)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:-20|c"))

        counter.increment(delta=2, sample_rate=0.5)
        self.assertEqual(self.transport.send.call_count, 4)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:2|c|@0.5"))

    def test_decr(self):
        counter = metrics.Counter(self.transport, b"example")

        counter.decrement()
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:-1|c"))

        counter.decrement(delta=3)
        self.assertEqual(self.transport.send.call_count, 2)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:-3|c"))

    def test_send_tagged(self):
        tags = {"test": "true"}
        counter = metrics.Counter(self.transport, b"example", tags)
        counter.send(delta=2, sample_rate=0.5)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example,test=true:2|c|@0.5"))


class BatchCounterTests(unittest.TestCase):
    def setUp(self):
        self.transport = mock.Mock(spec=metrics.NullTransport)

    def test_increment(self):
        batch_counter = metrics.BatchCounter(self.transport, b"example")
        batch_counter.increment()
        self.assertEqual(len(batch_counter.packets), 1)
        self.assertEqual(batch_counter.packets[1.0], 1)
        self.assertFalse(self.transport.send.called)

    def test_multiple_increments(self):
        batch_counter = metrics.BatchCounter(self.transport, b"example")
        batch_counter.increment()
        self.assertFalse(self.transport.send.called)
        batch_counter.increment()
        self.assertEqual(batch_counter.packets[1.0], 2)
        self.assertFalse(self.transport.send.called)

    def test_flush(self):
        batch_counter = metrics.BatchCounter(self.transport, b"example")
        batch_counter.increment()
        batch_counter.increment(3)
        self.assertEqual(batch_counter.packets[1.0], 4)
        batch_counter.flush()
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:4|c"))

    def test_multiple_sample_rates(self):
        batch_counter = metrics.BatchCounter(self.transport, b"example")
        batch_counter.increment()
        batch_counter.increment()
        batch_counter.increment(sample_rate=0.5)
        batch_counter.flush()

        self.assertEqual(len(batch_counter.packets), 2)
        self.assertEqual(batch_counter.packets[1.0], 2)
        self.assertEqual(batch_counter.packets[0.5], 1)
        self.assertEqual(self.transport.send.call_count, 2)
        expected_call_args = [mock.call(b"example:2|c"), mock.call(b"example:1|c|@0.5")]
        for expected in expected_call_args:
            self.assertTrue(expected in self.transport.send.call_args_list)


class GaugeTests(unittest.TestCase):
    def setUp(self):
        self.transport = mock.Mock(spec=metrics.NullTransport)

    def test_replace(self):
        gauge = metrics.Gauge(self.transport, b"example")
        gauge.replace(33)
        self.assertIsNone(gauge.tags)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example:33|g"))

    def test_replace_disallow_negative(self):
        gauge = metrics.Gauge(self.transport, b"example")
        with self.assertRaises(Exception):
            gauge.replace(-2)

    def test_replace_tagged(self):
        tags = {"test": "true"}
        gauge = metrics.Gauge(self.transport, b"example", tags)
        gauge.replace(33)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example,test=true:33|g"))


class HistogramTests(unittest.TestCase):
    def setUp(self):
        self.transport = mock.Mock(spec=metrics.NullTransport)

    def test_log(self):
        histogram = metrics.Histogram(self.transport, b"example_hist")
        histogram.add_sample(33)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example_hist:33|h"))

    def test_log_tagged(self):
        tags = {"test": "true"}
        histogram = metrics.Histogram(self.transport, b"example_hist", tags)
        self.assertEqual(histogram.tags, tags)
        histogram.add_sample(33)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args, mock.call(b"example_hist,test=true:33|h"))


class MakeClientTests(unittest.TestCase):
    def test_no_endpoint(self):
        client = metrics.make_client("namespace", None, log_if_unconfigured=False)
        self.assertIsInstance(client.transport, metrics.NullTransport)

    def test_valid_endpoint(self):
        client = metrics.make_client("namespace", EXAMPLE_ENDPOINT, log_if_unconfigured=False)
        self.assertIsInstance(client.transport, metrics.RawTransport)
