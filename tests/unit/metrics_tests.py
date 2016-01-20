# coding=utf8

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import socket
import unittest

from baseplate import metrics, config

from .. import mock


EXAMPLE_ENDPOINT = config.EndpointConfiguration(
    socket.AF_INET, ("127.0.0.1", 1234))


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


class NullTransportTests(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_nothing_sent(self, mock_make_socket):
        transport = metrics.NullTransport()
        transport.send(b"metric")
        self.assertEqual(mock_make_socket.call_count, 0)


class RawTransportTests(unittest.TestCase):
    @mock.patch("socket.socket")
    def test_sent_immediately(self, mock_make_socket):
        mocket = mock_make_socket.return_value
        transport = metrics.RawTransport(EXAMPLE_ENDPOINT)

        self.assertEqual(mocket.connect.call_args,
            mock.call(("127.0.0.1", 1234)))

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
        self.assertEqual(mocket.sendall.call_args,
            mock.call(b"a\nb\nc"))


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


class ClientTests(unittest.TestCase):
    def test_make_batch(self):
        transport = mock.Mock(spec=metrics.NullTransport)
        client = metrics.Client(transport, "namespace")
        batch = client.batch()

        self.assertIsInstance(batch, metrics.Batch)
        self.assertEqual(batch.namespace, b"namespace")


class BatchTests(unittest.TestCase):
    @mock.patch("baseplate.metrics.BufferedTransport", autospec=True)
    def test_context(self, MockBufferedTransport):
        mock_buffer = MockBufferedTransport.return_value
        mock_transport = mock.Mock(spec=metrics.NullTransport)

        batch = metrics.Batch(mock_transport, "namespace")
        with batch as b:
            self.assertEqual(b, batch)
        self.assertTrue(mock_buffer.flush.called)


class TimerTests(unittest.TestCase):
    def setUp(self):
        self.transport = mock.Mock(spec=metrics.NullTransport)

    @mock.patch("time.time", autospec=True)
    def test_basic_operation(self, mock_time):
        timer = metrics.Timer(self.transport, b"example")

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
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:4000|ms"))

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
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:3000|ms"))


class CounterTests(unittest.TestCase):
    def setUp(self):
        self.transport = mock.Mock(spec=metrics.NullTransport)

    def test_incr(self):
        counter = metrics.Counter(self.transport, b"example")

        counter.increment()
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:1|c"))

        counter.increment(delta=10)
        self.assertEqual(self.transport.send.call_count, 2)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:10|c"))

        counter.increment(delta=-20)
        self.assertEqual(self.transport.send.call_count, 3)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:-20|c"))

        counter.increment(delta=2, sample_rate=.5)
        self.assertEqual(self.transport.send.call_count, 4)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:2|c|@0.5"))

    def test_decr(self):
        counter = metrics.Counter(self.transport, b"example")

        counter.decrement()
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:-1|c"))

        counter.decrement(delta=3)
        self.assertEqual(self.transport.send.call_count, 2)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:-3|c"))


class GaugeTests(unittest.TestCase):
    def setUp(self):
        self.transport = mock.Mock(spec=metrics.NullTransport)

    def test_replace(self):
        gauge = metrics.Gauge(self.transport, b"example")
        gauge.replace(33)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:33|g"))

    def test_replace_disallow_negative(self):
        gauge = metrics.Gauge(self.transport, b"example")
        with self.assertRaises(Exception):
            gauge.replace(-2)

    def test_increment(self):
        gauge = metrics.Gauge(self.transport, b"example")
        gauge.increment(33)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:+33|g"))

    def test_decrement(self):
        gauge = metrics.Gauge(self.transport, b"example")
        gauge.decrement(33)
        self.assertEqual(self.transport.send.call_count, 1)
        self.assertEqual(self.transport.send.call_args,
            mock.call(b"example:-33|g"))


class MakeClientTests(unittest.TestCase):
    def test_no_endpoint(self):
        client = metrics.make_client("namespace", None)
        self.assertIsInstance(client.transport, metrics.NullTransport)

    def test_valid_endpoint(self):
        client = metrics.make_client("namespace", EXAMPLE_ENDPOINT)
        self.assertIsInstance(client.transport, metrics.RawTransport)
