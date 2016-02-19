from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import gzip
import unittest

import requests

from baseplate import config, metrics
from baseplate._compat import BytesIO
from baseplate.events import publisher

from ... import mock


class BatcherTests(unittest.TestCase):
    def setUp(self):
        self.consumer = mock.Mock()
        self.consumer.batch_size_overhead = 0
        self.consumer.get_item_size = lambda item: len(item)
        self.consumer.batch_size_limit = 5
        self.batcher = publisher.Batcher(self.consumer)

    def test_flush_empty_does_nothing(self):
        self.batcher.flush()
        self.assertEqual(self.consumer.consume_batch.called, False)

    def test_start_time(self):
        with mock.patch("time.time") as mock_time:
            self.assertEqual(self.batcher.batch_age, 0)

            mock_time.return_value = 33
            self.batcher.add("a")
            self.assertEqual(self.batcher.batch_start, 33)

            mock_time.return_value = 34
            self.batcher.add("b")
            self.assertEqual(self.batcher.batch_start, 33)

            mock_time.return_value = 35
            self.assertEqual(self.batcher.batch_age, 2)

        self.batcher.flush()
        self.assertEqual(self.batcher.batch_start, None)

    def test_manual_flush(self):
        self.batcher.add("a")
        self.batcher.add("b")
        self.batcher.flush()
        self.consumer.consume_batch.assert_called_once_with(["a", "b"])

    def test_flush_when_full(self):
        for i in range(self.consumer.batch_size_limit+1):
            self.batcher.add(str(i))
        self.consumer.consume_batch.assert_called_once_with(
            ["0", "1", "2", "3", "4"])


class CompressTests(unittest.TestCase):
    def test_compress(self):
        raw = b"test"
        compressed = publisher.gzip_compress(raw)
        decompressed = gzip.GzipFile(fileobj=BytesIO(compressed)).read()
        self.assertEqual(raw, decompressed)


class ConsumerTests(unittest.TestCase):
    @mock.patch("requests.Session", autospec=True)
    def setUp(self, Session):
        self.config = config.ConfigNamespace()
        self.config.collector = config.ConfigNamespace()
        self.config.collector.hostname = "test.local"
        self.config.key = config.ConfigNamespace()
        self.config.key.name = "TestKey"
        self.config.key.secret = b"hunter2"

        self.session = Session.return_value

        self.metrics_client = mock.MagicMock(autospec=metrics.Client)

        self.consumer = publisher.BatchConsumer(
            self.metrics_client, self.config)

    def test_item_size(self):
        size = self.consumer.get_item_size(b"{}")
        self.assertEqual(size, 3)

    def test_publish_success(self):
        events = [b'{"example": "value"}']

        self.consumer.consume_batch(events)

        self.assertEqual(self.session.post.call_count, 1)

        args, kwargs = self.session.post.call_args
        headers = kwargs.get("headers", {})
        self.assertEqual(args[0], "https://test.local/v1")
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(headers["X-Signature"], "key=TestKey, mac=7c46d56b99cd4cb05e08238c1d4c10a2f330795e9d7327f17cc66fd206bf1179")

    @mock.patch("time.sleep")
    def test_publish_retry(self, mock_sleep):
        self.session.post.side_effect = [requests.HTTPError(504), IOError, mock.Mock()]
        events = [b'{"example": "value"}']

        self.consumer.consume_batch(events)

        self.assertEqual(mock_sleep.call_count, 2)
        self.assertEqual(self.session.post.call_count, 3)
