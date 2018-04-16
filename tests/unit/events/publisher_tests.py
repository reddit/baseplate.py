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
from baseplate._utils import SerializedBatch

from ... import mock


class TimeLimitedBatchTests(unittest.TestCase):
    def setUp(self):
        self.inner = mock.Mock(autospec=publisher.Batch)
        self.batch = publisher.TimeLimitedBatch(self.inner, max_age=1)

    def test_serialize(self):
        result = self.batch.serialize()
        self.assertEqual(result, self.inner.serialize.return_value)

    @mock.patch("time.time")
    def test_add_after_time_up_and_reset(self, mock_time):
        mock_time.return_value = 1

        self.batch.add("a")
        self.assertEqual(self.inner.add.call_count, 1)

        with self.assertRaises(publisher.BatchFull):
            mock_time.return_value = 3
            self.batch.add("b")
        self.assertEqual(self.inner.add.call_count, 1)

        self.batch.reset()
        self.batch.add("b")
        self.assertEqual(self.inner.add.call_count, 2)
        self.assertEqual(self.inner.reset.call_count, 1)


class BatchTests(unittest.TestCase):
    def test_v1(self):
        batch = publisher.V1Batch(max_size=10)
        batch.add(None)
        batch.add(b"1")
        batch.add(b"2")

        result = batch.serialize()
        self.assertEqual(result.count, 2)
        self.assertEqual(result.bytes, b'[1,2]')

        with self.assertRaises(publisher.BatchFull):
            batch.add(b"x" * 100)

        batch.reset()
        result = batch.serialize()
        self.assertEqual(result.count, 0)

    def test_v2(self):
        batch = publisher.V2Batch(max_size=50)
        batch.add(None)
        batch.add(b"a")
        batch.add(b"b")

        result = batch.serialize()
        self.assertEqual(result.count, 2)
        self.assertEqual(result.bytes, b'{"1":{"lst":["rec",2,a,b]}}')

        with self.assertRaises(publisher.BatchFull):
            batch.add(b"x" * 100)

        batch.reset()
        result = batch.serialize()
        self.assertEqual(result.count, 0)


class CompressTests(unittest.TestCase):
    def test_compress(self):
        raw = b"test"
        compressed = publisher.gzip_compress(raw)
        decompressed = gzip.GzipFile(fileobj=BytesIO(compressed)).read()
        self.assertEqual(raw, decompressed)


class PublisherTests(unittest.TestCase):
    @mock.patch("requests.Session", autospec=True)
    def setUp(self, Session):
        self.config = config.ConfigNamespace()
        self.config.collector = config.ConfigNamespace()
        self.config.collector.hostname = "test.local"
        self.config.collector.version = 1
        self.config.key = config.ConfigNamespace()
        self.config.key.name = "TestKey"
        self.config.key.secret = b"hunter2"

        self.session = Session.return_value

        self.metrics_client = mock.MagicMock(autospec=metrics.Client)

        self.publisher = publisher.BatchPublisher(
            self.metrics_client, self.config)

    def test_empty_batch(self):
        self.publisher.publish(SerializedBatch(count=0, bytes=b''))
        self.assertEqual(self.session.post.call_count, 0)

    def test_publish_success(self):
        events = b'[{"example": "value"}]'

        self.publisher.publish(SerializedBatch(count=1, bytes=events))

        self.assertEqual(self.session.post.call_count, 1)

        args, kwargs = self.session.post.call_args
        headers = kwargs.get("headers", {})
        self.assertEqual(args[0], "https://test.local/v1")
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(
            headers["X-Signature"],
            "key=TestKey, mac=7c46d56b99cd4cb05e08238c1d4c10a2f330795e9d7327f17cc66fd206bf1179")

    @mock.patch("time.sleep")
    def test_publish_retry(self, mock_sleep):
        self.session.post.side_effect = [requests.HTTPError(504), IOError, mock.Mock()]
        events = b'[{"example": "value"}]'

        self.publisher.publish(SerializedBatch(count=1, bytes=events))

        self.assertEqual(mock_sleep.call_count, 2)
        self.assertEqual(self.session.post.call_count, 3)

    @mock.patch("time.sleep")
    def test_fail_on_client_error(self, mock_sleep):
        self.session.post.side_effect = [
            requests.HTTPError(400, response=mock.Mock(status_code=400))]
        events = b'[{"example": "value"}]'

        with self.assertRaises(requests.HTTPError):
            self.publisher.publish(SerializedBatch(count=1, bytes=events))

        self.assertEqual(mock_sleep.call_count, 0)
        self.assertEqual(self.session.post.call_count, 1)
