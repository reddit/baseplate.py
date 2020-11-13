import unittest

from unittest import mock

import requests

from baseplate.lib import metrics
from baseplate.sidecars import SerializedBatch
from baseplate.sidecars import trace_publisher


class ZipkinPublisherTest(unittest.TestCase):
    @mock.patch("requests.Session", autospec=True)
    def setUp(self, mock_Session):
        self.session = mock_Session.return_value
        self.session.headers = {}
        self.metrics_client = mock.MagicMock(autospec=metrics.Client)
        self.zipkin_api_url = "http://test.local/api/v2"
        self.publisher = trace_publisher.ZipkinPublisher(self.zipkin_api_url, self.metrics_client)

    def test_initialization(self):
        self.assertEqual(self.publisher.endpoint, f"{self.zipkin_api_url}/spans")
        self.publisher.session.mount.assert_called_with("http://", mock.ANY)

    def test_empty_batch(self):
        self.publisher.publish(SerializedBatch(item_count=0, serialized=b""))
        self.assertEqual(self.session.post.call_count, 0)

    def test_publish_retry(self):
        # raise two errors and then return a mock response
        self.session.post.side_effect = [requests.HTTPError(504), OSError, mock.Mock()]
        spans = b"[]"
        self.publisher.publish(SerializedBatch(item_count=1, serialized=spans))
        self.assertEqual(self.session.post.call_count, 3)

    def test_client_error(self):
        self.session.post.side_effect = [
            requests.HTTPError(400, response=mock.Mock(status_code=400))
        ]
        spans = b"[]"

        with self.assertRaises(requests.HTTPError):
            self.publisher.publish(SerializedBatch(item_count=1, serialized=spans))

        self.assertEqual(self.session.post.call_count, 1)
