import unittest

from unittest import mock

import requests

from baseplate.lib import metrics
from baseplate.sidecars import SerializedBatch
from baseplate.sidecars import trace_publisher


class ZipkinPublisherTest(unittest.TestCase):
    @mock.patch("baseplate.clients.requests.BaseplateSession", autospec=True)
    @mock.patch("baseplate.RequestContext", autospec=True)
    @mock.patch("baseplate.Baseplate", autospec=True)
    def setUp(self, bp, context, session):
        self.metrics_client = mock.MagicMock(autospec=metrics.Client)
        self.zipkin_api_url = "http://test.local/api/v2"

        bp.server_context.return_value.__enter__.return_value = context
        context.http_client = session
        self.baseplate = bp
        self.session = session

        self.publisher = trace_publisher.ZipkinPublisher(
            bp, self.zipkin_api_url, self.metrics_client
        )

    def test_initialization(self):
        self.assertEqual(self.publisher.endpoint, f"{self.zipkin_api_url}/spans")

    def test_empty_batch(self):
        self.publisher.publish(SerializedBatch(item_count=0, serialized=b""))
        self.assertEqual(self.session.post.call_count, 0)

    def test_publish_retry(self):
        # raise two errors and then return a mock response
        self.session.post.side_effect = [requests.HTTPError(504), OSError, mock.Mock()]
        spans = b"[]"
        self.publisher.publish(SerializedBatch(item_count=1, serialized=spans))
        self.assertEqual(self.session.post.call_count, 3)
