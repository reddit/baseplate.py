from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

import requests

from baseplate import config, metrics
from baseplate._utils import SerializedBatch
from baseplate.diagnostics.tracing import publisher

from .... import mock


class ZipkinPublisherTest(unittest.TestCase):
    @mock.patch("requests.Session", autospec=True)
    def setUp(self, mock_Session):
        self.session = mock_Session.return_value
        self.metrics_client = mock.MagicMock(autospec=metrics.Client)
        self.zipkin_api_url = "http://test.local/api/v2"
        self.publisher = publisher.ZipkinPublisher(
            self.zipkin_api_url,
            self.metrics_client,
        )

    def test_initialization(self):
        self.assertEqual(self.publisher.endpoint, "{}/spans".format(self.zipkin_api_url))
        self.publisher.session.mount.assert_called_with("http://", mock.ANY)

    def test_empty_batch(self):
        self.publisher.publish(SerializedBatch(count=0, bytes=b""))
        self.assertEqual(self.session.post.call_count, 0)

    def test_publish_retry(self):
        # raise two errors and then return a mock response
        self.session.post.side_effect = [requests.HTTPError(504), IOError, mock.Mock()]
        spans = b"[]"
        self.publisher.publish(SerializedBatch(count=1, bytes=spans))
        self.assertEqual(self.session.post.call_count, 3)

    def test_client_error(self):
        self.session.post.side_effect = [
            requests.HTTPError(400, response=mock.Mock(status_code=400))]
        spans = b"[]"

        with self.assertRaises(requests.HTTPError):
            self.publisher.publish(SerializedBatch(count=1, bytes=spans))

        self.assertEqual(self.session.post.call_count, 1)
