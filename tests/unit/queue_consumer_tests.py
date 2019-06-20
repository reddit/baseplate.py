from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
import unittest

try:
    import kombu
except ImportError:
    raise unittest.SkipTest("kombu is not installed")
else:
    del kombu

from baseplate import queue_consumer

from .. import mock


class BaseKombuConsumerTests(unittest.TestCase):
    def test_get_message(self):
        message = mock.Mock()
        worker = mock.Mock()
        worker.get_message.side_effect = [message]
        worker_thread = mock.Mock()

        consumer = queue_consumer.BaseKombuConsumer(worker, worker_thread)

        ret = consumer.get_message()

        self.assertEqual(ret, message)
        worker.get_message.assert_called_once_with(block=True, timeout=None)

    @mock.patch('baseplate.queue_consumer.RetryPolicy')
    def test_get_batch(self, RetryPolicy):
        m1 = mock.Mock()
        m2 = mock.Mock()
        m3 = mock.Mock()
        worker = mock.Mock()
        worker.get_message.side_effect = [m1, m2, m3, None]
        worker_thread = mock.Mock()

        RetryPolicy.new.return_value = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]

        consumer = queue_consumer.BaseKombuConsumer(worker, worker_thread)

        ret = consumer.get_batch(max_items=10, timeout=10)

        self.assertEqual(ret, [m1, m2, m3])
        RetryPolicy.new.assert_called_once_with(attempts=10, budget=10)
        worker.get_message.assert_has_calls([
            mock.call(block=True, timeout=10),
            mock.call(block=True, timeout=9),
            mock.call(block=True, timeout=8),
            mock.call(block=True, timeout=7),
        ])
        self.assertEqual(worker.get_message.call_count, 4)

    # Mock out threading.Thread so we don't actually start up phantom worker threads.
    @mock.patch("baseplate.queue_consumer.Thread")
    def test_queue_size(self, _):
        consumer = queue_consumer.BaseKombuConsumer.new(mock.Mock(), mock.Mock(), queue_size=10)
        self.assertEqual(consumer.worker.work_queue.maxsize, 10)

    # Mock out threading.Thread so we don't actually start up phantom worker threads.
    @mock.patch("baseplate.queue_consumer.Thread")
    def test_default_queue_size_gt_zero(self, _):
        # We don't really care to test the exact default queue size, just that
        # it is greater than zero (which is infinite/unbounded).
        consumer = queue_consumer.BaseKombuConsumer.new(mock.Mock(), mock.Mock())
        self.assertGreater(consumer.worker.work_queue.maxsize, 0)
