import queue
import unittest

from unittest import mock

try:
    import kombu
except ImportError:
    raise unittest.SkipTest("kombu is not installed")
else:
    del kombu

from baseplate.frameworks.queue_consumer.deprecated import BaseKombuConsumer


class BaseKombuConsumerTests(unittest.TestCase):
    def test_get_message(self):
        message = mock.Mock()
        worker = mock.Mock()
        work_queue = mock.Mock(spec=queue.Queue)
        work_queue.get.return_value = message
        worker_thread = mock.Mock()

        consumer = BaseKombuConsumer(worker, worker_thread, work_queue)

        ret = consumer.get_message()

        self.assertEqual(ret, message)
        work_queue.get.assert_called_once_with(block=True, timeout=None)

    @mock.patch("baseplate.frameworks.queue_consumer.deprecated.RetryPolicy")
    def test_get_batch(self, RetryPolicy):
        m1 = mock.Mock()
        m2 = mock.Mock()
        m3 = mock.Mock()
        worker = mock.Mock()
        work_queue = mock.Mock(spec=queue.Queue)
        work_queue.get.side_effect = [m1, m2, m3, None]
        worker_thread = mock.Mock()

        RetryPolicy.new.return_value = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]

        consumer = BaseKombuConsumer(worker, worker_thread, work_queue)

        ret = consumer.get_batch(max_items=10, timeout=10)

        self.assertEqual(ret, [m1, m2, m3])
        RetryPolicy.new.assert_called_once_with(attempts=10, budget=10)
        work_queue.get.assert_has_calls(
            [
                mock.call(block=True, timeout=10),
                mock.call(block=True, timeout=9),
                mock.call(block=True, timeout=8),
                mock.call(block=True, timeout=7),
            ]
        )
        self.assertEqual(work_queue.get.call_count, 4)

    # Mock out threading.Thread so we don't actually start up phantom worker threads.
    @mock.patch("baseplate.frameworks.queue_consumer.deprecated.Thread")
    def test_queue_size(self, _):
        consumer = BaseKombuConsumer.new(mock.Mock(), mock.Mock(), queue_size=10)
        self.assertEqual(consumer.worker.work_queue.maxsize, 10)

    # Mock out threading.Thread so we don't actually start up phantom worker threads.
    @mock.patch("baseplate.frameworks.queue_consumer.deprecated.Thread")
    def test_default_queue_size_gt_zero(self, _):
        # We don't really care to test the exact default queue size, just that
        # it is greater than zero (which is infinite/unbounded).
        consumer = BaseKombuConsumer.new(mock.Mock(), mock.Mock())
        self.assertGreater(consumer.worker.work_queue.maxsize, 0)
