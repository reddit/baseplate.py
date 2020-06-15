import contextlib
import time
import unittest

import posix_ipc

from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import TimedOutError


class TestMessageQueueCreation(unittest.TestCase):
    qname = "/baseplate-test-queue"

    def setUp(self):
        try:
            queue = posix_ipc.MessageQueue(self.qname)
        except posix_ipc.ExistentialError:
            pass
        else:
            queue.unlink()
            queue.close()

    def test_create_queue(self):
        message_queue = MessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            self.assertEqual(mq.queue.max_messages, 1)
            self.assertEqual(mq.queue.max_message_size, 1)

    def test_put_get(self):
        message_queue = MessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_get_timeout(self):
        message_queue = MessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.get(timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=2)

    def test_put_timeout(self):
        message_queue = MessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.put(b"x", timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=1)

    def test_put_zero_timeout(self):
        message_queue = MessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x", timeout=0)
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_put_full_zero_timeout(self):
        message_queue = MessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"1", timeout=0)

            with self.assertRaises(TimedOutError):
                mq.put(b"2", timeout=0)

    def tearDown(self):
        try:
            queue = posix_ipc.MessageQueue(self.qname)
        except posix_ipc.ExistentialError:
            pass
        else:
            queue.unlink()
            queue.close()
