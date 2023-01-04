import contextlib
import logging
import time
import unittest

from importlib import reload

import gevent
import posix_ipc
import pytest

from baseplate.lib.message_queue import InMemoryMessageQueue
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import PosixMessageQueue
from baseplate.lib.message_queue import RemoteMessageQueue
from baseplate.lib.message_queue import TimedOutError

class TestPosixMessageQueueCreation(unittest.TestCase):
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
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            self.assertEqual(mq.queue.max_messages, 1)
            self.assertEqual(mq.queue.max_message_size, 1)

    def test_put_get(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_get_timeout(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.get(timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=2)

    def test_put_timeout(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.put(b"x", timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=1)

    def test_put_zero_timeout(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x", timeout=0)
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_put_full_zero_timeout(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1)

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


class TestInMemoryMessageQueueCreation(unittest.TestCase):
    qname = "/baseplate-test-queue"

    def test_create_queue(self):
        message_queue = InMemoryMessageQueue(self.qname, max_messages=1)

        with contextlib.closing(message_queue) as mq:
            self.assertEqual(mq.queue.maxsize, 1)

    def test_put_get(self):
        message_queue = InMemoryMessageQueue(self.qname, max_messages=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_get_timeout(self):
        message_queue = InMemoryMessageQueue(self.qname, max_messages=1)

        with contextlib.closing(message_queue) as mq:
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.get(timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=2)

    def test_put_timeout(self):
        message_queue = InMemoryMessageQueue(self.qname, max_messages=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.put(b"x", timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=1)

    def test_put_zero_timeout(self):
        message_queue = InMemoryMessageQueue(self.qname, max_messages=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x", timeout=0)
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_put_full_zero_timeout(self):
        message_queue = InMemoryMessageQueue(self.qname, max_messages=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"1", timeout=0)

            with self.assertRaises(TimedOutError):
                mq.put(b"2", timeout=0)


class GeventPatchedTestCase(unittest.TestCase):
    def setUp(self):
        gevent.monkey.patch_socket()

    def tearDown(self):
        import socket

        reload(socket)
        gevent.monkey.saved.clear()


class TestRemoteMessageQueueCreation(GeventPatchedTestCase):
    qname = "/baseplate-test-queue"

    def test_put_get(self):
        with event_publisher.start_queue_server(host="127.0.0.1", port=9090) as server:
            message_queue = RemoteMessageQueue(self.qname, max_messages=10)

            with contextlib.closing(message_queue) as mq:
                mq.put(b"x", timeout=0)
                message = mq.get()
                self.assertEqual(message, b"x")

    def test_multiple_queues(self):
        with event_publisher.start_queue_server(host="127.0.0.1", port=9090) as server:
            mq1 = RemoteMessageQueue(self.qname, max_messages=10)
            mq2 = RemoteMessageQueue(self.qname + "2", max_messages=10)

            mq1.put(b"x", timeout=0)
            mq2.put(b"a", timeout=0)

            # Check the queues in reverse order
            self.assertEqual(mq2.get(), b"a")
            self.assertEqual(mq1.get(), b"x")

            mq1.close()
            mq2.close()

    def test_get_timeout(self):
        with event_publisher.start_queue_server(host="127.0.0.1", port=9090) as server:
            message_queue = RemoteMessageQueue(self.qname, max_messages=1)

            with contextlib.closing(message_queue) as mq:
                start = time.time()
                with self.assertRaises(TimedOutError):
                    mq.get(timeout=0.1)
                elapsed = time.time() - start
                self.assertAlmostEqual(elapsed, 0.1, places=2)

    def test_put_timeout(self):
        with event_publisher.start_queue_server(host="127.0.0.1", port=9090) as server:
            message_queue = RemoteMessageQueue(self.qname, max_messages=1)

            with contextlib.closing(message_queue) as mq:
                mq.put(b"x")
                start = time.time()
                with self.assertRaises(TimedOutError):
                    mq.put(b"x", timeout=0.1)
                elapsed = time.time() - start
                self.assertAlmostEqual(elapsed, 0.1, places=1)
