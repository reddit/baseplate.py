import contextlib
import time
import unittest

from importlib import reload

import gevent
import posix_ipc

from baseplate.lib.message_queue import create_queue
from baseplate.lib.message_queue import InMemoryMessageQueue
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import PosixMessageQueue
from baseplate.lib.message_queue import QueueType
from baseplate.lib.message_queue import RemoteMessageQueue
from baseplate.lib.message_queue import TimedOutError
from baseplate.sidecars import publisher_queue_utils


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

    def test_instantiate_queue(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1000)

        with contextlib.closing(message_queue) as mq:
            self.assertEqual(mq.queue.max_messages, 1)
            self.assertEqual(mq.queue.max_message_size, 1000)

    def test_put_get(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1000)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_get_timeout(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1000)

        with contextlib.closing(message_queue) as mq:
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.get(timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=2)

    def test_put_timeout(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1000)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.put(b"x", timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=1)

    def test_put_zero_timeout(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1000)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x", timeout=0)
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_put_full_zero_timeout(self):
        message_queue = PosixMessageQueue(self.qname, max_messages=1, max_message_size=1000)

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
    def test_instantiate_queue(self):
        mq = InMemoryMessageQueue(max_messages=1)
        self.assertEqual(mq.queue.maxsize, 1)

    def test_put_get(self):
        mq = InMemoryMessageQueue(max_messages=1)
        mq.put(b"x")
        message = mq.get()
        self.assertEqual(message, b"x")

    def test_get_timeout(self):
        mq = InMemoryMessageQueue(max_messages=1)
        start = time.time()
        with self.assertRaises(TimedOutError):
            mq.get(timeout=0.1)
        elapsed = time.time() - start
        self.assertAlmostEqual(elapsed, 0.1, places=2)

    def test_put_timeout(self):
        mq = InMemoryMessageQueue(max_messages=1)
        mq.put(b"x")
        start = time.time()
        with self.assertRaises(TimedOutError):
            mq.put(b"x", timeout=0.1)
        elapsed = time.time() - start
        self.assertAlmostEqual(elapsed, 0.1, places=1)

    def test_put_zero_timeout(self):
        mq = InMemoryMessageQueue(max_messages=1)
        mq.put(b"x", timeout=0)
        message = mq.get()
        self.assertEqual(message, b"x")

    def test_put_full_zero_timeout(self):
        mq = InMemoryMessageQueue(max_messages=1)
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
        # create the queue and start the server that would ordinarily be running on the sidecar
        imq = InMemoryMessageQueue(max_messages=10)
        with publisher_queue_utils.start_queue_server(imq, host="127.0.0.1", port=9091):
            # create the corresponding queue that would ordinarily be on the main baseplate application/client-side
            mq = RemoteMessageQueue(self.qname, pool_size=1)
            g = mq.put(b"x")
            gevent.joinall([g])
            message = imq.get(timeout=0.1)
            self.assertEqual(message, b"x")

    def test_queues_alternate_port(self):
        imq = InMemoryMessageQueue(max_messages=10)
        with publisher_queue_utils.start_queue_server(imq, host="127.0.0.1", port=9092):
            mq = RemoteMessageQueue(self.qname, port=9092, pool_size=1)

            g = mq.put(b"x", timeout=0.1)
            gevent.joinall([g])
            self.assertEqual(imq.get(timeout=2), b"x")

    def test_get_timeout(self):
        imq = InMemoryMessageQueue(max_messages=1)
        with publisher_queue_utils.start_queue_server(imq, host="127.0.0.1", port=9091):
            _ = RemoteMessageQueue(self.qname, pool_size=1)  # create the empty queue

            start = time.time()
            with self.assertRaises(TimedOutError):
                imq.get(timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(
                elapsed, 0.1, places=1
            )  # TODO: this routinely takes 0.105-0.11 seconds, is 1 place ok?

    def test_put_timeout(self):
        imq = InMemoryMessageQueue(max_messages=1)
        # `put` is non-blocking, so if we try to put onto a full queue and a TimeOutError
        # is raised, we dont actually know unless we explicitly check
        with publisher_queue_utils.start_queue_server(imq, host="127.0.0.1", port=9091):
            mq = RemoteMessageQueue(self.qname, pool_size=2)

            g = mq.put(b"x")  # fill the queue
            start = time.time()
            with self.assertRaises(TimedOutError):  # queue should be full
                # put is non-blocking, so we need to wait for the result
                g2 = mq.put(b"x", timeout=0.1)
                gevent.joinall([g, g2])
                g2.get()  # this should expose any exceptions encountered, i.e. TimedOutError
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=1)


class TestCreateQueue:
    def test_posix_queue(self):
        queue: MessageQueue = create_queue(QueueType.POSIX, "/test", 5, 1000)
        assert isinstance(queue, PosixMessageQueue)

    def test_in_memory_create_queue(self):
        queue: MessageQueue = create_queue(QueueType.IN_MEMORY, "/test", 5, 1000)
        assert isinstance(queue, InMemoryMessageQueue)
