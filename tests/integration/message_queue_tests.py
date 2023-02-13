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
    qname = "/baseplate-test-queue"

    def test_instantiate_queue(self):
        message_queue = InMemoryMessageQueue(max_messages=1)

        with contextlib.closing(message_queue) as mq:
            self.assertEqual(mq.queue.maxsize, 1)

    def test_put_get(self):
        message_queue = InMemoryMessageQueue(max_messages=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_get_timeout(self):
        message_queue = InMemoryMessageQueue(max_messages=1)

        with contextlib.closing(message_queue) as mq:
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.get(timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=2)

    def test_put_timeout(self):
        message_queue = InMemoryMessageQueue(max_messages=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            start = time.time()
            with self.assertRaises(TimedOutError):
                mq.put(b"x", timeout=0.1)
            elapsed = time.time() - start
            self.assertAlmostEqual(elapsed, 0.1, places=1)

    def test_put_zero_timeout(self):
        message_queue = InMemoryMessageQueue(max_messages=1)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x", timeout=0)
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_put_full_zero_timeout(self):
        message_queue = InMemoryMessageQueue(max_messages=1)

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
        # start the server that would ordinarily be running on the sidecar
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090):
            message_queue = RemoteMessageQueue(self.qname, max_messages=10)

            with contextlib.closing(message_queue) as mq:
                mq.put(b"x", timeout=0)
                message = mq.get()
                self.assertEqual(message, b"x")

    def test_multiple_queues(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090):
            mq1 = RemoteMessageQueue(self.qname, max_messages=10)
            mq2 = RemoteMessageQueue(self.qname + "2", max_messages=10)

            mq1.put(b"x", timeout=0)
            mq2.put(b"a", timeout=0)

            # Check the queues in reverse order
            self.assertEqual(mq2.get(), b"a")
            self.assertEqual(mq1.get(), b"x")

            mq1.close()
            mq2.close()

    def test_queues_alternate_port(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9091):
            message_queue = RemoteMessageQueue(self.qname, max_messages=10, port=9091)

            with contextlib.closing(message_queue) as mq:
                mq.put(b"x", timeout=0)
                self.assertEqual(mq.get(), b"x")

    def test_get_timeout(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090):
            message_queue = RemoteMessageQueue(self.qname, max_messages=1)

            with contextlib.closing(message_queue) as mq:
                start = time.time()
                with self.assertRaises(TimedOutError):
                    mq.get(timeout=0.1)
                elapsed = time.time() - start
                self.assertAlmostEqual(
                    elapsed, 0.1, places=1
                )  # TODO: this routinely takes 0.105-0.11 seconds, is 1 place ok?

    def test_put_timeout(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090):
            message_queue = RemoteMessageQueue(self.qname, max_messages=1)

            with contextlib.closing(message_queue) as mq:
                mq.put(b"x")
                start = time.time()
                with self.assertRaises(TimedOutError):
                    mq.put(b"x", timeout=0.1)
                elapsed = time.time() - start
                self.assertAlmostEqual(elapsed, 0.1, places=1)

    def test_thrift_retry(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090):
            message_queue = RemoteMessageQueue(self.qname, max_messages=1)

            with contextlib.closing(message_queue) as mq:
                mq.put(b"x")
                # close the connection manually
                mq.close()
                # this should still pass, as it catches the thrift error and re-connects
                self.assertEqual(mq.get(), b"x")

    def test_get_thrift_retry_and_timeout(self):
        # Check that we still catch a timeout error even if we have to reconnect
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090):
            message_queue = RemoteMessageQueue(self.qname, max_messages=1)

            with contextlib.closing(message_queue) as mq:
                mq.close()
                start = time.time()
                with self.assertRaises(TimedOutError):
                    mq.get(timeout=0.1)
                elapsed = time.time() - start
                self.assertAlmostEqual(elapsed, 0.1, places=1)


class TestCreateQueue(GeventPatchedTestCase):
    def test_posix_queue(self):
        queue: MessageQueue = create_queue(QueueType.POSIX, "/test", 5, 1000)
        assert isinstance(queue, PosixMessageQueue)

    def test_in_memory_create_queue(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090):
            queue: MessageQueue = create_queue(QueueType.IN_MEMORY, "/test", 5, 1000)
            assert isinstance(queue, RemoteMessageQueue)
