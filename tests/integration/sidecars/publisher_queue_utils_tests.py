import contextlib
import unittest

from importlib import reload
from unittest import mock

import gevent.monkey
import posix_ipc

from baseplate.lib import config
from baseplate.lib.events import MAX_QUEUE_SIZE
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import PosixMessageQueue
from baseplate.lib.message_queue import RemoteMessageQueue
from baseplate.sidecars import publisher_queue_utils
from baseplate.thrift.message_queue.ttypes import GetResponse


class GeventPatchedTestCase(unittest.TestCase):
    def setUp(self):
        gevent.monkey.patch_socket()

    def tearDown(self):
        import socket

        reload(socket)
        gevent.monkey.saved.clear()


class PublisherQueueUtilTests(GeventPatchedTestCase):
    def test_posix_queue(self):
        queue: MessageQueue = publisher_queue_utils.create_queue("posix", "test", 5, 8000)
        assert isinstance(queue, PosixMessageQueue)

    def test_posix_queue_get_put(self):
        queue: MessageQueue = publisher_queue_utils.create_queue("posix", "test", 5, 8000)

        test_message = bytes("message", "utf-8")
        test_message_2 = bytes("2nd message", "utf-8")
        queue.put(test_message)
        queue.put(test_message_2)
        output = queue.get()
        assert output == test_message
        output = queue.get()
        assert output == test_message_2

    def test_in_memory_create_queue(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090) as server:
            queue: MessageQueue = publisher_queue_utils.create_queue("in_memory", "test", 5, 8000)
            assert isinstance(queue, RemoteMessageQueue)

    def test_in_memory_queue_get_put(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9090) as server:
            queue: MessageQueue = publisher_queue_utils.create_queue("in_memory", "test", 5, 8000)

            test_message = bytes("message", "utf-8")
            test_message_2 = bytes("2nd message", "utf-8")
            queue.put(test_message)
            queue.put(test_message_2)
            output = queue.get()
            assert output == test_message
            output = queue.get()
            assert output == test_message_2

    def test_in_memory_queue_alternate_port(self):
        with publisher_queue_utils.start_queue_server(host="127.0.0.1", port=9091) as server:
            queue: MessageQueue = publisher_queue_utils.create_queue("in_memory", "test", 5, 8000, port=9091)

            test_message = bytes("message", "utf-8")
            test_message_2 = bytes("2nd message", "utf-8")
            queue.put(test_message)
            queue.put(test_message_2)
            output = queue.get()
            assert output == test_message
            output = queue.get()
            assert output == test_message_2

