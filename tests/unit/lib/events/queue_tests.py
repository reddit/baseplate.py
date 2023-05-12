import unittest

from unittest import mock

from baseplate.lib.events import EventQueue
from baseplate.lib.events import EventQueueFullError
from baseplate.lib.events import EventTooLargeError
from baseplate.lib.events import MAX_EVENT_SIZE
from baseplate.lib.events import MAX_QUEUE_SIZE
from baseplate.lib.message_queue import PosixMessageQueue
from baseplate.lib.message_queue import RemoteMessageQueue
from baseplate.lib.message_queue import TimedOutError


class PosixEventQueueTests(unittest.TestCase):
    @mock.patch("baseplate.lib.message_queue.PosixMessageQueue", autospec=PosixMessageQueue)
    def setUp(self, PosixMessageQueue):
        self.message_queue = PosixMessageQueue.return_value
        self.mock_serializer = mock.Mock()
        queue = PosixMessageQueue("/test", 10, 100)
        self.queue = EventQueue("test", event_serializer=self.mock_serializer, queue=queue)

    def test_send_event(self):
        self.mock_serializer.return_value = "i_am_serialized"
        event = object()

        self.queue.put(event)

        self.assertEqual(self.message_queue.put.call_count, 1)
        self.mock_serializer.assert_called_with(event)
        args, kwargs = self.message_queue.put.call_args
        self.assertEqual(args[0], self.mock_serializer.return_value)

    def test_event_too_large(self):
        self.mock_serializer.return_value = "x" * (MAX_EVENT_SIZE + 1)

        with self.assertRaises(EventTooLargeError):
            self.queue.put(object())

    def test_event_queue_full(self):
        self.mock_serializer.return_value = ""

        self.message_queue.put.side_effect = TimedOutError

        with self.assertRaises(EventQueueFullError):
            self.queue.put(object())

    @mock.patch("baseplate.lib.events.MAX_QUEUE_SIZE", 10)
    @mock.patch("baseplate.lib.events.MAX_EVENT_SIZE", 100)
    def test_default_queue(self):
        queue = EventQueue("test", event_serializer=self.mock_serializer)
        assert isinstance(queue.queue, PosixMessageQueue)


class RemoteMessageQueueTests(unittest.TestCase):
    @mock.patch("baseplate.lib.message_queue.RemoteMessageQueue", autospec=RemoteMessageQueue)
    def setUp(self, RemoteMessageQueue):
        self.message_queue = RemoteMessageQueue.return_value
        self.mock_serializer = mock.Mock()
        queue = RemoteMessageQueue("test", MAX_QUEUE_SIZE, "127.0.0.1", 9090)
        self.queue = EventQueue("test", event_serializer=self.mock_serializer, queue=queue)

    def test_send_event(self):
        self.mock_serializer.return_value = "i_am_serialized"
        event = object()

        self.queue.put(event)

        self.assertEqual(self.message_queue.put.call_count, 1)
        self.mock_serializer.assert_called_with(event)
        args, kwargs = self.message_queue.put.call_args
        self.assertEqual(args[0], self.mock_serializer.return_value)

    def test_event_queue_full(self):
        self.mock_serializer.return_value = ""

        self.message_queue.put.side_effect = TimedOutError

        with self.assertRaises(EventQueueFullError):
            self.queue.put(object())
