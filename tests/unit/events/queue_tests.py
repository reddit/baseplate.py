from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import json
import unittest

from baseplate.events import (
    Event,
    EventError,
    EventQueue,
    EventQueueFullError,
    EventTooLargeError,
    MAX_EVENT_SIZE,
)
from baseplate.message_queue import MessageQueue, TimedOutError

from ... import mock


class MockTZ(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=2)


class EventTests(unittest.TestCase):
    @mock.patch("uuid.uuid4")
    @mock.patch("time.time")
    def test_minimal_constructor(self, time, uuid):
        time.return_value = 333
        uuid.return_value = "1-2-3-4"

        event = Event("topic", "type")

        self.assertEqual(event.topic, "topic")
        self.assertEqual(event.event_type, "type")
        self.assertEqual(event.timestamp, 333000)
        self.assertEqual(event.id, "1-2-3-4")

    def test_timestamp_specified(self):
        timestamp = datetime.datetime(2016, 2, 23, 0, 3, 17)
        event = Event("topic", "type", timestamp=timestamp)
        self.assertEqual(event.timestamp, 1456185797000)

    def test_timestamp_not_utc(self):
        timestamp = datetime.datetime(2016, 2, 23, 0, 3, 17, tzinfo=MockTZ())
        with self.assertRaises(ValueError):
            Event("topic", "type", timestamp=timestamp)

    @mock.patch("uuid.uuid4")
    @mock.patch("time.time")
    def test_serialize(self, time, uuid):
        time.return_value = 333
        uuid.return_value = "1-2-3-4"

        event = Event("topic", "type")
        event.set_field("normal", "value1")
        event.set_field("obfuscated", "value2", obfuscate=True)
        event.set_field("empty", "")
        event.set_field("null", None)

        serialized = event.serialize()
        deserialized = json.loads(serialized)

        self.assertEqual(deserialized, {
            "event_topic": "topic",
            "event_type": "type",
            "event_ts": 333000,
            "uuid": "1-2-3-4",
            "payload": {
                "normal": "value1",
                "obfuscated_data": {
                    "obfuscated": "value2",
                },
            },
        })

    def test_unset_field(self):
        event = Event("topic", "type")
        event.set_field("foo", "bar")

        # foo should point to bar
        self.assertEqual("bar", event.get_field("foo"))
        # removing the field should return bar
        self.assertEqual("bar", event.unset_field("foo"))
        # getting the field should return None
        self.assertIsNone(event.get_field("foo"))
        # removing it again should return None
        self.assertIsNone(event.unset_field("foo"))

    def test_unset_fields(self):
        event = Event("topic", "type")

        fields = ["a", "b", "c"]
        n = len(fields)
        for i in xrange(n):
            event.set_field(fields[i], i)

        # they should exist
        for i in xrange(n):
            self.assertIsNotNone(event.get_field(fields[i]))

        # number of removed entries should be n
        self.assertEqual(event.unset_fields(fields), n)

        # getting them should result in None
        for i in xrange(n):
            self.assertIsNone(event.get_field(fields[i]))


class EventQueueTests(unittest.TestCase):
    @mock.patch("baseplate.events.queue.MessageQueue", autospec=MessageQueue)
    def setUp(self, MessageQueue):
        self.message_queue = MessageQueue.return_value
        self.queue = EventQueue("test")

    def test_send_event(self):
        mock_event = mock.Mock(autospec=Event)
        mock_event.serialize.return_value = "i_am_serialized"

        self.queue.put(mock_event)

        self.assertEqual(self.message_queue.put.call_count, 1)
        args, kwargs = self.message_queue.put.call_args
        self.assertEqual(args[0], mock_event.serialize.return_value)

    def test_event_too_large(self):
        mock_event = mock.Mock(autospec=Event)
        mock_event.serialize.return_value = "x" * (MAX_EVENT_SIZE+1)

        with self.assertRaises(EventTooLargeError):
            self.queue.put(mock_event)

    def test_event_queue_full(self):
        mock_event = mock.Mock(autospec=Event)
        mock_event.serialize.return_value = ""

        self.message_queue.put.side_effect = TimedOutError

        with self.assertRaises(EventQueueFullError):
            self.queue.put(mock_event)
