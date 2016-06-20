from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import json
import unittest
import warnings

from baseplate.events import (
    Event,
    EventQueue,
    EventQueueFullError,
    EventTooLargeError,
    FieldKind,
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
        event.set_field("obfuscated", "value2", kind=FieldKind.OBFUSCATED)
        event.set_field("high_cardinality", "value3",
                        kind=FieldKind.HIGH_CARDINALITY)
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
                "interana_excluded": {
                    "high_cardinality": "value3",
                },
            },
        })

    def _assert_payload(self, event, payload):
        self.assertEqual(json.loads(event.serialize()), {
            "event_topic": event.topic,
            "event_type": event.event_type,
            "event_ts": event.timestamp,
            "uuid": event.id,
            "payload": payload,
        })

    @mock.patch("uuid.uuid4")
    @mock.patch("time.time")
    def test_set_field(self, time, uuid):
        time.return_value = 333
        uuid.return_value = "1-2-3-4"

        event = Event("topic", "type")

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            event.set_field("deprecated", "value", obfuscate=True)

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))
            self.assertIn("deprecated", str(w[-1].message))

        self._assert_payload(event, {
            "obfuscated_data": {
                "deprecated": "value",
            },
        })

    @mock.patch("uuid.uuid4")
    @mock.patch("time.time")
    def test_set_field_same_key_different_sections(self, time, uuid):
        time.return_value = 333
        uuid.return_value = "1-2-3-4"

        event = Event("topic", "type")

        event.set_field("foo", "bar")
        self._assert_payload(event, {
            "foo": "bar",
        })

        event.set_field("foo", "bar", kind=FieldKind.OBFUSCATED)
        self._assert_payload(event, {
            "obfuscated_data": {
                "foo": "bar",
            },
        })

        event.set_field("foo", "bar")
        self._assert_payload(event, {
            "foo": "bar",
        })

        event.set_field("foo", "bar", kind=FieldKind.HIGH_CARDINALITY)
        self._assert_payload(event, {
            "interana_excluded": {
                "foo": "bar",
            },
        })

        event.set_field("foo", "bar")
        self._assert_payload(event, {
            "foo": "bar",
        })

        event.set_field("foo", "bar", kind=FieldKind.HIGH_CARDINALITY)
        self._assert_payload(event, {
            "interana_excluded": {
                "foo": "bar",
            },
        })

        event.set_field("foo", "bar", kind=FieldKind.OBFUSCATED)
        self._assert_payload(event, {
            "obfuscated_data": {
                "foo": "bar",
            },
        })

        event.set_field("foo", "bar", kind=FieldKind.HIGH_CARDINALITY)
        self._assert_payload(event, {
            "interana_excluded": {
                "foo": "bar",
            },
        })

    @mock.patch("uuid.uuid4")
    @mock.patch("time.time")
    def test_get_field(self, time, uuid):
        time.return_value = 333
        uuid.return_value = "1-2-3-4"

        event = Event("topic", "type")
        event.set_field("normal", "value1")
        event.set_field("obfuscated", "value2", kind=FieldKind.OBFUSCATED)
        event.set_field("high_cardinality", "value3",
                        kind=FieldKind.HIGH_CARDINALITY)

        self.assertEqual(event.get_field("normal"), "value1")
        self.assertEqual(event.get_field("obfuscated"), "value2")
        self.assertEqual(event.get_field("high_cardinality"), "value3")


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
