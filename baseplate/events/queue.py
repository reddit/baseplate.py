"""Client library for sending events to the data processing system.

This is for use with the event collector system. Events generally track
something that happens in production that we want to instrument for planning
and analytical purposes.

Events are serialized and put onto a message queue on the same server. These
serialized events are then consumed and published to the remote event collector
by a separate daemon.

See also: https://github.com/reddit/event-collector

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import calendar
import json
import time
import uuid

from . import MAX_EVENT_SIZE, MAX_QUEUE_SIZE
from baseplate.message_queue import MessageQueue, TimedOutError


class Event(object):
    """An event."""
    def __init__(self, topic, event_type, timestamp=None, id=None):
        self.topic = topic
        self.event_type = event_type
        if timestamp:
            if timestamp.tzinfo and timestamp.utcoffset().total_seconds() != 0:
                raise ValueError("Timestamps must be in UTC")
            self.timestamp = calendar.timegm(timestamp.timetuple()) * 1000
        else:
            self.timestamp = time.time() * 1000
        self.id = id or uuid.uuid4()
        self.payload = {}

    def get_field(self, key, obfuscated=False):
        """Get the value of a field in the event.

        If the field is not present, :py:data:`None` is returned.

        :param str key: The name of the field.
        :param bool obfuscated: Whether to look for the field in the obfuscated
            payload.

        """

        if not obfuscated:
            payload = self.payload
        else:
            payload = self.payload.get("obfuscated_data", {})
        return payload.get(key, None)

    def set_field(self, key, value, obfuscate=False):
        """Set the value for a field in the event.

        :param str key: The name of the field.
        :param value: The value to set the field to. Should be JSON
            serializable.
        :param bool obfuscate: Whether or not to put the field in the obfuscated
            section. This is used for sensitive info like IP addresses that must
            be treated with care.

        """

        # There's no need to send null/empty values, the collector will act
        # the same whether they're sent or not. Zeros are important though,
        # so we can't use a simple boolean truth check here.
        if value is None or value == "":
            return

        if not obfuscate:
            self.payload[key] = value
        else:
            obfuscated_payload = self.payload.setdefault("obfuscated_data", {})
            obfuscated_payload[key] = value

    def unset_field(self, key, obfuscated=False):
        """Remove the key from payload if it exists.

        :param str key: The name of the field
        :param bool obfuscated: Whether to look for the field in
            the obfuscated payload.
        :return: value of key that was removed.  :py:data:`None` otherwise.
        """
        payload = self.payload if not obfuscated\
            else self.payload.get("obfuscated_data")
        return payload.pop(key, None)

    def serialize(self):
        return json.dumps({
            "event_topic": self.topic,
            "event_type": self.event_type,
            "event_ts": int(self.timestamp),
            "uuid": str(self.id),
            "payload": self.payload,
        })


class EventError(Exception):
    """Base class for event related exceptions."""
    pass


class EventTooLargeError(EventError):
    """Raised when a serialized event is too large to send."""
    def __init__(self, size):
        super(EventTooLargeError, self).__init__(
            "Event is too large to send (%d bytes)" % size)


class EventQueueFullError(EventError):
    """Raised when the queue of events is full.

    This usually indicates that the event publisher is having trouble talking
    to the event collector.

    """
    def __init__(self):
        super(EventQueueFullError, self).__init__("The event queue is full.")


class EventQueue(object):
    """A queue to transfer events to the publisher."""

    def __init__(self, name):
        self.queue = MessageQueue(
            "/events-" + name,
            max_messages=MAX_QUEUE_SIZE,
            max_message_size=MAX_EVENT_SIZE,
        )

    def put(self, event):
        """Add an event to the queue.

        The queue is local to the server this code is run on. The event
        publisher on the server will take these events and send them to the
        collector.

        :param baseplate.events.Event event: The event to send.
        :raises: :py:exc:`EventTooLargeError` The serialized event is too large.
        :raises: :py:exc:`EventQueueFullError` The queue is full. Events are
            not being published fast enough.

        """
        serialized = event.serialize()
        if len(serialized) > MAX_EVENT_SIZE:
            raise EventTooLargeError(len(serialized))

        try:
            self.queue.put(serialized, timeout=0)
        except TimedOutError:
            raise EventQueueFullError
