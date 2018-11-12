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
import logging
import time
import uuid

from enum import Enum
from thrift import TSerialization
from thrift.protocol.TJSONProtocol import TJSONProtocolFactory

from . import MAX_EVENT_SIZE, MAX_QUEUE_SIZE
from baseplate.context import ContextFactory
from baseplate.message_queue import MessageQueue, TimedOutError
from baseplate._utils import warn_deprecated


# pylint: disable=pointless-string-statement,no-init
class FieldKind(Enum):
    """Field kinds."""
    NORMAL = None
    """
    For fields normal fields with no hashing/indexing requirements.
    """

    OBFUSCATED = "obfuscated_data"
    """
    For fields containing sensitive information like IP addresses that must
    be treated with care.
    """

    HIGH_CARDINALITY = "interana_excluded"
    """
    For fields that should not be indexed due to high cardinality
    (e.g. not used in Interana)
    """


class Event(object):
    """An event."""
    # pylint: disable=invalid-name,redefined-builtin
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
        self.payload_types = {}

    def get_field(self, key):
        """Get the value of a field in the event.

        If the field is not present, :py:data:`None` is returned.

        :param str key: The name of the field.

        """

        return self.payload.get(key, None)

    def set_field(self, key, value, obfuscate=False, kind=FieldKind.NORMAL):
        """Set the value for a field in the event.

        :param str key: The name of the field.
        :param value: The value to set the field to. Should be JSON
            serializable.
        :param baseplate.events.FieldKind kind: The kind the field is.
            Used to determine what section of the payload the field belongs
            in when serialized.

        """

        # There's no need to send null/empty values, the collector will act
        # the same whether they're sent or not. Zeros are important though,
        # so we can't use a simple boolean truth check here.
        if value is None or value == "":
            return

        if obfuscate:
            kind = FieldKind.OBFUSCATED
            warn_deprecated("Passing obfuscate to set_field is deprecated in"
                            " favor of passing a FieldKind value as kind.")

        self.payload[key] = value
        self.payload_types[key] = kind

    def serialize(self):
        payload = {}

        for key, value in self.payload.items():
            kind = self.payload_types[key]

            if kind.value is None:
                payload[key] = value
            else:
                section = payload.setdefault(kind.value, {})
                section[key] = value

        return json.dumps({
            "event_topic": self.topic,
            "event_type": self.event_type,
            "event_ts": int(self.timestamp),
            "uuid": str(self.id),
            "payload": payload,
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


def serialize_v1_event(event):
    """Serialize an Event object for the V1 event protocol.

    :param baseplate.events.Event event: An event object.

    """
    return event.serialize()


_V2_PROTOCOL_FACTORY = TJSONProtocolFactory()


def serialize_v2_event(event):
    """Serialize a Thrift struct to bytes for the V2 event protocol.

    :param event: A Thrift struct from the event schemas.

    """
    return TSerialization.serialize(event, _V2_PROTOCOL_FACTORY)


class EventLogger(object):
    def log(self, **kwargs):
        raise NotImplementedError


class DebugLogger(EventLogger):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def log(self, **kwargs):
        self.logger.debug("Would send event: {}".format(kwargs))


class EventQueue(ContextFactory):
    """A queue to transfer events to the publisher.

    :param str name: The name of the event queue to send to. This specifies
        which publisher should send the events which can be useful for routing
        to different event pipelines (prod/test/v2 etc.).
    :param callable event_serializer: A callable that takes an event object
        and returns serialized bytes ready to send on the wire. See below for
        options.

    """

    def __init__(self, name, event_serializer=serialize_v1_event):
        self.queue = MessageQueue(
            "/events-" + name,
            max_messages=MAX_QUEUE_SIZE,
            max_message_size=MAX_EVENT_SIZE,
        )
        self.serialize_event = event_serializer

    def put(self, event):
        """Add an event to the queue.

        The queue is local to the server this code is run on. The event
        publisher on the server will take these events and send them to the
        collector.

        :param event: The event to send. The type of event object passed in
            depends on the selected ``event_serializer``.
        :raises: :py:exc:`EventTooLargeError` The serialized event is too large.
        :raises: :py:exc:`EventQueueFullError` The queue is full. Events are
            not being published fast enough.

        """
        serialized = self.serialize_event(event)
        if len(serialized) > MAX_EVENT_SIZE:
            raise EventTooLargeError(len(serialized))

        try:
            self.queue.put(serialized, timeout=0)
        except TimedOutError:
            raise EventQueueFullError

    def make_object_for_context(self, name, server_span):
        return self
