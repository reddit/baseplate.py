"""Client library for sending events to the data processing system.

This is for use with the event collector system. Events generally track
something that happens in production that we want to instrument for planning
and analytical purposes.

Events are serialized and put onto a message queue on the same server. These
serialized events are then consumed and published to the remote event collector
by a separate daemon.

"""
import logging

from typing import Any
from typing import Callable
from typing import Generic
from typing import TypeVar

from thrift import TSerialization
from thrift.protocol.TJSONProtocol import TJSONProtocolFactory

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import TimedOutError


MAX_EVENT_SIZE = 102400
MAX_QUEUE_SIZE = 10000


class EventError(Exception):
    """Base class for event related exceptions."""


class EventTooLargeError(EventError):
    """Raised when a serialized event is too large to send."""

    def __init__(self, size: int):
        super().__init__(f"Event is too large to send ({size:d} bytes)")


class EventQueueFullError(EventError):
    """Raised when the queue of events is full.

    This usually indicates that the event publisher is having trouble talking
    to the event collector.

    """

    def __init__(self) -> None:
        super().__init__("The event queue is full.")


_V2_PROTOCOL_FACTORY = TJSONProtocolFactory()


def serialize_v2_event(event: Any) -> bytes:
    """Serialize a Thrift struct to bytes for the V2 event protocol.

    :param event: A Thrift struct from the event schemas.

    """
    return TSerialization.serialize(event, _V2_PROTOCOL_FACTORY)


class EventLogger:
    def log(self, **kwargs: Any) -> None:
        raise NotImplementedError


class DebugLogger(EventLogger):
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def log(self, **kwargs: Any) -> None:
        self.logger.debug("Would send event: %s", kwargs)


T = TypeVar("T")


class EventQueue(ContextFactory, config.Parser, Generic[T]):
    """A queue to transfer events to the publisher.

    :param name: The name of the event queue to send to. This specifies
        which publisher should send the events which can be useful for routing
        to different event pipelines (prod/test/v2 etc.).
    :param event_serializer: A callable that takes an event object
        and returns serialized bytes ready to send on the wire. See below for
        options.

    """

    def __init__(self, name: str, event_serializer: Callable[[T], bytes]):
        self.queue = MessageQueue(
            "/events-" + name, max_messages=MAX_QUEUE_SIZE, max_message_size=MAX_EVENT_SIZE
        )
        self.serialize_event = event_serializer

    def put(self, event: T) -> None:
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

    def make_object_for_context(self, name: str, span: Span) -> "EventQueue[T]":
        return self

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "EventQueue[T]":
        return self
