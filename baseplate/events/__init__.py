from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
#from __future__ import unicode_literals This breaks __all__ on PY2

MAX_EVENT_SIZE = 102400
MAX_QUEUE_SIZE = 10000

from .queue import (
    DebugLogger,
    Event,
    EventError,
    EventQueue,
    EventQueueFullError,
    EventTooLargeError,
    EventLogger,
    FieldKind,
    serialize_v1_event,
    serialize_v2_event,
)

__all__ = [
    "DebugLogger",
    "Event",
    "EventError",
    "EventQueue",
    "EventQueueFullError",
    "EventTooLargeError",
    "EventLogger",
    "FieldKind",
    "serialize_v1_event",
    "serialize_v2_event",
]
