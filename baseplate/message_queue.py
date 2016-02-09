from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import select
import time

import posix_ipc


class MessageQueueError(Exception):
    """Base exception for message queue related errors."""
    pass


class TimedOutError(MessageQueueError):
    """Raised when a message queue operation times out."""
    def __init__(self):
        super(TimedOutError, self).__init__(
            "Timed out waiting for the message queue.")


class _CumulativeTimeoutSelector(object):
    """Helper to track a cumulative timeout across multiple select calls."""
    def __init__(self, timeout):
        self.time_remaining = timeout

    def select(self, rfds=None, wfds=None):
        if self.time_remaining is not None and self.time_remaining <= 0:
            raise TimedOutError

        # we use select.select here instead of the queue's own timeout ability
        # so that if we're in gevent, the hub can monitor the queue's readiness
        # rather than blocking the whole process.
        start = time.time()
        readable, writable, _ = select.select(
            rfds or [], wfds or [], [], self.time_remaining)
        elapsed = time.time() - start

        if self.time_remaining > 0:
            self.time_remaining -= elapsed

        return readable, writable


class MessageQueue(object):
    """A gevent-friendly (but not required) inter process message queue.

    ``name`` should be a string of up to 255 characters consisting of an
    initial slash, followed by one or more characters, none of which are
    slashes.

    Note: This relies on POSIX message queues being available and
    select(2)-able like other file descriptors. Not all operating systems
    support this.

    """
    def __init__(self, name, max_messages, max_message_size):
        self.queue = posix_ipc.MessageQueue(
            name,
            flags=posix_ipc.O_CREAT,
            mode=0o0644,
            max_messages=max_messages,
            max_message_size=max_message_size,
        )
        self.queue.block = False

    def get(self, timeout=None):
        """Read a message from the queue.

        :param float timeout: If the queue is empty, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was empty for the allowed
            duration of the call.

        """
        selector = _CumulativeTimeoutSelector(timeout)

        while True:
            try:
                message, priority = self.queue.receive()
                return message
            except posix_ipc.SignalError:  # pragma: nocover
                continue  # interrupted, just try again
            except posix_ipc.BusyError:
                selector.select(rfds=[self.queue.mqd])

    def put(self, message, timeout=None):
        """Add a message to the queue.

        :param float timeout: If the queue is full, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was full for the allowed
            duration of the call.

        """
        selector = _CumulativeTimeoutSelector(timeout)

        while True:
            try:
                return self.queue.send(message=message)
            except posix_ipc.SignalError:  # pragma: nocover
                continue  # interrupted, just try again
            except posix_ipc.BusyError:
                selector.select(wfds=[self.queue.mqd])

    def unlink(self):
        """Remove the queue from the system.

        The queue will not leave until the last active user closes it.

        """
        self.queue.unlink()

    def close(self):
        """Close the queue, freeing related resources.

        This must be called explicitly if queues are created/destroyed on the
        fly. It is not automatically called when the object is reclaimed by
        Python.

        """
        self.queue.close()
