"""A gevent-friendly POSIX message queue."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import select

import posix_ipc

from .retry import RetryPolicy


class MessageQueueError(Exception):
    """Base exception for message queue related errors."""
    pass


class TimedOutError(MessageQueueError):
    """Raised when a message queue operation times out."""
    def __init__(self):
        super(TimedOutError, self).__init__(
            "Timed out waiting for the message queue.")


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
        for time_remaining in RetryPolicy.new(budget=timeout):
            try:
                message, _ = self.queue.receive()
                return message
            except posix_ipc.SignalError:  # pragma: nocover
                continue  # interrupted, just try again
            except posix_ipc.BusyError:
                select.select([self.queue.mqd], [], [], time_remaining)

        raise TimedOutError

    def put(self, message, timeout=None):
        """Add a message to the queue.

        :param float timeout: If the queue is full, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was full for the allowed
            duration of the call.

        """
        for time_remaining in RetryPolicy.new(budget=timeout):
            try:
                return self.queue.send(message=message)
            except posix_ipc.SignalError:  # pragma: nocover
                continue  # interrupted, just try again
            except posix_ipc.BusyError:
                select.select([], [self.queue.mqd], [], time_remaining)

        raise TimedOutError

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


def queue_tool():
    import argparse
    import sys

    parser = argparse.ArgumentParser()

    parser.add_argument("--max-messages", type=int, default=10,
        help="if creating the queue, what to set the maximum queue length to")
    parser.add_argument("--max-message-size", type=int, default=8096,
        help="if creating the queue, what to set the maximum message size to")
    parser.add_argument("queue_name", help="the name of the queue to consume")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--create", action="store_const", dest="mode", const="create",
        help="create the named queue if it doesn't exist and exit")
    group.add_argument("--read", action="store_const", dest="mode", const="read",
        help="read, log, and discard messages from the named queue")
    group.add_argument("--write", action="store_const", dest="mode", const="write",
        help="read messages from stdin and write them to the named queue")

    args = parser.parse_args()

    queue = MessageQueue(args.queue_name, args.max_messages, args.max_message_size)

    if args.mode == "read":
        while True:
            item = queue.get()
            print(item)
    elif args.mode == "write":
        for line in sys.stdin:
            queue.put(line.rstrip("\n"))


if __name__ == "__main__":
    queue_tool()
