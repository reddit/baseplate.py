"""A Gevent-friendly POSIX message queue."""
from abc import abstractmethod
import queue
import select

from typing import Optional

import posix_ipc

from baseplate.lib.retry import RetryPolicy


class MessageQueueError(Exception):
    """Base exception for message queue related errors."""


class TimedOutError(MessageQueueError):
    """Raised when a message queue operation times out."""

    def __init__(self) -> None:
        super().__init__("Timed out waiting for the message queue.")


# this wrapper-exception is here just to give the user a bit more of an idea
# how to fix the error should they run into it since the base error message
# is rather opaque.
class InvalidParametersError(ValueError):
    def __init__(self, inner: Exception):
        super().__init__(f"{inner} (check fs.mqueue.{{msg_max,msgsize_max}} sysctls?)")


# this wrapper-exception is here just to give the user a bit more of an idea
# how to fix the error should they run into it since the base error message
# is rather opaque.
class MessageQueueOSError(OSError):
    def __init__(self, inner: Exception):
        super().__init__(f"{inner} (check `ulimit -q`?)")

class MessageQueue:
    @abstractmethod
    def __init__(self, name: str, max_messages: int, max_message_size: int):
        pass

    @abstractmethod
    def get(self, timeout: Optional[float] = None) -> bytes:
        """Read a message from the queue.

        :param timeout: If the queue is empty, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was empty for the allowed
            duration of the call.

        """
        pass

    @abstractmethod
    def put(self, message: bytes, timeout: Optional[float] = None) -> None:
        """Add a message to the queue.

        :param timeout: If the queue is full, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was full for the allowed
            duration of the call.

        """
        pass

class InMemoryMessageQueue(MessageQueue): 
    def __init__(self, name: str, max_messages: int):
        self.queue = queue.Queue(max_messages)
        self.max_messages = max_messages
        self.name = name

    def get(self, timeout: Optional[float] = None) -> bytes:
        """Read a message from the queue.

        :param timeout: If the queue is empty, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was empty for the allowed
            duration of the call.

        """
        try: 
            message = self.queue.get(timeout=timeout)
            self.queue.task_done()
            return message
        except queue.Empty:
            raise TimedOutError
        
    def put(self, message: bytes, timeout: Optional[float] = None) -> None:
        """Add a message to the queue.

        :param timeout: If the queue is full, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was full for the allowed
            duration of the call.

        """
        try: 
            return self.queue.put(message, timeout=timeout)
        except queue.Full:
            raise TimedOutError

    def unlink(self) -> None:
        pass

    def close(self) -> None: 
        pass

class PosixMessageQueue(MessageQueue):
    """A Gevent-friendly (but not required) inter process message queue.

    ``name`` should be a string of up to 255 characters consisting of an
    initial slash, followed by one or more characters, none of which are
    slashes.

    Note: This relies on POSIX message queues being available and
    select(2)-able like other file descriptors. Not all operating systems
    support this.

    """
    def __init__(self, name: str, max_messages: int, max_message_size: int):
        try:
            self.queue = posix_ipc.MessageQueue(
                name,
                flags=posix_ipc.O_CREAT,
                mode=0o0644,
                max_messages=max_messages,
                max_message_size=max_message_size,
            )
        except ValueError as exc:
            raise InvalidParametersError(exc)
        except OSError as exc:
            raise MessageQueueOSError(exc)
        self.queue.block = False

    def get(self, timeout: Optional[float] = None) -> bytes:
        """Read a message from the queue.

        :param timeout: If the queue is empty, the call will block up to
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

    def put(self, message: bytes, timeout: Optional[float] = None) -> None:
        """Add a message to the queue.

        :param timeout: If the queue is full, the call will block up to
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
    
    def unlink(self) -> None:
        """Remove the queue from the system.

        The queue will not leave until the last active user closes it.

        """
        self.queue.unlink()

    def close(self) -> None:
        """Close the queue, freeing related resources.

        This must be called explicitly if queues are created/destroyed on the
        fly. It is not automatically called when the object is reclaimed by
        Python.

        """
        self.queue.close()
    


def queue_tool() -> None:
    import argparse
    import sys

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-messages",
        type=int,
        default=10,
        help="if creating the queue, what to set the maximum queue length to",
    )
    parser.add_argument(
        "--max-message-size",
        type=int,
        default=8096,
        help="if creating the queue, what to set the maximum message size to",
    )
    parser.add_argument("queue_name", help="the name of the queue to consume")
    parser.add_argument("use_in_memory_queue", default=False, help="whether to use an in-memory queue or a posix queue")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--create",
        action="store_const",
        dest="mode",
        const="create",
        help="create the named queue if it doesn't exist and exit",
    )
    group.add_argument(
        "--read",
        action="store_const",
        dest="mode",
        const="read",
        help="read, log, and discard messages from the named queue",
    )
    group.add_argument(
        "--write",
        action="store_const",
        dest="mode",
        const="write",
        help="read messages from stdin and write them to the named queue",
    )

    args = parser.parse_args()

    if args.use_in_memory_queue:
        queue = InMemoryMessageQueue(args.queue_name, args.max_messages)
    else: 
        queue = PosixMessageQueue(args.queue_name, args.max_messages, args.max_message_size)

    if args.mode == "read":
        while True:
            item = queue.get()
            print(item.decode())
    elif args.mode == "write":
        for line in sys.stdin:
            queue.put(line.rstrip("\n").encode())


if __name__ == "__main__":
    queue_tool()
