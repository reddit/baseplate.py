"""A message queue, with three implementations: POSIX-based, in-memory, or remote using a Thrift server."""
import abc
import queue as q
import select
import time

from enum import Enum
from typing import Any
from typing import Optional

import gevent
import posix_ipc

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from baseplate.lib import config
from baseplate.lib.prometheus_metrics import default_latency_buckets
from baseplate.lib.retry import RetryPolicy
from baseplate.lib.thrift_pool import ThriftConnectionPool
from baseplate.thrift.message_queue import RemoteMessageQueueService
from baseplate.thrift.message_queue.ttypes import ThriftTimedOutError

DEFAULT_QUEUE_HOST = "127.0.0.1"
DEFAULT_QUEUE_PORT = 9090
PROM_PREFIX = "message_queue"


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


class QueueType(Enum):
    IN_MEMORY = "in_memory"
    POSIX = "posix"


class MessageQueue(abc.ABC):
    """Abstract class for an inter-process message queue."""

    name: str

    @abc.abstractmethod
    def get(self, timeout: Optional[float] = None) -> bytes:
        """Read a message from the queue.

        :param timeout: If the queue is empty, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was empty for the allowed
            duration of the call.

        """

    @abc.abstractmethod
    def put(self, message: bytes, timeout: Optional[float] = None) -> None:
        """Add a message to the queue.

        :param timeout: If the queue is full, the call will block up to
            ``timeout`` seconds or forever if ``None``.
        :raises: :py:exc:`TimedOutError` The queue was full for the allowed
            duration of the call.

        """


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
        super().__init__()
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
        self.name = name

    def get(self, timeout: Optional[float] = None) -> bytes:
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


class InMemoryMessageQueue(MessageQueue):
    """An in-memory inter process message queue.

    Uses a simple Python Queue to store data.

    """

    def __init__(self, max_messages: int):
        self.queue: q.Queue = q.Queue(max_messages)
        self.max_messages = max_messages

    def get(self, timeout: Optional[float] = None) -> bytes:
        try:
            message = self.queue.get(timeout=timeout)
            self.queue.task_done()
            return message
        except q.Empty:
            raise TimedOutError

    def put(self, message: bytes, timeout: Optional[float] = None) -> None:
        try:
            self.queue.put(message, timeout=timeout)
        except q.Full:
            raise TimedOutError


class RemoteMessageQueue(MessageQueue):
    """A write-only message queue that uses a remote Thrift server.

    This implementation is a temporary compromise and should only be used
    under very specific circumstances if the POSIX alternative is unavailable.
    Specifically, using Thrift here may have significant performance and/or
    resource impacts.

    """

    prom_labels = [
        "queue_name",
        "queue_host",
        "queue_port",
        "queue_max_messages",
    ]

    remote_queue_put_requests_queued = Gauge(
        f"{PROM_PREFIX}_pending_puts_to_sidecar",
        "total number of queue requests in flight, being sent to the publishing sidecar.",
        prom_labels,
        multiprocess_mode="livesum",
    )

    remote_queue_put_requests_success = Counter(
        f"{PROM_PREFIX}_sidecar_put_success",
        "successful queue requests sent to the publishing sidecar.",
        prom_labels,
    )

    remote_queue_put_requests_fail = Counter(
        f"{PROM_PREFIX}_sidecar_put_fail",
        "failed queue requests sent to the publishing sidecar.",
        prom_labels,
    )

    remote_queue_put_request_latency = Histogram(
        f"{PROM_PREFIX}_sidecar_latency",
        "latency of message requests to the publishing sidecar.",
        prom_labels,
        buckets=default_latency_buckets,
    )

    def __init__(
        self,
        name: str,
        max_messages: int,
        host: str = DEFAULT_QUEUE_HOST,
        port: int = DEFAULT_QUEUE_PORT,
        pool_size: int = 10,
        pool_timeout: int = 1,
        pool_conn_max_age: int = 120,
    ):
        # Connect to the remote queue server, and creeate the new queue
        self.name = name
        self.max_messages = max_messages
        self.host = host
        self.port = port
        self.pool = self.create_connection_pool(pool_size, pool_timeout, pool_conn_max_age)
        with self.pool.connection() as protocol:
            client = RemoteMessageQueueService.Client(protocol)
            client.create_queue(name, max_messages)

    def create_connection_pool(
        self, pool_size: int, pool_timeout: int, pool_conn_max_age: int
    ) -> ThriftConnectionPool:
        endpoint = config.Endpoint(f"{self.host}:{self.port}")
        pool = ThriftConnectionPool(
            endpoint, size=pool_size, timeout=pool_timeout, max_age=pool_conn_max_age
        )
        return pool

    def _update_counters(self, outcome: str) -> None:
        # This request is no longer queued
        self.remote_queue_put_requests_queued.labels(
            queue_name=self.name,
            queue_host=self.host,
            queue_port=self.port,
            queue_max_messages=self.max_messages,
        ).dec()
        # Increment success/failure counters
        if outcome == "success":
            metric = self.remote_queue_put_requests_success
        else:
            metric = self.remote_queue_put_requests_fail
        metric.labels(
            queue_name=self.name,
            queue_host=self.host,
            queue_port=self.port,
            queue_max_messages=self.max_messages,
        ).inc()

    def _put_success_callback(self, greenlet: Any) -> None:
        self._update_counters("success")
        gevent.joinall([greenlet])

    def _put_fail_callback(self, greenlet: Any) -> None:
        self._update_counters("fail")
        gevent.joinall([greenlet])
        try:
            greenlet.get()
        except Exception as e:
            print("Remote queue `put` failed, exception found: ", e)

    def get(self, _: Optional[float] = None) -> bytes:
        raise NotImplementedError  # This queue type is write-only

    def _try_to_put(self, message: bytes, timeout: Optional[float], start_time: float) -> bool:
        # get a connection from the pool
        with self.pool.connection() as protocol:
            client = RemoteMessageQueueService.Client(protocol)
            try:
                client.put(self.name, message, timeout)

                # record latency
                self.remote_queue_put_request_latency.labels(
                    queue_name=self.name,
                    queue_host=self.host,
                    queue_port=self.port,
                    queue_max_messages=self.max_messages,
                ).observe(time.perf_counter() - start_time)
                return True  # Success
            # If the server responded with a timeout, raise our own timeout to be consistent with the posix queue
            except ThriftTimedOutError:
                raise TimedOutError

    def put(self, message: bytes, timeout: Optional[float] = None) -> Any:
        # increment in-flight counter
        self.remote_queue_put_requests_queued.labels(
            queue_name=self.name,
            queue_host=self.host,
            queue_port=self.port,
            queue_max_messages=self.max_messages,
        ).inc()
        start_time = time.perf_counter()

        greenlet = gevent.spawn(self._try_to_put, message, timeout, start_time)
        greenlet.link_value(self._put_success_callback)
        greenlet.link_exception(self._put_fail_callback)
        return greenlet


def create_queue(
    queue_type: QueueType,
    queue_full_name: str,
    max_queue_size: int,
    max_element_size: int,
) -> MessageQueue:
    # The in-memory queue is created on the sidecar, and the main baseplate
    # application will use a remote queue to interact with it.
    if queue_type == QueueType.IN_MEMORY:
        event_queue = InMemoryMessageQueue(max_queue_size)

    else:
        event_queue = PosixMessageQueue(  # type: ignore
            queue_full_name,
            max_messages=max_queue_size,
            max_message_size=max_element_size,
        )

    return event_queue


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
    parser.add_argument(
        "--queue-type",
        default=QueueType.POSIX.value,
        choices=[qt.value for qt in QueueType],
        help="allows selection of the queue implementation",
    )
    parser.add_argument(
        "--queue-host",
        type=str,
        default=DEFAULT_QUEUE_HOST,
        help="for a remote queue, what host to use",
    )
    parser.add_argument(
        "--queue-port",
        type=int,
        default=DEFAULT_QUEUE_PORT,
        help="for a remote queue, what port to use",
    )

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

    queue = create_queue(
        args.queue_type,
        args.queue_name,
        args.max_messages,
        args.max_message_size,
    )

    if args.mode == "read":
        while True:
            item = queue.get()
            print(item.decode())
    elif args.mode == "write":
        for line in sys.stdin:
            queue.put(line.rstrip("\n").encode())


if __name__ == "__main__":
    queue_tool()
