import contextlib

from typing import Dict
from typing import Generator
from typing import Optional

import gevent

from gevent.server import StreamServer

from baseplate.lib import config
from baseplate.lib.message_queue import InMemoryMessageQueue
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import PosixMessageQueue
from baseplate.lib.message_queue import QueueType
from baseplate.lib.message_queue import RemoteMessageQueue
from baseplate.lib.message_queue import TimedOutError
from baseplate.server import make_listener
from baseplate.server.thrift import make_server
from baseplate.thrift.message_queue import RemoteMessageQueueService
from baseplate.thrift.message_queue.ttypes import CreateResponse
from baseplate.thrift.message_queue.ttypes import GetResponse
from baseplate.thrift.message_queue.ttypes import PutResponse
from baseplate.thrift.message_queue.ttypes import ThriftTimedOutError


class RemoteMessageQueueHandler:  # On the queue server, create the queue and define get/put using the InMemoryQueue implementation
    def is_healthy(self) -> bool:
        pass

    def __init__(self) -> None:
        # Store the queue by name with its max messages
        self.queues: Dict[str, MessageQueue] = {}

    def create_queue(self, queue_name: str, max_messages: int) -> CreateResponse:
        queue = InMemoryMessageQueue(queue_name, max_messages)
        self.queues[queue_name] = queue

        return CreateResponse()

    def get(self, queue_name: str, timeout: Optional[float] = None) -> GetResponse:
        try:
            queue = self.queues[queue_name]
            # Get element from list, waiting if necessary
            result = queue.get(timeout)
        # If the queue timed out, raise a timeout as the server response
        except TimedOutError as e:
            raise ThriftTimedOutError from e

        return GetResponse(result)

    def put(self, queue_name: str, message: bytes, timeout: Optional[float] = None) -> PutResponse:
        try:
            queue = self.queues[queue_name]
            queue.put(message, timeout)
        except TimedOutError as e:
            raise ThriftTimedOutError from e
        return PutResponse()


@contextlib.contextmanager
def start_queue_server(host: str, port: int) -> Generator[StreamServer, None, None]:
    # Start a thrift server that will house the remote queue data
    processor = RemoteMessageQueueService.Processor(RemoteMessageQueueHandler())
    server_bind_endpoint = config.Endpoint(f"{host}:{port}")
    listener = make_listener(server_bind_endpoint)
    server = make_server(server_config={}, listener=listener, app=processor)

    # figure out what port the server ended up on
    server_address = listener.getsockname()
    server.endpoint = config.Endpoint(f"{server_address[0]}:{server_address[1]}")
    # run the server until our caller is done with it
    server_greenlet = gevent.spawn(server.serve_forever)
    try:
        yield server
    finally:
        server_greenlet.kill()


def create_queue(
    queue_type: QueueType,
    queue_name: str,
    max_queue_size: int,
    max_element_size: int,
    host: str = "127.0.0.1",
    port: int = 9090,
) -> MessageQueue:
    if queue_type == QueueType.IN_MEMORY:
        event_queue = RemoteMessageQueue("/events-" + queue_name, max_queue_size, host, port)

    else:
        event_queue = PosixMessageQueue(  # type: ignore
            "/events-" + queue_name,
            max_messages=max_queue_size,
            max_message_size=max_element_size,
        )

    return event_queue
