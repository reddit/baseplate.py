import contextlib

from typing import Optional

import gevent

from baseplate.lib import config
from baseplate.lib.message_queue import InMemoryMessageQueue
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import PosixMessageQueue
from baseplate.lib.message_queue import RemoteMessageQueue
from baseplate.lib.message_queue import TimedOutError as MessageQueueTimedOutError
from baseplate.server import make_listener
from baseplate.server import make_server
from baseplate.thrift.message_queue import RemoteMessageQueueService
from baseplate.thrift.message_queue.ttypes import GetResponse
from baseplate.thrift.message_queue.ttypes import PutResponse
from baseplate.thrift.message_queue.ttypes import TimedOutError as ThriftTimedOutError

class RemoteMessageQueueHandler:  # On the queue server, create the queue and define get/put using the InMemoryQueue implementation
    def is_healthy(self) -> bool:
        pass

    def __init__(self):
        self.queue_list = {}

    def get(
        self, queue_name: str, max_messages: int, timeout: Optional[float] = None
    ) -> GetResponse:
        try:
            # Create queue if doesnt exist
            # We need to create the queue on both get & put - if get() is called on a queue before it exists, we
            # still want to wait the appropriate timeout in case anyone puts elements in it
            queue = self.queue_list.get(queue_name)
            if not queue:
                queue = InMemoryMessageQueue(queue_name, max_messages)
                self.queue_list[queue_name] = queue
            # Get element from list, waiting if necessary
            result = queue.get(timeout)
        except MessageQueueTimedOutError:
            raise ThriftTimedOutError()

        return GetResponse(result)

    def put(
        self, queue_name: str, max_messages: int, message: bytes, timeout: Optional[float] = None
    ) -> PutResponse:
        # Create queue if it does not exist yet
        try:
            queue = self.queue_list.get(queue_name)
            if not self.queue_list.get(queue_name):
                queue = InMemoryMessageQueue(queue_name, max_messages)
                self.queue_list[queue_name] = queue
            queue.put(message, timeout)
        except MessageQueueTimedOutError:
            raise ThriftTimedOutError()
        return PutResponse()


@contextlib.contextmanager
def start_queue_server(host: str, port: int) -> None:
    # Start a thrift server that will house the remote queue data
    processor = RemoteMessageQueueService.Processor(RemoteMessageQueueHandler())
    server_bind_endpoint = config.Endpoint(f"{host}:{port}")
    listener = make_listener(server_bind_endpoint)
    server = make_server(server_config={}, listener=listener, app=processor)

    # figure out what port the server ended up on
    server_address = listener.getsockname()
    server.endpoint = config.Endpoint(f"{server_address[0]}:{server_address[1]}")
    print(server_address)  # localhost 9090
    # run the server until our caller is done with it
    server_greenlet = gevent.spawn(server.serve_forever)
    try:
        yield server
    finally:
        server_greenlet.kill()


def create_queue(
    queue_type: str, queue_name: str, max_queue_size: int, max_element_size: int
) -> MessageQueue:
    if queue_type == "in_memory":
        with start_queue_server(host="127.0.0.1", port=9090):
            event_queue = RemoteMessageQueue(  # type: ignore
                "/events-" + queue_name, max_queue_size
            )

    else:
        event_queue = PosixMessageQueue(  # type: ignore
            "/events-" + queue_name,
            max_messages=max_queue_size,
            max_message_size=max_element_size,
        )

    return event_queue
