"""Shared functions for the event & trace publisher sidecars and message queues."""
import contextlib

from typing import Generator
from typing import Optional

import gevent

from gevent.server import StreamServer

from baseplate.lib import config
from baseplate.lib.message_queue import InMemoryMessageQueue
from baseplate.lib.message_queue import TimedOutError
from baseplate.server import make_listener
from baseplate.server.thrift import make_server
from baseplate.thrift.message_queue import RemoteMessageQueueService
from baseplate.thrift.message_queue.ttypes import PutResponse
from baseplate.thrift.message_queue.ttypes import ThriftTimedOutError


class RemoteMessageQueueHandler:
    """Create an InMemoryMessageQueue locally and expose get/put methods.

    This implementation is a temporary compromise and should only be used
    under very specific circumstances if the POSIX alternative is unavailable.
    Specifically, using Thrift here has significant performance and/or
    resource impacts.

    """

    def __init__(self, queue: InMemoryMessageQueue) -> None:
        # Store the queue by name with its max messages
        self.queue = queue

    def put(self, message: bytes, timeout: Optional[float] = None) -> PutResponse:
        # May raise TimedOutError
        try:
            self.queue.put(message, timeout)
            return PutResponse()
        except TimedOutError:
            raise ThriftTimedOutError


@contextlib.contextmanager
def start_queue_server(
    queue: InMemoryMessageQueue, host: str, port: int
) -> Generator[StreamServer, None, None]:
    # Start a thrift server that will store the queue data in memory
    processor = RemoteMessageQueueService.Processor(RemoteMessageQueueHandler(queue))
    server_bind_endpoint = config.Endpoint(f"{host}:{port}")
    listener = make_listener(server_bind_endpoint)
    server = make_server(server_config={}, listener=listener, app=processor)
    # run the server until our caller is done with it
    server_greenlet = gevent.spawn(server.serve_forever)
    try:
        yield server
    finally:
        server_greenlet.kill()
