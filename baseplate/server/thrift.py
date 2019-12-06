import datetime
import logging
import socket

from typing import Any
from typing import Dict
from typing import Tuple
from typing import Union

from gevent.pool import Pool
from gevent.server import StreamServer
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory
from thrift.Thrift import TProcessor
from thrift.transport.THeaderTransport import THeaderClientType
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransportFactory
from thrift.transport.TTransport import TTransportException

from baseplate.lib import config
from baseplate.server import runtime_monitor


logger = logging.getLogger(__name__)


Address = Union[Tuple[str, int], str]


# pylint: disable=too-many-public-methods
class GeventServer(StreamServer):
    def __init__(self, processor: TProcessor, *args: Any, **kwargs: Any):
        self.processor = processor
        self.transport_factory = TBufferedTransportFactory()
        self.protocol_factory = THeaderProtocolFactory(
            # allow non-headerprotocol clients to talk with us
            allowed_client_types=[
                THeaderClientType.HEADERS,
                THeaderClientType.FRAMED_BINARY,
                THeaderClientType.UNFRAMED_BINARY,
            ]
        )
        super().__init__(*args, **kwargs)

    # pylint: disable=method-hidden,unused-argument
    def handle(self, client_socket: socket.socket, address: Address) -> None:
        client = TSocket()
        client.setHandle(client_socket)

        trans = self.transport_factory.getTransport(client)
        prot = self.protocol_factory.getProtocol(trans)

        try:
            while self.started:
                self.processor.process(prot, prot)
        except TTransportException:
            pass
        finally:
            trans.close()


def make_server(server_config: Dict[str, str], listener: socket.socket, app: Any) -> StreamServer:
    # pylint: disable=maybe-no-member
    cfg = config.parse_config(
        server_config,
        {
            "max_concurrency": config.Optional(config.Integer),
            "stop_timeout": config.Optional(
                config.TimespanWithLegacyFallback, default=datetime.timedelta(seconds=10)
            ),
        },
    )

    if cfg.max_concurrency is not None:
        logger.warning(
            "The max_concurrency setting is deprecated for Thrift servers. See https://git.io/Jeywc."
        )

    pool = Pool(size=cfg.max_concurrency)
    server = GeventServer(processor=app, listener=listener, spawn=pool)
    server.stop_timeout = cfg.stop_timeout.total_seconds()

    runtime_monitor.start(server_config, app, pool)
    return server
