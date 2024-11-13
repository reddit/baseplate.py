import datetime
import logging
import socket
from typing import Any, Union

from form_observability import ctx
from gevent.pool import Pool
from gevent.server import StreamServer
from opentelemetry import trace
from opentelemetry.semconv.trace import SpanAttributes
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory
from thrift.Thrift import TProcessor
from thrift.transport.THeaderTransport import THeaderClientType
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransportFactory, TTransportException

from baseplate.lib import config
from baseplate.server import runtime_monitor

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


Address = Union[tuple[str, int], str]


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

        otel_attributes = {
            SpanAttributes.RPC_SYSTEM: "thrift",
            SpanAttributes.RPC_SERVICE: self.processor.baseplate.service_name,
            SpanAttributes.NET_HOST_IP: self.server_host,
            SpanAttributes.NET_HOST_PORT: self.server_port,
        }

        if otel_attributes.get(SpanAttributes.NET_HOST_IP) in ["127.0.0.1", "::1"]:
            otel_attributes[SpanAttributes.NET_HOST_NAME] = "localhost"

        client_addr = None
        client_port = None
        if isinstance(address, str):
            client_addr = address
        elif address is not None:
            client_addr = address[0]
            client_port = address[1]
        if client_addr:
            otel_attributes[SpanAttributes.NET_PEER_IP] = client_addr
            if client_port:
                otel_attributes[SpanAttributes.NET_PEER_PORT] = client_port
            if otel_attributes.get(SpanAttributes.NET_PEER_IP) in ["127.0.0.1", "::1"]:
                otel_attributes[SpanAttributes.NET_PEER_NAME] = "localhost"

        try:
            # set global thrift attributes in this context so that all children
            # traces can just inherit them
            with ctx.set(otel_attributes):
                while self.started:
                    self.processor.process(prot, prot)
        except TTransportException:
            pass
        finally:
            trans.close()


def make_server(server_config: dict[str, str], listener: socket.socket, app: Any) -> StreamServer:
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
        raise ValueError(
            "The max_concurrency setting is not allowed for Thrift servers. See https://github.com/reddit/baseplate.py-upgrader/wiki/v1.2#max_concurrency-is-deprecated."
        )

    pool = Pool()
    server = GeventServer(processor=app, listener=listener, spawn=pool)
    server.stop_timeout = cfg.stop_timeout.total_seconds()

    runtime_monitor.start(server_config, app, pool)
    return server
