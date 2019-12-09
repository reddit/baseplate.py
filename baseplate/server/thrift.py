import datetime
import logging
import socket

from typing import Any
from typing import Dict
from typing import Tuple
from typing import Union

import wrapt

from gevent.pool import Pool
from gevent.server import StreamServer
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory
from thrift.Thrift import TApplicationException
from thrift.Thrift import TMessageType
from thrift.Thrift import TProcessor
from thrift.Thrift import TType
from thrift.transport.THeaderTransport import THeaderClientType
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransportFactory
from thrift.transport.TTransport import TTransportException

from baseplate.lib import config
from baseplate.server import runtime_monitor

logger = logging.getLogger(__name__)

Address = Union[Tuple[str, int], str]


class ReplayIprot(wrapt.ObjectProxy):
    def __init__(self, inner, name, type, seqid):
        super(ReplayIprot, self).__init__(inner)
        self.____name = name
        self.___type = type
        self.___seqid = seqid

    def readMessageBegin(self):
        return (self.____name, self.___type, self.___seqid)


class CircuitBreakingProcessor(TProcessor):
    def __init__(self, inner, max_concurrency, *args: Any, **kwargs: Any):
        self.max_concurrency = max_concurrency
        self.current_requests = 0
        self.inner = inner

    def process(self, iprot, oprot):
        # wait for the first byte before declaring the message in-progress
        (name, type, seqid) = iprot.readMessageBegin()

        if self.current_requests < self.max_concurrency:
            # I'd use atomics, but the GIL takes care of this
            self.current_requests += 1
            try:
                # now we'll have to replay the first byte, so we have this wrapper
                wrapped = ReplayIprot(iprot, name, type, seqid)

                # do the real work
                self.inner.process(wrapped, oprot)
            finally:
                self.current_requests -= 1
        else:
            iprot.skip(TType.STRUCT)
            iprot.readMessageEnd()
            x = TApplicationException(TApplicationException.INTERNAL_ERROR, "slow down, tiger")
            oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.trans.flush()
            return


# pylint: disable=too-many-public-methods
class GeventServer(StreamServer):
    def __init__(self, processor: TProcessor, max_concurrency: int, *args: Any, **kwargs: Any):
        self.processor = processor
        if max_concurrency:
            self.processor = CircuitBreakingProcessor(self.processor, max_concurrency)
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

    pool = Pool(size=None)
    server = GeventServer(
        processor=app, listener=listener, spawn=pool, max_concurrency=cfg.max_concurrency
    )
    server.stop_timeout = cfg.stop_timeout.total_seconds()

    runtime_monitor.start(server_config, app, pool)
    return server
