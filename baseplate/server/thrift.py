from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import signal

from gevent.pool import Pool
from gevent.server import StreamServer
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory
from thrift.transport.TSocket import TSocket
from thrift.transport.THeaderTransport import THeaderClientType
from thrift.transport.TTransport import (
    TTransportException, TBufferedTransportFactory)

from baseplate import config
from baseplate.server import runtime_monitor


# pylint: disable=too-many-public-methods
class GeventServer(StreamServer):
    def __init__(self, processor, *args, **kwargs):
        self.processor = processor
        self.transport_factory = TBufferedTransportFactory()
        self.protocol_factory = THeaderProtocolFactory(
            # allow non-headerprotocol clients to talk with us
            allowed_client_types=[
                THeaderClientType.HEADERS,
                THeaderClientType.FRAMED_BINARY,
                THeaderClientType.UNFRAMED_BINARY,
            ],
        )
        super(GeventServer, self).__init__(*args, **kwargs)

    def serve_forever(self, stop_timeout=None):
        signal.signal(signal.SIGINT, lambda sig, frame: self.stop())
        signal.signal(signal.SIGTERM, lambda sig, frame: self.stop())
        super(GeventServer, self).serve_forever(stop_timeout=stop_timeout)

    # pylint: disable=method-hidden
    def handle(self, client_socket, _):
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


def make_server(server_config, listener, app):
    # pylint: disable=maybe-no-member
    cfg = config.parse_config(server_config, {
        "max_concurrency": config.Integer,
        "stop_timeout": config.Optional(config.Integer, default=0),
    })

    pool = Pool(size=cfg.max_concurrency)
    server = GeventServer(
        processor=app,
        listener=listener,
        spawn=pool,
    )
    server.stop_timeout = cfg.stop_timeout

    runtime_monitor.start(server_config, app, pool)
    return server
