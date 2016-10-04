from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import signal

from gevent.pool import Pool
from gevent.server import StreamServer
from thrift.protocol.THeaderProtocol import THeaderProtocolFactory
from thrift.server.TServer import TRpcConnectionContext
from thrift.transport.TSocket import TSocket
from thrift.transport.THeaderTransport import THeaderTransport
from thrift.transport.TTransport import (
    TTransportException, TBufferedTransportFactory)


# pylint: disable=too-many-public-methods
class GeventServer(StreamServer):
    def __init__(self, processor, *args, **kwargs):
        self.processor = processor
        self.transport_factory = TBufferedTransportFactory()
        self.protocol_factory = THeaderProtocolFactory(
            # allow non-headerprotocol clients to talk with us
            client_types=[
                THeaderTransport.HEADERS_CLIENT_TYPE,
                THeaderTransport.FRAMED_DEPRECATED,
                THeaderTransport.UNFRAMED_DEPRECATED,
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

        server_context = TRpcConnectionContext(client, prot, prot)

        try:
            while self.started:
                self.processor.process(prot, prot, server_context)
        except TTransportException:
            pass
        finally:
            trans.close()


def make_server(config, listener, app):
    max_concurrency = int(config.get("max_concurrency", 0)) or None
    stop_timeout = int(config.get("stop_timeout", 0))

    pool = Pool(size=max_concurrency)
    server = GeventServer(
        processor=app,
        listener=listener,
        spawn=pool,
    )
    server.stop_timeout = stop_timeout
    return server
