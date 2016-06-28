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
from thrift.transport.TTransport import (
    TTransportException, TBufferedTransportFactory)


# pylint: disable=too-many-public-methods
class GeventServer(StreamServer):
    def __init__(self, processor, *args, **kwargs):
        self.processor = processor
        self.transport_factory = TBufferedTransportFactory()
        self.protocol_factory = THeaderProtocolFactory()
        super(GeventServer, self).__init__(*args, **kwargs)

    def serve_forever(self, stop_timeout=None):
        signal.signal(signal.SIGINT, lambda sig, frame: self.stop())
        signal.signal(signal.SIGTERM, lambda sig, frame: self.stop())
        super(GeventServer, self).serve_forever(stop_timeout=stop_timeout)

    # pylint: disable=method-hidden
    def handle(self, client_socket, _):
        client = TSocket()
        client.setHandle(client_socket)

        itrans = self.transport_factory.getTransport(client)
        iprot = self.protocol_factory.getProtocol(itrans)

        otrans = self.transport_factory.getTransport(client)
        oprot = self.protocol_factory.getProtocol(otrans)

        server_context = TRpcConnectionContext(client, iprot, oprot)

        try:
            while self.started:
                self.processor.process(iprot, oprot, server_context)
        except TTransportException:
            pass
        finally:
            itrans.close()
            otrans.close()


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
