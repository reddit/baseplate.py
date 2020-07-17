import contextlib
import logging
import unittest

from unittest import mock

import gevent.monkey

from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate import ServerSpanObserver
from baseplate import SpanObserver
from baseplate import TraceInfo
from baseplate.clients.thrift import ThriftClient
from baseplate.frameworks.thrift import baseplateify_processor
from baseplate.lib import config
from baseplate.lib.edge_context import EdgeRequestContextFactory
from baseplate.lib.file_watcher import FileWatcher
from baseplate.lib.secrets import SecretsStore
from baseplate.lib.thrift_pool import ThriftConnectionPool
from baseplate.server import make_listener
from baseplate.server.thrift import make_server
from baseplate.thrift import BaseplateService
from baseplate.thrift import BaseplateServiceV2
from baseplate.thrift.ttypes import IsHealthyProbe
from baseplate.thrift.ttypes import IsHealthyRequest

from .. import AUTH_TOKEN_PUBLIC_KEY
from .. import SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH


cryptography_installed = True
try:
    import cryptography
except ImportError:
    cryptography_installed = False
else:
    del cryptography


try:
    from importlib import reload
except ImportError:
    pass


def make_edge_context_factory():
    mock_filewatcher = mock.Mock(spec=FileWatcher)
    mock_filewatcher.get_data.return_value = {
        "secrets": {
            "secret/authentication/public-key": {
                "type": "versioned",
                "current": AUTH_TOKEN_PUBLIC_KEY,
            }
        },
        "vault": {"token": "test", "url": "http://vault.example.com:8200/"},
    }
    secrets = SecretsStore("/secrets")
    secrets._filewatcher = mock_filewatcher
    return EdgeRequestContextFactory(secrets)


@contextlib.contextmanager
def serve_thrift(handler, server_spec, server_span_observer=None):
    # create baseplate root
    baseplate = Baseplate()
    if server_span_observer:

        class TestBaseplateObserver(BaseplateObserver):
            def on_server_span_created(self, context, server_span):
                server_span.register(server_span_observer)

        baseplate.register(TestBaseplateObserver())

    # set up the server's processor
    logger = mock.Mock(spec=logging.Logger)
    edge_context_factory = make_edge_context_factory()
    processor = server_spec.Processor(handler)
    processor = baseplateify_processor(processor, logger, baseplate, edge_context_factory)

    # bind a server socket on an available port
    server_bind_endpoint = config.Endpoint("127.0.0.1:0")
    listener = make_listener(server_bind_endpoint)
    server = make_server(
        {"max_concurrency": "100", "stop_timeout": "1 millisecond"}, listener, processor
    )

    # figure out what port the server ended up on
    server_address = listener.getsockname()
    server.endpoint = config.Endpoint(f"{server_address[0]}:{server_address[1]}")

    # run the server until our caller is done with it
    server_greenlet = gevent.spawn(server.serve_forever)
    try:
        yield server
    finally:
        server_greenlet.kill()


@contextlib.contextmanager
def raw_thrift_client(endpoint):
    pool = ThriftConnectionPool(endpoint)
    with pool.connection() as client_protocol:
        yield BaseplateServiceV2.Client(client_protocol)


@contextlib.contextmanager
def baseplate_thrift_client(endpoint, client_span_observer=None):
    baseplate = Baseplate(
        app_config={
            "baseplate.service_name": "fancy test client",
            "example_service.endpoint": str(endpoint),
        }
    )

    if client_span_observer:

        class TestServerSpanObserver(ServerSpanObserver):
            def on_child_span_created(self, span):
                span.register(client_span_observer)

        observer = TestServerSpanObserver()

        class TestBaseplateObserver(BaseplateObserver):
            def on_server_span_created(self, context, span):
                span.register(observer)

        baseplate.register(TestBaseplateObserver())

    context = baseplate.make_context_object()
    trace_info = TraceInfo.from_upstream(
        trace_id=1234, parent_id=2345, span_id=3456, flags=4567, sampled=True
    )

    baseplate.configure_context({"example_service": ThriftClient(BaseplateServiceV2.Client)})

    baseplate.make_server_span(context, "example_service.example", trace_info)

    edge_context_factory = make_edge_context_factory()
    edge_context = edge_context_factory.from_upstream(SERIALIZED_EDGECONTEXT_WITH_VALID_AUTH)
    edge_context.attach_context(context)

    yield context


class GeventPatchedTestCase(unittest.TestCase):
    def setUp(self):
        gevent.monkey.patch_socket()

    def tearDown(self):
        import socket

        reload(socket)


class ThriftHealthcheck(GeventPatchedTestCase):
    def test_v2_client_v1_server(self):
        class Handler(BaseplateService.Iface):
            def is_healthy(self, context):
                return True

        handler = Handler()

        span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, server_spec=BaseplateService) as server:
            with baseplate_thrift_client(server.endpoint, span_observer) as context:
                healthy = context.example_service.is_healthy(
                    request=IsHealthyRequest(probe=IsHealthyProbe.READINESS),
                )
                self.assertTrue(healthy)

    def test_v2_client_v2_server(self):
        class Handler(BaseplateServiceV2.Iface):
            def __init__(self):
                self.probe = None

            def is_healthy(self, context, req=None):
                self.probe = req.probe
                return True

        handler = Handler()

        span_observer = mock.Mock(spec=SpanObserver)
        with serve_thrift(handler, server_spec=BaseplateServiceV2) as server:
            with baseplate_thrift_client(server.endpoint, span_observer) as context:
                healthy = context.example_service.is_healthy(
                    request=IsHealthyRequest(probe=IsHealthyProbe.LIVENESS),
                )
                self.assertTrue(healthy)
                self.assertEqual(handler.probe, IsHealthyProbe.LIVENESS)
