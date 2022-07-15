import importlib
import logging

import gevent
import pytest
import requests

from pyramid.config import Configurator
from pyramid.httpexceptions import HTTPNoContent

from baseplate import Baseplate
from baseplate.clients.requests import ExternalRequestsClient
from baseplate.clients.requests import InternalRequestsClient
from baseplate.frameworks.pyramid import BaseplateConfigurator
from baseplate.frameworks.pyramid import StaticTrustHandler
from baseplate.lib import config
from baseplate.server import make_listener
from baseplate.server.wsgi import make_server

from . import TestBaseplateObserver

logger = logging.getLogger(__name__)


@pytest.fixture
def gevent_socket():
    try:
        gevent.monkey.patch_socket()
        yield
    finally:
        import socket

        importlib.reload(socket)
        gevent.monkey.saved.clear()


@pytest.fixture
def http_server(gevent_socket):
    class HttpServer:
        def __init__(self, address):
            self.url = f"http://{address[0]}:{address[1]}/"
            self.requests = []

        def handle_request(self, request):
            self.requests.append(request)
            return HTTPNoContent()

    server_bind_endpoint = config.Endpoint("127.0.0.1:0")
    listener = make_listener(server_bind_endpoint)
    server_address = listener.getsockname()
    http_server = HttpServer(server_address)

    baseplate = Baseplate()
    trust_handler = StaticTrustHandler(trust_headers=True)
    baseplate_configurator = BaseplateConfigurator(baseplate, header_trust_handler=trust_handler)
    configurator = Configurator()
    configurator.include(baseplate_configurator.includeme)
    configurator.add_route("test_view", "/")
    configurator.add_view(http_server.handle_request, route_name="test_view", renderer="json")
    wsgi_app = configurator.make_wsgi_app()

    server = make_server({"stop_timeout": "1 millisecond"}, listener, wsgi_app)
    server_greenlet = gevent.spawn(server.serve_forever)
    try:
        yield http_server
    finally:
        server_greenlet.kill()


@pytest.mark.parametrize("client_cls", [InternalRequestsClient, ExternalRequestsClient])
@pytest.mark.parametrize("client_name", [None, "", "complex.client$name"])
@pytest.mark.parametrize("method", ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "PUT", "POST"])
def test_client_makes_client_span(client_cls, client_name, method, http_server):
    baseplate = Baseplate(
        {"myclient.filter.ip_allowlist": "127.0.0.0/8", "myclient.filter.port_denylist": "0"}
    )
    if client_name is None:
        baseplate.configure_context({"myclient": client_cls()})
    else:
        baseplate.configure_context({"myclient": client_cls(client_name=client_name)})

    observer = TestBaseplateObserver()
    baseplate.register(observer)

    with baseplate.server_context("test") as context:
        fn = getattr(context.myclient, method.lower())
        response = fn(http_server.url)

    assert response.status_code == 204

    server_span_observer = observer.children[0]
    assert len(server_span_observer.children) == 1

    client_span_observer = server_span_observer.children[0]
    assert client_span_observer.span.name == "myclient.request"
    assert client_span_observer.on_start_called
    assert client_span_observer.on_finish_called
    assert client_span_observer.on_finish_exc_info is None
    assert client_span_observer.tags["http.url"] == http_server.url
    assert client_span_observer.tags["http.method"] == method.lower()
    assert client_span_observer.tags["http.status_code"] == 204
    assert client_span_observer.tags["http.slug"] == (
        client_name if client_name is not None else "myclient"
    )


@pytest.mark.parametrize("client_cls", [InternalRequestsClient, ExternalRequestsClient])
def test_connection_error(client_cls):
    baseplate = Baseplate(
        {"myclient.filter.ip_allowlist": "127.0.0.0/8", "myclient.filter.port_denylist": "0"}
    )
    baseplate.configure_context({"myclient": client_cls()})

    observer = TestBaseplateObserver()
    baseplate.register(observer)

    bogus_url = "http://localhost:1/"
    with pytest.raises(requests.exceptions.ConnectionError):
        with baseplate.server_context("test") as context:
            context.myclient.get(bogus_url)

    server_span_observer = observer.children[0]
    assert len(server_span_observer.children) == 1

    client_span_observer = server_span_observer.children[0]
    assert client_span_observer.span.name == "myclient.request"
    assert client_span_observer.on_start_called
    assert client_span_observer.on_finish_called
    assert client_span_observer.on_finish_exc_info is not None
    assert client_span_observer.tags["http.url"] == bogus_url
    assert client_span_observer.tags["http.method"] == "GET".lower()
    assert "http.status_code" not in client_span_observer.tags


def test_internal_client_sends_headers(http_server):
    baseplate = Baseplate()
    baseplate.configure_context({"internal": InternalRequestsClient()})

    with baseplate.server_context("test") as context:
        setattr(context, "raw_edge_context", b"test payload")

        response = context.internal.get(http_server.url)

        assert response.status_code == 204
        assert response.text == ""
        assert http_server.requests[0].method == "GET"
        assert http_server.requests[0].span.trace_id == context.span.trace_id
        assert http_server.requests[0].span.parent_id == context.span.id
        assert http_server.requests[0].span.id != context.span.id
        assert http_server.requests[0].raw_edge_context == b"test payload"


def test_external_client_doesnt_send_headers(http_server):
    baseplate = Baseplate(
        {"external.filter.ip_allowlist": "127.0.0.0/8", "external.filter.port_denylist": "0"}
    )
    baseplate.configure_context({"external": ExternalRequestsClient()})

    with baseplate.server_context("test") as context:
        setattr(context, "raw_edge_context", b"test payload")

        response = context.external.get(http_server.url)

        assert response.status_code == 204
        assert response.text == ""
        assert http_server.requests[0].method == "GET"
        assert "X-Trace" not in http_server.requests[0].headers
        assert "X-Parent" not in http_server.requests[0].headers
        assert "X-Span" not in http_server.requests[0].headers
        assert "X-Edge-Request" not in http_server.requests[0].headers
