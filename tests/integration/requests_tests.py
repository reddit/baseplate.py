from __future__ import annotations

import contextlib
import dataclasses
import importlib
import logging
import time
import urllib.parse

import gevent
import pytest
import requests
import urllib3.connection
from gevent.pywsgi import WSGIServer
from pyramid.config import Configurator
from pyramid.httpexceptions import HTTPNoContent

from baseplate import Baseplate
from baseplate.clients.requests import ExternalRequestsClient, InternalRequestsClient
from baseplate.frameworks.pyramid import BaseplateConfigurator, StaticTrustHandler
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
        server: WSGIServer

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

    http_server.server = make_server({"stop_timeout": "1 millisecond"}, listener, wsgi_app)
    server_greenlet = gevent.spawn(http_server.server.serve_forever)
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


def test_internal_client_sends_headers_with_none_edge_context(http_server):
    baseplate = Baseplate()
    baseplate.configure_context({"internal": InternalRequestsClient()})

    with baseplate.server_context("test") as context:
        setattr(context, "raw_edge_context", None)
        response = context.internal.get(http_server.url)

        assert response.status_code == 204
        assert response.text == ""
        assert http_server.requests[0].method == "GET"
        assert http_server.requests[0].span.trace_id == context.span.trace_id
        assert http_server.requests[0].span.parent_id == context.span.id
        assert http_server.requests[0].span.id != context.span.id
        assert http_server.requests[0].raw_edge_context is None


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


def _is_connected(conn: urllib3.connection.HTTPConnection) -> bool:
    """Backport of urllib3.connection.HTTPConnection.is_connected().

    Based on urllib3 v2.2.3:
    https://github.com/urllib3/urllib3/blob/f9d37add7983d441b151146db447318dff4186c9/src/urllib3/connection.py#L299
    """
    if conn.sock is None:
        return False
    return not urllib3.util.wait_for_read(conn.sock, timeout=0.0)


@dataclasses.dataclass
class KeepaliveClientResult:
    requests_completed: int = 0
    connection_closed_time: float | None = None


def _keepalive_client(
    url: str, ready_event: gevent.event.Event, wait_time: float
) -> KeepaliveClientResult:
    """HTTP client that makes requests forever over a single keepalive connection.

    Returns iff the connection is closed. Otherwise, it must be killed.
    """
    parsed = urllib.parse.urlparse(url)
    with contextlib.closing(
        urllib3.connection.HTTPConnection(parsed.hostname, parsed.port, timeout=1),
    ) as conn:
        ret = KeepaliveClientResult()
        conn.connect()
        ready_event.set()

        last_request_time = None
        while True:
            if not _is_connected(conn):
                print("Client lost connection to server, stopping request loop.")
                ret.connection_closed_time = time.time()
                break

            if last_request_time is None or time.time() - last_request_time >= wait_time:
                print("Client making request.")
                last_request_time = time.time()
                conn.request("GET", "/")
                response = conn.getresponse()
                response.close()

                assert response.status == 204
                print("Client got expected response.")
                ret.requests_completed += 1

            # Sleeping for a short time rather than the full `wait_time` so we
            # can notice if the connection closes.
            gevent.sleep(0.01)

        return ret


@pytest.mark.parametrize(
    (
        "delay_between_requests",
        "min_expected_successful_requests",
        "max_expected_successful_requests",
    ),
    (
        # Client that sends a request every 0.1 seconds.
        (
            0.1,
            # ~10 requests in 1 second.
            5,
            15,
        ),
        # Client that sends one request then sleeps forever while keeping the
        # connection open.
        #
        # This is used to test that the server closes keepalive connections
        # even if they remain idle for the entire shutdown period.
        (
            999999999,
            # The client should make exactly one request.
            1,
            1,
        ),
    ),
)
def test_shutdown_closes_existing_keepalive_connection(
    http_server,
    delay_between_requests,
    min_expected_successful_requests,
    max_expected_successful_requests,
):
    """Ensure that the server closes keepalive connections when shutting down.

    By default, calling `stop()` on a gevent WSGIServer prevents new
    connections but does not close existing ones. This allows clients to
    continue sending new requests over existing connections right up until the
    server's stop_timeout, resulting in slow shutdown and connections being
    killed mid-flight, which causes user-facing errors.

    We work around this by subclassing WSGIHandler and (a) disabling keepalive
    when the server is in shutdown, and (b) closing existing idle connections
    when the server enters shutdown.
    """
    http_server.server.stop_timeout = 10

    ready_event = gevent.event.Event()
    client_greenlet = gevent.spawn(
        _keepalive_client,
        http_server.url,
        ready_event,
        delay_between_requests,
    )
    try:
        print("Waiting for client to connect...")
        ready_event.wait()

        print("Client connected, now waiting while it makes requests.")
        gevent.sleep(1)

        print("Triggering server shutdown...")
        shutdown_start = time.time()
        http_server.server.stop()
    finally:
        # Server usually exits before the client notices the connection closed,
        # so give it a second to finish.
        client_greenlet.join(timeout=5)

    print(f"Shutdown completed after {time.time() - shutdown_start:.1f}s.")

    ret = client_greenlet.get()
    if isinstance(ret, BaseException):
        # This usually happens with GreenletExit.
        raise ret

    print("Requests completed:", ret.requests_completed)
    connection_closed_delay = ret.connection_closed_time - shutdown_start
    print("Connection closed delay:", connection_closed_delay)

    assert (
        min_expected_successful_requests
        <= ret.requests_completed
        <= max_expected_successful_requests
    )

    # connection_closed_time should be within ~2 seconds after the shutdown
    # start time, but not before it.
    assert 0 <= connection_closed_delay <= 2
