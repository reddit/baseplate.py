import importlib
import logging

import gevent
import pytest
import requests

from prometheus_client import REGISTRY
from pyramid.config import Configurator
from pyramid.httpexceptions import HTTPNoContent

from baseplate import Baseplate
from baseplate.clients.requests import ExternalRequestsClient
from baseplate.clients.requests import InternalRequestsClient
from baseplate.frameworks.pyramid import BaseplateConfigurator
from baseplate.frameworks.pyramid import StaticTrustHandler
from baseplate.lib import config
from baseplate.observers.prometheus import PrometheusBaseplateObserver
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
    assert client_span_observer.tags["http.method"] == method
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
    assert client_span_observer.tags["http.method"] == "GET"
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


@pytest.mark.parametrize("client_cls", [InternalRequestsClient, ExternalRequestsClient])
@pytest.mark.parametrize("client_name", [None, "", "complex.client$name"])
@pytest.mark.parametrize("method", ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "PUT", "POST"])
def test_prometheus_http_client_metrics(client_cls, client_name, method, http_server):
    baseplate = Baseplate(
        {"myclient.filter.ip_allowlist": "127.0.0.0/8", "myclient.filter.port_denylist": "0"}
    )
    logger.info("Created baseplate.")
    if client_name is None:
        baseplate.configure_context({"myclient": client_cls()})
    else:
        baseplate.configure_context({"myclient": client_cls(client_name=client_name)})
    logger.info(
        f"Configured baseplate context with requests client. [type={client_cls.__name__}, client_name={client_name}]"
    )

    observer = PrometheusBaseplateObserver()
    logger.info(f"Created observer. [type={observer.__class__.__name__}]")
    baseplate.register(observer)
    logger.info("Registered observer with baseplate.")

    # we need to clear metrics between test runs from the parametrize annotations
    REGISTRY._names_to_collectors["http_client_requests_total"].clear()
    REGISTRY._names_to_collectors["http_client_latency_seconds"].clear()
    REGISTRY._names_to_collectors["http_client_active_requests"].clear()

    with baseplate.server_context("test") as context:
        logger.debug("Created server context.")
        fn = getattr(context.myclient, method.lower())
        logger.debug(f"Making HTTP call. [method={method}, endpoint={http_server.url}]")
        response = fn(http_server.url)
        logger.debug(f"Made HTTP call. [method={method}, endpoint={http_server.url}]")

    assert response.status_code == 204

    http_client_requests_total = REGISTRY._names_to_collectors[
        "http_client_requests_total"
    ].collect()
    assert len(http_client_requests_total) == 1
    assert len(http_client_requests_total[0].samples) == 2  # _total and _created
    http_client_requests_total_total = http_client_requests_total[0].samples[0]
    assert http_client_requests_total_total.name == "http_client_requests_total"
    assert http_client_requests_total_total.labels.get("http_method") == method
    assert http_client_requests_total_total.labels.get("http_response_code") == "204"
    assert http_client_requests_total_total.labels.get("http_success") == "true"
    assert http_client_requests_total_total.labels.get("http_slug") == (
        client_name if client_name is not None else "myclient"
    )
    assert http_client_requests_total_total.value == 1

    http_client_latency_seconds = REGISTRY._names_to_collectors[
        "http_client_latency_seconds"
    ].collect()
    assert len(http_client_latency_seconds) == 1
    assert (
        len(http_client_latency_seconds[0].samples) == 18
    )  # 15 time buckets (inc. inf), _count, _sum and _created
    http_client_latency_seconds_inf = http_client_latency_seconds[0].samples[14]
    assert http_client_latency_seconds_inf.labels.get("le") == "+Inf"
    assert http_client_latency_seconds_inf.labels.get("http_method") == method
    assert http_client_latency_seconds_inf.labels.get("http_success") == "true"
    assert http_client_latency_seconds_inf.labels.get("http_slug") == (
        client_name if client_name is not None else "myclient"
    )
    assert http_client_latency_seconds_inf.value == 1
    http_client_latency_seconds_count = http_client_latency_seconds[0].samples[
        15
    ]  # no need to test the tags again since they all in the same sample
    assert http_client_latency_seconds_count.value == 1

    http_client_active_requests = REGISTRY._names_to_collectors[
        "http_client_active_requests"
    ].collect()
    assert len(http_client_active_requests) == 1
    assert len(http_client_active_requests[0].samples) == 1
    http_client_active_requests_samples = http_client_active_requests[0].samples[0]
    assert http_client_active_requests_samples.labels.get("http_method") == method
    assert http_client_active_requests_samples.labels.get("http_slug") == (
        client_name if client_name is not None else "myclient"
    )
    assert http_client_active_requests_samples.value == 0


@pytest.mark.parametrize("method", ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "PUT", "POST"])
def test_prometheus_http_client_metrics_inside_local_span(method, http_server):
    baseplate = Baseplate(
        {"myclient.filter.ip_allowlist": "127.0.0.0/8", "myclient.filter.port_denylist": "0"}
    )
    logger.info("Created baseplate.")
    baseplate.configure_context({"myclient": InternalRequestsClient()})

    observer = PrometheusBaseplateObserver()
    logger.info(f"Created observer. [type={observer.__class__.__name__}]")
    baseplate.register(observer)
    logger.info("Registered observer with baseplate.")

    # we need to clear metrics between test runs from the parametrize annotations
    for collector_name in REGISTRY._names_to_collectors:
        reg = REGISTRY._names_to_collectors[collector_name]
        if hasattr(reg, "clear") and callable(getattr(reg, "clear")):
            REGISTRY._names_to_collectors[collector_name].clear()

    with baseplate.server_context("test") as context:
        with context.span.make_child("local", local=True) as span:
            logger.debug("Created local context.")
            fn = getattr(span.context.myclient, method.lower())
            logger.debug(f"Making HTTP call. [method={method}, endpoint={http_server.url}]")
            response = fn(http_server.url)
            logger.debug(f"Made HTTP call. [method={method}, endpoint={http_server.url}]")

    assert response.status_code == 204

    # Only making quick checks here, since this is meant to ensure that we do record metrics inside
    # a local span at all. Whether tags etc. are valid are tested in a previous test:
    # test_prometheus_http_client_metrics
    http_client_requests_total = REGISTRY._names_to_collectors[
        "http_client_requests_total"
    ].collect()
    assert len(http_client_requests_total) == 1
    assert len(http_client_requests_total[0].samples) == 2  # _total and _created
    http_client_requests_total_total = http_client_requests_total[0].samples[0]
    assert http_client_requests_total_total.value == 1

    http_client_latency_seconds = REGISTRY._names_to_collectors[
        "http_client_latency_seconds"
    ].collect()
    assert len(http_client_latency_seconds) == 1
    assert (
        len(http_client_latency_seconds[0].samples) == 18
    )  # 15 time buckets (inc. inf), _count, _sum and _created
    http_client_latency_seconds_inf = http_client_latency_seconds[0].samples[14]
    assert http_client_latency_seconds_inf.labels.get("le") == "+Inf"
    assert http_client_latency_seconds_inf.value == 1
    http_client_latency_seconds_count = http_client_latency_seconds[0].samples[15]
    assert http_client_latency_seconds_count.value == 1

    http_client_active_requests = REGISTRY._names_to_collectors[
        "http_client_active_requests"
    ].collect()
    assert len(http_client_active_requests) == 1
    assert len(http_client_active_requests[0].samples) == 1
    http_client_active_requests_samples = http_client_active_requests[0].samples[0]
    assert http_client_active_requests_samples.value == 0

    # checking that we got the local span metrics we expected
    local_span_latency_seconds = REGISTRY._names_to_collectors["local_latency_seconds"].collect()
    assert len(local_span_latency_seconds) == 1
    assert (
        len(local_span_latency_seconds[0].samples) == 18
    )  # 15 time buckets (inc. inf), _count, _sum and _created
    local_span_latency_seconds_inf = local_span_latency_seconds[0].samples[14]
    assert local_span_latency_seconds_inf.labels.get("le") == "+Inf"
    assert local_span_latency_seconds_inf.labels.get("span") == "local"
    assert local_span_latency_seconds_inf.value == 1
    local_span_latency_seconds_count = local_span_latency_seconds[0].samples[15]
    assert local_span_latency_seconds_count.value == 1

    local_span_requests_total = REGISTRY._names_to_collectors["local_requests_total"].collect()
    assert len(local_span_requests_total) == 1
    assert len(local_span_requests_total[0].samples) == 2  # _total and _created
    local_span_requests_total_total = local_span_requests_total[0].samples[0]
    assert local_span_requests_total_total.value == 1

    local_span_active_requests = REGISTRY._names_to_collectors["local_active_requests"].collect()
    assert len(local_span_active_requests) == 1
    assert len(local_span_active_requests[0].samples) == 1
    local_span_active_requests_samples = local_span_active_requests[0].samples[0]
    assert local_span_active_requests_samples.value == 0
