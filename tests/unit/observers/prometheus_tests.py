import unittest

from unittest import mock

import pytest

from prometheus_client import REGISTRY

from baseplate import ServerSpan
from baseplate.lib.prometheus_metrics import getHTTPSuccessLabel
from baseplate.observers.prometheus import PrometheusBaseplateObserver
from baseplate.observers.prometheus import PrometheusClientSpanObserver
from baseplate.observers.prometheus import PrometheusLocalSpanObserver
from baseplate.observers.prometheus import PrometheusServerSpanObserver


class TestException(Exception):
    pass


@pytest.mark.parametrize(
    "protocol,client_or_server,observer_cls,labels",
    (
        (
            "thrift",
            "server",
            PrometheusServerSpanObserver,
            {
                "latency_labels": {"thrift_method": "", "thrift_success": "true"},
                "requests_labels": {
                    "thrift_method": "",
                    "thrift_success": "true",
                    "thrift_exception_type": "",
                    "thrift_baseplate_status": "",
                    "thrift_baseplate_status_code": "",
                },
                "active_labels": {"thrift_method": ""},
            },
        ),
        (
            "thrift",
            "client",
            PrometheusClientSpanObserver,
            {
                "latency_labels": {"thrift_slug": "", "thrift_success": "true"},
                "requests_labels": {
                    "thrift_slug": "",
                    "thrift_success": "true",
                    "thrift_exception_type": "",
                    "thrift_baseplate_status": "",
                    "thrift_baseplate_status_code": "",
                },
                "active_labels": {
                    "thrift_slug": "",
                    "thrift_method": "",
                },
            },
        ),
        (
            "http",
            "server",
            PrometheusServerSpanObserver,
            {
                "requests_labels": {
                    "http_response_code": "",
                    "http_method": "",
                    "http_endpoint": "",
                    "http_success": "false",
                },
                "latency_labels": {"http_method": "", "http_endpoint": "", "http_success": "false"},
                "active_labels": {"http_method": "", "http_endpoint": ""},
            },
        ),
        (
            "http",
            "client",
            PrometheusClientSpanObserver,
            {
                "latency_labels": {
                    "http_method": "",
                    "http_success": "false",
                    "http_slug": "",
                },
                "requests_labels": {
                    "http_method": "",
                    "http_success": "false",
                    "http_response_code": "",
                    "http_slug": "",
                },
                "active_labels": {
                    "http_method": "",
                    "http_slug": "",
                },
            },
        ),
        (
            "local",
            None,
            PrometheusLocalSpanObserver,
            {
                "latency_labels": {
                    "span": "",
                },
                "requests_labels": {
                    "span": "",
                },
                "active_labels": {
                    "span": "",
                },
            },
        ),
    ),
)
def test_observer_metrics(protocol, client_or_server, observer_cls, labels):
    prefix = protocol
    if client_or_server is not None:
        prefix = f"{protocol}_{client_or_server}"

    before_start = REGISTRY.get_sample_value(
        f"{prefix}_latency_seconds_count", labels.get("latency_labels", "")
    )

    assert before_start is None
    before_start = REGISTRY.get_sample_value(
        f"{prefix}_requests_total", labels.get("requests_labels", "")
    )
    assert before_start is None
    before_start = REGISTRY.get_sample_value(
        f"{prefix}_active_requests", labels.get("active_labels", "")
    )
    assert before_start is None

    observer = observer_cls()
    observer.on_set_tag("protocol", protocol)

    observer.on_start()

    assert observer.metrics.prefix == f"{prefix}"
    after_start = REGISTRY.get_sample_value(
        f"{prefix}_latency_seconds_count", labels.get("latency_labels", "")
    )
    assert after_start is None
    after_start = REGISTRY.get_sample_value(
        f"{prefix}_requests_total", labels.get("requests_labels", "")
    )
    assert after_start is None
    after_start = REGISTRY.get_sample_value(
        f"{prefix}_active_requests", labels.get("active_labels", "")
    )
    assert after_start == 1.0

    observer.on_finish(None)
    after_done = REGISTRY.get_sample_value(
        f"{prefix}_latency_seconds_count", labels.get("latency_labels", "")
    )
    assert after_done == 1.0
    after_done = REGISTRY.get_sample_value(
        f"{prefix}_requests_total", labels.get("requests_labels", "")
    )
    assert after_done == 1.0
    after_done = REGISTRY.get_sample_value(
        f"{prefix}_active_requests", labels.get("active_labels", "")
    )
    assert after_done == 0.0


class ObserverTests(unittest.TestCase):
    def test_create_server_span(self):
        mock_server_span = mock.Mock(spec=ServerSpan)
        mock_server_span.name = "name"
        mock_context = mock.Mock()

        observer = PrometheusBaseplateObserver()
        observer.on_server_span_created(mock_context, mock_server_span)
        mock_server_span.set_tag("protocol", "thrift")

        self.assertEqual(mock_server_span.register.call_count, 1)

    def test_http_success_label(self):
        self.assertEqual("true", getHTTPSuccessLabel(222))
        self.assertEqual("true", getHTTPSuccessLabel(200))
        self.assertEqual("true", getHTTPSuccessLabel(399))
        self.assertEqual("false", getHTTPSuccessLabel(199))
        self.assertEqual("false", getHTTPSuccessLabel(400))
        self.assertEqual("false", getHTTPSuccessLabel(111))
        self.assertEqual("false", getHTTPSuccessLabel(418))
