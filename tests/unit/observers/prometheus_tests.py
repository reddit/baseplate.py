import unittest

from unittest import mock

import pytest

from prometheus_client import REGISTRY

from baseplate import ServerSpan
from baseplate.observers.prometheus import PrometheusBaseplateObserver
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
    ),
)
def test_observer_metrics(protocol, client_or_server, observer_cls, labels):
    before_start = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_latency_seconds_count", labels.get("latency_labels", "")
    )

    assert before_start is None
    before_start = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_requests_total", labels.get("requests_labels", "")
    )
    assert before_start is None
    before_start = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_active_requests", labels.get("active_labels", "")
    )
    assert before_start is None

    observer = observer_cls()
    observer.on_set_tag("protocol", protocol)
    assert observer.get_prefix() == f"{protocol}_{client_or_server}"

    observer.on_start()
    after_start = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_latency_seconds_count", labels.get("latency_labels", "")
    )
    assert after_start is None
    after_start = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_requests_total", labels.get("requests_labels", "")
    )
    assert after_start is None
    after_start = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_active_requests", labels.get("active_labels", "")
    )
    assert after_start == 1.0

    observer.on_finish(None)
    after_done = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_latency_seconds_count", labels.get("latency_labels", "")
    )
    assert after_done == 1.0
    after_done = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_requests_total", labels.get("requests_labels", "")
    )
    assert after_done == 1.0
    after_done = REGISTRY.get_sample_value(
        f"{protocol}_{client_or_server}_active_requests", labels.get("active_labels", "")
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
