import types

from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from prometheus_client import REGISTRY
from pyramid.response import Response

from baseplate.frameworks.pyramid import _make_baseplate_tween
from baseplate.frameworks.pyramid import ACTIVE_REQUESTS
from baseplate.frameworks.pyramid import BaseplateConfigurator
from baseplate.frameworks.pyramid import REQUEST_LATENCY
from baseplate.frameworks.pyramid import REQUEST_SIZE
from baseplate.frameworks.pyramid import REQUESTS_TOTAL
from baseplate.frameworks.pyramid import RESPONSE_SIZE


class TestPyramidHttpServerIntegrationPrometheus:
    def setup(self):
        ACTIVE_REQUESTS.clear()
        REQUESTS_TOTAL.clear()
        REQUEST_LATENCY.clear()
        REQUEST_SIZE.clear()
        RESPONSE_SIZE.clear()

    @pytest.mark.parametrize(
        "response",
        [
            Response("ok"),
            Response("page not found", status=404),
            Response("service unavailable", status=503),
            Exception("some generic exception"),
        ],
    )
    def test_http_server_metric_collection_method_with_request_pattern(self, response):
        status_code = ""
        http_success = "false"
        expectation = does_not_raise()
        if not isinstance(response, Exception):
            status_code = str(response.status_code)
            if status_code == "200":
                http_success = "true"
        else:
            expectation = pytest.raises(type(response))

        prom_labels = {
            "http_method": "get",
            "http_endpoint": "route_pattern",
        }

        handler = mock.MagicMock(return_value=response)
        registry = mock.MagicMock()
        request = mock.MagicMock(content_length=42, method="GET")
        request.matched_route.name = "route"
        request.matched_route.pattern = "route_pattern"
        event = mock.MagicMock(request=request)
        bpConfigurator = BaseplateConfigurator(mock.MagicMock())

        mock_manager = mock.Mock()
        with mock.patch.object(
            ACTIVE_REQUESTS.labels(**prom_labels),
            "inc",
            wraps=ACTIVE_REQUESTS.labels(**prom_labels).inc,
        ) as active_inc_spy_method:
            mock_manager.attach_mock(active_inc_spy_method, "inc")
            with mock.patch.object(
                ACTIVE_REQUESTS.labels(**prom_labels),
                "dec",
                wraps=ACTIVE_REQUESTS.labels(**prom_labels).dec,
            ) as active_dec_spy_method:
                mock_manager.attach_mock(active_dec_spy_method, "dec")
                with mock.patch.object(
                    REQUEST_SIZE.labels(**prom_labels, http_success=http_success),
                    "observe",
                    wraps=REQUEST_SIZE.labels(**prom_labels, http_success=http_success).observe,
                ) as request_size_spy_method:
                    mock_manager.attach_mock(request_size_spy_method, "request_observe")
                    with mock.patch.object(
                        RESPONSE_SIZE.labels(**prom_labels, http_success=http_success),
                        "observe",
                        wraps=RESPONSE_SIZE.labels(
                            **prom_labels, http_success=http_success
                        ).observe,
                    ) as response_size_spy_method:
                        mock_manager.attach_mock(response_size_spy_method, "response_observe")
                        with expectation:
                            bpConfigurator._on_new_request(event)
                            _make_baseplate_tween(handler=handler, _registry=registry)(request)

        assert (
            REGISTRY.get_sample_value(
                "http_server_requests_total",
                {**prom_labels, "http_response_code": status_code, "http_success": http_success},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                "http_server_latency_seconds_bucket",
                {**prom_labels, "http_success": http_success, "le": "+Inf"},
            )
            == 1
        )
        assert REGISTRY.get_sample_value("http_server_active_requests", prom_labels) == 0
        expected_calls = [
            mock.call.inc(),  # ensures we first increase number of active requests
            mock.call.dec(),
            mock.call.request_observe(42),
        ]
        if not isinstance(response, Exception):
            expected_calls.append(mock.call.response_observe(response.content_length))
        assert mock_manager.mock_calls == expected_calls

        assert (
            REGISTRY.get_sample_value(
                "http_server_request_size_bytes_bucket",
                {**prom_labels, "http_success": http_success, "le": "+Inf"},
            )
            == 1
        )
        expected_response_size_count = 0 if isinstance(response, Exception) else 1
        assert (
            REGISTRY.get_sample_value(
                "http_server_response_size_bytes_bucket",
                {**prom_labels, "http_success": http_success, "le": "+Inf"},
            )
            == expected_response_size_count
        )

    @pytest.mark.parametrize(
        "response",
        [
            Response("ok"),
            Response("page not found", status=404),
            Response("service unavailable", status=503),
            Exception("some generic exception"),
        ],
    )
    def test_http_server_metric_collection_method_no_pattern(self, response):
        status_code = ""
        http_success = "false"
        expectation = does_not_raise()
        if not isinstance(response, Exception):
            status_code = str(response.status_code)
            if status_code == "200":
                http_success = "true"
        else:
            expectation = pytest.raises(type(response))

        prom_labels = {
            "http_method": "get",
            "http_endpoint": "route_name",
        }

        handler = mock.MagicMock(return_value=response)
        registry = mock.MagicMock()
        request = mock.MagicMock(content_length=42, method="GET")
        request.matched_route = types.SimpleNamespace(name="route_name")
        event = mock.MagicMock(request=request)
        bpConfigurator = BaseplateConfigurator(mock.MagicMock())

        mock_manager = mock.Mock()
        with mock.patch.object(
            ACTIVE_REQUESTS.labels(**prom_labels),
            "inc",
            wraps=ACTIVE_REQUESTS.labels(**prom_labels).inc,
        ) as active_inc_spy_method:
            mock_manager.attach_mock(active_inc_spy_method, "inc")
            with mock.patch.object(
                ACTIVE_REQUESTS.labels(**prom_labels),
                "dec",
                wraps=ACTIVE_REQUESTS.labels(**prom_labels).dec,
            ) as active_dec_spy_method:
                mock_manager.attach_mock(active_dec_spy_method, "dec")
                with mock.patch.object(
                    REQUEST_SIZE.labels(**prom_labels, http_success=http_success),
                    "observe",
                    wraps=REQUEST_SIZE.labels(**prom_labels, http_success=http_success).observe,
                ) as request_size_spy_method:
                    mock_manager.attach_mock(request_size_spy_method, "request_observe")
                    with mock.patch.object(
                        RESPONSE_SIZE.labels(**prom_labels, http_success=http_success),
                        "observe",
                        wraps=RESPONSE_SIZE.labels(
                            **prom_labels, http_success=http_success
                        ).observe,
                    ) as response_size_spy_method:
                        mock_manager.attach_mock(response_size_spy_method, "response_observe")
                        with expectation:
                            bpConfigurator._on_new_request(event)
                            _make_baseplate_tween(handler=handler, _registry=registry)(request)

        assert (
            REGISTRY.get_sample_value(
                "http_server_requests_total",
                {**prom_labels, "http_response_code": status_code, "http_success": http_success},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                "http_server_latency_seconds_bucket",
                {**prom_labels, "http_success": http_success, "le": "+Inf"},
            )
            == 1
        )
        assert REGISTRY.get_sample_value("http_server_active_requests", prom_labels) == 0
        expected_calls = [
            mock.call.inc(),  # ensures we first increase number of active requests
            mock.call.dec(),
            mock.call.request_observe(42),
        ]
        if not isinstance(response, Exception):
            expected_calls.append(mock.call.response_observe(response.content_length))
        assert mock_manager.mock_calls == expected_calls

        assert (
            REGISTRY.get_sample_value(
                "http_server_request_size_bytes_bucket",
                {**prom_labels, "http_success": http_success, "le": "+Inf"},
            )
            == 1
        )
        expected_response_size_count = 0 if isinstance(response, Exception) else 1
        assert (
            REGISTRY.get_sample_value(
                "http_server_response_size_bytes_bucket",
                {**prom_labels, "http_success": http_success, "le": "+Inf"},
            )
            == expected_response_size_count
        )
