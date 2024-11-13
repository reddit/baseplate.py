from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from prometheus_client import REGISTRY
from requests import Request
from requests import Response
from requests import Session

from baseplate.clients.requests import ACTIVE_REQUESTS
from baseplate.clients.requests import BaseplateSession
from baseplate.clients.requests import LATENCY_SECONDS
from baseplate.clients.requests import REQUESTS_TOTAL
from baseplate.lib.prometheus_metrics import getHTTPSuccessLabel


@pytest.fixture
def baseplate_session(request):
    yield BaseplateSession(
        adapter=mock.MagicMock(),
        name="session_name",
        span=mock.MagicMock(),
        client_name=request.param,
    )


class TestBaseplateSessionProm:
    def setup(self):
        ACTIVE_REQUESTS.clear()
        LATENCY_SECONDS.clear()
        REQUESTS_TOTAL.clear()

    # request is a reserved name that can't be used in parametrize
    @pytest.mark.parametrize(
        "req",
        [
            Request("DELETE", "http://example.com/foo/bar"),
            Request("GET", "http://example.com/foo/bar"),
            Request("HEAD", "http://example.com/foo/bar"),
            Request("OPTIONS", "http://example.com/foo/bar"),
            Request("PATCH", "http://example.com/foo/bar"),
            Request("POST", "http://example.com/foo/bar"),
            Request("PUT", "http://example.com/foo/bar"),
        ],
    )
    @pytest.mark.parametrize(
        "response",
        [
            mock.MagicMock(spec=Response, status_code=200),
            mock.MagicMock(spec=Response, status_code=404),
            mock.MagicMock(spec=Response, status_code=503),
            Exception("smth went wrong"),
        ],
    )
    @pytest.mark.parametrize(
        "baseplate_session",
        [
            "test_client_name",
            "",
            None,
        ],
        indirect=True,
    )
    def test_send(self, req, response, baseplate_session):
        prom_labels = {
            "http_method": req.method.lower(),
            "http_client_name": baseplate_session.client_name
            if baseplate_session.client_name is not None
            else baseplate_session.name,
        }

        http_success = "true"
        status_code = ""
        expectation = does_not_raise()
        if isinstance(response, Exception):
            # if an exception is raised, success is false and status code is empty
            http_success = "false"
            expectation = pytest.raises(type(response))
        else:
            # otherwise we just grab that info from the response itself
            status_code = response.status_code
            http_success = getHTTPSuccessLabel(status_code)

        with mock.patch("baseplate.clients.requests.Session", spec=Session) as session:
            session().send.return_value = response
            with expectation:
                baseplate_session.send(req.prepare())

        assert REGISTRY.get_sample_value("http_client_active_requests", prom_labels) == 0

        assert (
            REGISTRY.get_sample_value(
                "http_client_latency_seconds_bucket",
                {**prom_labels, "http_success": http_success, "le": "+Inf"},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                "http_client_requests_total",
                {
                    **prom_labels,
                    "http_response_code": str(status_code),
                    "http_success": http_success,
                },
            )
            == 1
        )
