from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from prometheus_client import REGISTRY
from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
from thrift.transport.TTransport import TTransportException

from baseplate.frameworks.thrift import _ContextAwareHandler
from baseplate.frameworks.thrift import PROM_ACTIVE
from baseplate.frameworks.thrift import PROM_LATENCY
from baseplate.frameworks.thrift import PROM_REQUESTS
from baseplate.thrift.ttypes import Error
from baseplate.thrift.ttypes import ErrorCode


class Test_ThriftServerPrometheusMetrics:
    def setup(self):
        PROM_LATENCY.clear()
        PROM_REQUESTS.clear()
        PROM_ACTIVE.clear()

    @pytest.mark.parametrize(
        "exc,convert,exc_type,status,status_code,expectation",
        [
            (None, True, "", "", "", does_not_raise()),
            (
                TApplicationException(TApplicationException.UNKNOWN_METHOD, "unknown method"),
                True,
                "TApplicationException",
                "",
                "",
                pytest.raises(TApplicationException),
            ),
            (
                TProtocolException(message="Required field is unset!"),
                True,
                "TProtocolException",
                "",
                "",
                pytest.raises(TProtocolException),
            ),
            (
                TTransportException(message="Something is wrong with the transport"),
                True,
                "TTransportException",
                "",
                "",
                pytest.raises(TTransportException),
            ),
            (
                Error(ErrorCode.NOT_FOUND, "404 not found"),
                True,
                "Error",
                "NOT_FOUND",
                "404",
                pytest.raises(Error),
            ),
            (
                Error(ErrorCode.SERVICE_UNAVAILABLE, "503 unavailable"),
                True,
                "Error",
                "SERVICE_UNAVAILABLE",
                "503",
                pytest.raises(Error),
            ),
            (
                TException(message="Some other generic thrift exception"),
                True,
                "TException",
                "",
                "",
                pytest.raises(TException),
            ),
            (
                Exception("Some very generic exception"),
                False,
                "Exception",
                "",
                "",
                pytest.raises(Exception),
            ),
            (
                Exception("Some very generic exception"),
                True,
                "Error",
                "INTERNAL_SERVER_ERROR",
                "500",
                pytest.raises(Error),
            ),
        ],
    )
    def test_thrift_server_prometheus_metrics(
        self, exc, convert, exc_type, status, status_code, expectation
    ):
        class Handler:
            def handle(*args, **kwargs):
                if exc is None:
                    pass
                else:
                    raise exc

        context_handler = _ContextAwareHandler(
            handler=Handler(),
            context=mock.MagicMock(),
            logger=mock.MagicMock(),
            convert_to_baseplate_error=convert,
        )

        with expectation:
            context_handler.handle()

        common_prom_labels = {
            "thrift_method": "handle",
            "thrift_success": str(exc is None).lower(),
        }

        other_prom_labels = {
            "thrift_exception_type": exc_type,
            "thrift_baseplate_status": status,
            "thrift_baseplate_status_code": status_code,
        }

        assert (
            REGISTRY.get_sample_value(
                "thrift_server_requests_total", {**common_prom_labels, **other_prom_labels}
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                "thrift_server_latency_seconds_bucket", {**common_prom_labels, "le": "+Inf"}
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value("thrift_server_active_requests", {"thrift_method": "handle"})
            == 0
        )
