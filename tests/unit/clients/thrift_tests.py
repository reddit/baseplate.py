import unittest

from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from prometheus_client import REGISTRY
from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
from thrift.Thrift import TProcessor
from thrift.transport.TTransport import TTransportException

from baseplate.clients import thrift
from baseplate.clients.thrift import _build_thrift_proxy_method
from baseplate.clients.thrift import ACTIVE_REQUESTS
from baseplate.clients.thrift import REQUEST_LATENCY
from baseplate.clients.thrift import REQUESTS_TOTAL
from baseplate.clients.thrift import ThriftContextFactory
from baseplate.thrift import BaseplateServiceV2
from baseplate.thrift.ttypes import Error
from baseplate.thrift.ttypes import ErrorCode


class EnumerateServiceMethodsTests(unittest.TestCase):
    def test_enumerate_none(self):
        class Iface:
            pass

        class ExampleClient(Iface):
            pass

        methods = list(thrift._enumerate_service_methods(ExampleClient))

        self.assertEqual(methods, [])

    def test_enumerate_some(self):
        class Iface:
            def some_method(self):
                pass

        class ExampleClient(Iface):
            pass

        methods = list(thrift._enumerate_service_methods(ExampleClient))

        self.assertEqual(set(methods), {"some_method"})

    def test_inherited(self):
        class Iface:
            def local_method(self):
                pass

        class ExampleClient(BaseplateServiceV2.Client, Iface):
            pass

        methods = list(thrift._enumerate_service_methods(ExampleClient))

        self.assertEqual(set(methods), {"is_healthy", "local_method"})

    def test_not_subclass_of_iface(self):
        class ExampleClient:
            pass

        with self.assertRaises(AssertionError):
            list(thrift._enumerate_service_methods(ExampleClient))


class TestPrometheusMetrics:
    def setup(self):
        REQUEST_LATENCY.clear()
        REQUESTS_TOTAL.clear()
        ACTIVE_REQUESTS.clear()

    @pytest.mark.parametrize(
        "exc,exc_type,status,status_code,expectation",
        [
            (None, "", "", "", does_not_raise()),
            (
                TApplicationException(TApplicationException.UNKNOWN_METHOD, "unknown method"),
                "TApplicationException",
                "",
                "",
                pytest.raises(TApplicationException),
            ),
            (
                TProtocolException(message="Required field is unset!"),
                "TProtocolException",
                "",
                "",
                pytest.raises(TProtocolException),
            ),
            (
                TTransportException(message="Something is wrong with the transport"),
                "TTransportException",
                "",
                "",
                pytest.raises(
                    TTransportException, match=r"retry policy exhausted while attempting.*"
                ),
            ),
            (
                Error(ErrorCode.NOT_FOUND, "404 not found"),
                "Error",
                "NOT_FOUND",
                "404",
                pytest.raises(Error),
            ),
            (
                Error(ErrorCode.SERVICE_UNAVAILABLE, "503 unavailable"),
                "Error",
                "SERVICE_UNAVAILABLE",
                "503",
                pytest.raises(Error),
            ),
            (
                TException(message="Some other generic thrift exception"),
                "TException",
                "",
                "",
                pytest.raises(TException),
            ),
            (
                Exception("Some very generic exception"),
                "Exception",
                "",
                "",
                pytest.raises(Exception),
            ),
        ],
    )
    def test_build_thrift_proxy_method(self, exc, exc_type, status, status_code, expectation):
        def handle(*args, **kwargs):
            if exc is None:
                return 42
            else:
                raise exc

        proxy_method = _build_thrift_proxy_method("handle")
        pool = mock.MagicMock(timeout=None)
        pool.__enter__.side_effect = [mock.MagicMock()]
        client_cls = mock.MagicMock()
        client_cls.handle = handle
        handler = mock.MagicMock(
            retry_policy=[None],
            pool=pool,
        )
        handler.client_cls.side_effect = [client_cls]

        thrift_success = str((exc is None)).lower()
        prom_labels = {
            "thrift_method": "handle",
            "thrift_client_name": "",
        }
        requests_total_prom_labels = {
            "thrift_exception_type": exc_type,
            "thrift_baseplate_status": status,
            "thrift_baseplate_status_code": status_code,
        }

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
                with expectation:
                    proxy_method(self=handler)

        assert (
            REGISTRY.get_sample_value(
                "thrift_client_requests_total",
                {**prom_labels, **requests_total_prom_labels, "thrift_success": thrift_success},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                "thrift_client_latency_seconds_bucket",
                {**prom_labels, "thrift_success": thrift_success, "le": "+Inf"},
            )
            == 1
        )
        assert REGISTRY.get_sample_value("thrift_client_active_requests", prom_labels) == 0
        assert mock_manager.mock_calls == [mock.call.inc(), mock.call.dec()]  # ensures we first increase number of active requests
