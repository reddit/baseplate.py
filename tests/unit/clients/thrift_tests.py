import unittest

from contextlib import nullcontext as does_not_raise
from unittest import mock

import pytest

from prometheus_client import REGISTRY
from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
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
        prot = mock.MagicMock()
        pool.connection().__enter__.return_value = prot
        client_cls = mock.MagicMock()
        client_cls.handle = handle
        handler = mock.MagicMock(
            retry_policy=[None, None],
            pool=pool,
            namespace="test_namespace",
        )
        handler.client_cls.return_value = client_cls

        thrift_success = str((exc is None)).lower()
        prom_labels = {
            "thrift_method": "handle",
            "thrift_client_name": "test_namespace",
        }
        requests_total_prom_labels = {
            "thrift_exception_type": exc_type,
            "thrift_baseplate_status": status,
            "thrift_baseplate_status_code": status_code,
        }

        with expectation:
            proxy_method(self=handler)

        tries = 1 if exc_type != "TTransportException" else 2
        assert (
            REGISTRY.get_sample_value(
                "thrift_client_requests_total",
                {**prom_labels, **requests_total_prom_labels, "thrift_success": thrift_success},
            )
            == tries
        )
        assert (
            REGISTRY.get_sample_value(
                "thrift_client_latency_seconds_bucket",
                {**prom_labels, "thrift_success": thrift_success, "le": "+Inf"},
            )
            == tries
        )
        assert REGISTRY.get_sample_value("thrift_client_active_requests", prom_labels) == 0

    def test_build_thrift_proxy_method_fail_connection(self):
        def handle(*args, **kwargs):
            return 42

        proxy_method = _build_thrift_proxy_method("handle")
        pool = mock.MagicMock(timeout=None)
        pool.connection().__enter__.side_effect = Exception("failed to establish connection")
        client_cls = mock.MagicMock()
        client_cls.handle = handle
        handler = mock.MagicMock(
            retry_policy=[None, None],
            pool=pool,
            namespace="test_namespace",
        )
        handler.client_cls.return_value = client_cls

        with pytest.raises(Exception):
            proxy_method(self=handler)

        prom_labels = {
            "thrift_method": "handle",
            "thrift_client_name": "test_namespace",
        }
        assert REGISTRY.get_sample_value("thrift_client_active_requests", prom_labels) is None


class TestThriftContextFactory:
    @pytest.fixture
    def pool(self):
        yield mock.MagicMock(size=4, checkedout=8)

    @pytest.fixture
    def context_factory(self, pool):
        class Iface:
            def handle(*args, **kwargs):
                pass

        context_factory = ThriftContextFactory(
            pool=pool,
            client_cls=Iface,
        )
        context_factory.max_connections_gauge.clear()
        context_factory.active_connections_gauge.clear()
        yield context_factory

    def test_thrift_server_pool_prometheus_metrics(self, context_factory):
        context_factory.report_runtime_metrics(batch=mock.MagicMock())
        prom_labels = {
            "thrift_pool": "Iface",
        }
        assert REGISTRY.get_sample_value("thrift_client_pool_max_size", prom_labels) == 4
        assert REGISTRY.get_sample_value("thrift_client_pool_active_connections", prom_labels) == 8
