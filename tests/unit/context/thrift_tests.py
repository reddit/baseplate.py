from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate import core, thrift_pool
from baseplate.context import thrift
from baseplate.thrift import BaseplateService

from ... import mock


class EnumerateServiceMethodsTests(unittest.TestCase):
    def test_enumerate_none(self):
        class Iface(object):
            pass

        class ExampleClient(Iface):
            pass

        methods = list(thrift._enumerate_service_methods(ExampleClient))

        self.assertEqual(methods, [])

    def test_enumerate_some(self):
        class Iface(object):
            def some_method(self):
                pass

        class ExampleClient(Iface):
            pass

        methods = list(thrift._enumerate_service_methods(ExampleClient))

        self.assertEqual(set(methods), {"some_method"})

    def test_inherited(self):
        class Iface(object):
            def local_method(self):
                pass

        class ExampleClient(BaseplateService.Client, Iface):
            pass

        methods = list(thrift._enumerate_service_methods(ExampleClient))

        self.assertEqual(set(methods), {"is_healthy", "local_method"})

    def test_not_subclass_of_iface(self):
        class ExampleClient(object):
            pass

        with self.assertRaises(AssertionError):
            list(thrift._enumerate_service_methods(ExampleClient))


class PooledClientProxyTests(unittest.TestCase):
    def setUp(self):
        self.mock_pool = mock.MagicMock(spec=thrift_pool.ThriftConnectionPool)
        self.mock_client_cls = mock.Mock(spec=BaseplateService.Client)
        self.mock_client = self.mock_client_cls.return_value
        self.mock_server_span = mock.MagicMock(spec=core.ServerSpan)

    @mock.patch("baseplate.context.thrift._enumerate_service_methods")
    def test_proxy_methods_attached(self, mock_enumerate):
        mock_enumerate.return_value = ["one", "two"]

        proxy = thrift.PooledClientProxy(
            self.mock_client_cls, self.mock_pool, self.mock_server_span, "namespace")

        self.assertTrue(callable(proxy.one))
        self.assertTrue(callable(proxy.two))

    @mock.patch("baseplate.context.thrift._enumerate_service_methods")
    def test_call_proxy_method(self, mock_enumerate):
        mock_enumerate.return_value = ["one", "two"]

        proxy = thrift.PooledClientProxy(
            self.mock_client_cls, self.mock_pool, self.mock_server_span, "namespace")
        result = proxy.one(mock.sentinel.first, mock.sentinel.second)

        self.assertEqual(self.mock_client.one.call_count, 1)
        self.assertEqual(result, self.mock_client.one.return_value)
        self.assertEqual(self.mock_server_span.make_child.call_args, mock.call("namespace.one"))
