import unittest

from baseplate.clients import thrift
from baseplate.thrift import BaseplateServiceV2


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
