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
