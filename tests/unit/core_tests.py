import unittest

from unittest import mock

from opentelemetry import trace

from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate import RequestContext
from baseplate import ReusedContextObjectError
from baseplate import ServerSpanObserver
from baseplate import SpanObserver
from baseplate.clients import ContextFactory
from baseplate.lib import config


class BaseplateTests(unittest.TestCase):
    def test_configure_context_supports_complex_specs(self):
        from baseplate.clients.thrift import ThriftClient
        from baseplate.thrift import BaseplateServiceV2

        app_config = {
            "enable_some_fancy_feature": "true",
            "thrift.foo.endpoint": "localhost:9090",
            "thrift.bar.endpoint": "localhost:9091",
        }

        baseplate = Baseplate(app_config)
        baseplate.configure_context(
            {
                "enable_some_fancy_feature": config.Boolean,
                "thrift": {
                    "foo": ThriftClient(BaseplateServiceV2.Client),
                    "bar": ThriftClient(BaseplateServiceV2.Client),
                },
            },
        )

        context = baseplate.make_context_object()
        with baseplate.make_server_span(context, "test"):
            self.assertTrue(context.enable_some_fancy_feature)
            self.assertIsNotNone(context.thrift.foo)
            self.assertIsNotNone(context.thrift.bar)

    def test_with_server_context(self):
        baseplate = Baseplate()
        observer = mock.Mock(spec=BaseplateObserver)
        baseplate.register(observer)

        observer.on_server_span_created.assert_not_called()
        with baseplate.server_context("example") as context:
            observer.on_server_span_created.assert_called_once()
            self.assertIsInstance(context, RequestContext)

    def test_add_to_context(self):
        baseplate = Baseplate()
        forty_two_factory = mock.Mock(spec=ContextFactory)
        forty_two_factory.make_object_for_context = mock.Mock(return_value=42)
        baseplate.add_to_context("forty_two", forty_two_factory)
        baseplate.add_to_context("true", True)

        context = baseplate.make_context_object()

        self.assertEqual(42, context.forty_two)
        self.assertTrue(context.true)

    def test_add_to_context_supports_complex_specs(self):
        baseplate = Baseplate()
        forty_two_factory = mock.Mock(spec=ContextFactory)
        forty_two_factory.make_object_for_context = mock.Mock(return_value=42)
        context_spec = {
            "forty_two": forty_two_factory,
            "true": True,
            "nested": {"foo": "bar"},
        }
        baseplate.add_to_context("complex", context_spec)

        context = baseplate.make_context_object()

        self.assertEqual(42, context.complex.forty_two)
        self.assertTrue(context.complex.true)
        self.assertEqual("bar", context.complex.nested.foo)


class SpanTests(unittest.TestCase):
    def test_events(self):
        """TODO: @trevor"""
        pass

    def test_context(self):
        """TODO: @trevor"""
        pass

    def test_context_with_exception(self):
        """TODO: @trevor"""
        pass


class ServerSpanTests(unittest.TestCase):
    def test_make_child(self):
        """TODO: @trevor"""
        pass

    def test_make_local_span(self):
        """TODO: @trevor"""


class LocalSpanTests(unittest.TestCase):
    def test_events(self):
        """TODO: @trevor"""

    def test_make_child(self):
        """TODO: @trevor"""


# TODO: @trevor
# Add tests for propagator
