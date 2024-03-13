import unittest

from unittest import mock

from opentelemetry import propagate
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from baseplate import Baseplate
from baseplate.lib.propagator_redditb3 import RedditB3Format
from baseplate.lib.propagator_redditb3_thrift import RedditB3ThriftFormat

try:
    import webtest

    from pyramid.config import Configurator

    from baseplate.frameworks.pyramid import BaseplateConfigurator
    from baseplate.frameworks.pyramid import StaticTrustHandler
    from opentelemetry.test.test_base import TestBase
except ImportError:
    raise unittest.SkipTest("pyramid/webtest is not installed")


propagate.set_global_textmap(
    CompositePropagator([RedditB3ThriftFormat(), RedditB3Format(), TraceContextTextMapPropagator()])
)


class TestException(Exception):
    pass


def example_application(request):
    if "error" in request.params:
        raise TestException("this is a test")
    return {"test": "success"}


def local_parent_trace_within_context(request):
    # For testing embedded tracing contexts
    #  See `TracingTests.test_local_tracing_embedded`
    with request.span.make_child("local-req", local=True, component_name="in-context") as span:
        with span.make_child("local-req", local=True, component_name="in-context"):
            pass


class TracingTests(TestBase):
    def setUp(self):
        thread_patch = mock.patch("threading.Thread", autospec=True)
        thread_patch.start()
        self.addCleanup(thread_patch.stop)
        configurator = Configurator()
        configurator.add_route("example", "/example", request_method="GET")
        configurator.add_view(example_application, route_name="example", renderer="json")

        configurator.add_route("local_test", "/local_test", request_method="GET")
        configurator.add_view(
            local_parent_trace_within_context, route_name="local_test", renderer="json"
        )

        self.baseplate = Baseplate()

        self.baseplate_configurator = BaseplateConfigurator(
            self.baseplate,
            header_trust_handler=StaticTrustHandler(trust_headers=True),
        )
        configurator.include(self.baseplate_configurator.includeme)
        app = configurator.make_wsgi_app()
        self.test_app = webtest.TestApp(app)
        super().setUp()

    def test_trace_on_inbound_request(self):
        self.test_app.get("/example")

        span = self.get_finished_spans()[0]
        self.assertEqual(span["name"], "example")
        self.assertEqual(len(span["annotations"]), 2)
        self.assertEqual(span["parentId"], 0)

    def test_local_tracing_embedded(self):
        self.test_app.get("/local_test")
        # Verify that child span can be created within a local span context
        #  and parent IDs are inherited accordingly.
        span = self.get_finished_spans()[0]
        self.assertEqual(span["name"], "local-req")
        self.assertEqual(len(span["annotations"]), 0)
        self.assertEqual(span["parentId"], self.local_span_ids[-2])
