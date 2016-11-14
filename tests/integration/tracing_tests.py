from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import webtest

from baseplate import Baseplate
from baseplate.diagnostics.tracing import (
    TraceBaseplateObserver,
    TraceServerSpanObserver,
    TraceSpanObserver,
    NullRecorder,
    RemoteRecorder,
)

try:
    from baseplate.integration.pyramid import BaseplateConfigurator
    from pyramid.config import Configurator
    from pyramid.request import Request
except ImportError:
    raise unittest.SkipTest("pyramid is not installed")

from .. import mock


class TestException(Exception):
    pass


def example_application(request):
    if "error" in request.params:
        raise TestException("this is a test")
    return {"test": "success"}


class TracingTests(unittest.TestCase):

    def _register_mock(self, context, server_span):
        server_span_observer = TraceServerSpanObserver('test-service', server_span,
                                                       NullRecorder())
        server_span.register(server_span_observer)
        self.server_span_observer = server_span_observer

    def setUp(self):
        configurator = Configurator()
        configurator.add_route("example", "/example", request_method="GET")
        configurator.add_view(
            example_application, route_name="example", renderer="json")

        self.observer = TraceBaseplateObserver('test-service')

        self.baseplate = Baseplate()
        self.baseplate.register(self.observer)

        self.baseplate_configurator = BaseplateConfigurator(
            self.baseplate,
            trust_trace_headers=True,
        )
        configurator.include(self.baseplate_configurator.includeme)
        app = configurator.make_wsgi_app()
        self.test_app = webtest.TestApp(app)

    def test_trace_on_inbound_request(self):
        with mock.patch.object(TraceBaseplateObserver, 'on_server_span_created',
                               side_effect=self._register_mock) as mocked:

            self.test_app.get('/example')
            span = self.server_span_observer._serialize()
            self.assertEqual(span['name'], 'example')
            self.assertEqual(len(span['annotations']), 2)
            self.assertEqual(span['parentId'], 0)
