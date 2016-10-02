from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# webtest doesn't play well with unicode literals for headers on py2 :(
#from __future__ import unicode_literals

import unittest

import webtest

from baseplate import Baseplate
from baseplate.core import BaseplateObserver, ServerSpanObserver

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


class ConfiguratorTests(unittest.TestCase):
    def setUp(self):
        configurator = Configurator()
        configurator.add_route("example", "/example", request_method="GET")
        configurator.add_view(
            example_application, route_name="example", renderer="json")

        self.observer = mock.Mock(spec=BaseplateObserver)
        self.server_observer = mock.Mock(spec=ServerSpanObserver)
        def _register_mock(context, server_span):
            server_span.register(self.server_observer)
        self.observer.on_server_span_created.side_effect = _register_mock

        self.baseplate = Baseplate()
        self.baseplate.register(self.observer)
        self.baseplate_configurator = BaseplateConfigurator(
            self.baseplate,
            trust_trace_headers=True,
        )
        configurator.include(self.baseplate_configurator.includeme)
        app = configurator.make_wsgi_app()
        self.test_app = webtest.TestApp(app)

    @mock.patch("random.getrandbits")
    def test_no_trace_headers(self, getrandbits):
        getrandbits.return_value = 1234
        self.test_app.get("/example")

        self.assertEqual(self.observer.on_server_span_created.call_count, 1)

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertIsInstance(context, Request)
        self.assertEqual(server_span.trace_id, 1234)
        self.assertEqual(server_span.parent_id, None)
        self.assertEqual(server_span.id, 1234)

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)

    def test_trace_headers(self):
        self.test_app.get("/example", headers={
            "X-Trace": "1234",
            "X-Parent": "2345",
            "X-Span": "3456",
        })

        self.assertEqual(self.observer.on_server_span_created.call_count, 1)

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertIsInstance(context, Request)
        self.assertEqual(server_span.trace_id, 1234)
        self.assertEqual(server_span.parent_id, 2345)
        self.assertEqual(server_span.id, 3456)

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)

    def test_not_found(self):
        self.test_app.get("/nope", status=404)

        self.assertFalse(self.observer.on_server_span_created.called)

    def test_exception_caught(self):
        with self.assertRaises(TestException):
            self.test_app.get("/example?error")

        self.assertTrue(self.server_observer.on_start.called)
        self.assertTrue(self.server_observer.on_finish.called)
        _, captured_exc, _ = self.server_observer.on_finish.call_args[0][0]
        self.assertIsInstance(captured_exc, TestException)

    @mock.patch("random.getrandbits")
    def test_distrust_headers(self, getrandbits):
        getrandbits.return_value = 1234
        self.baseplate_configurator.trust_trace_headers = False

        self.test_app.get("/example", headers={
            "X-Trace": "1234",
            "X-Parent": "2345",
            "X-Span": "3456",
        })

        context, server_span = self.observer.on_server_span_created.call_args[0]
        self.assertEqual(server_span.trace_id, getrandbits.return_value)
        self.assertEqual(server_span.parent_id, None)
        self.assertEqual(server_span.id, getrandbits.return_value)
