from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from baseplate import Baseplate
from baseplate.diagnostics.tracing import (
    TraceBaseplateObserver,
    TraceServerSpanObserver,
    TraceLocalSpanObserver,
    TraceSpanObserver,
    NullRecorder,
    RemoteRecorder,
    make_client,
)

try:
    import webtest

    from pyramid.config import Configurator

    from baseplate.integration.pyramid import BaseplateConfigurator
except ImportError:
    raise unittest.SkipTest("pyramid/webtest is not installed")

from .. import mock


class TestException(Exception):
    pass


def example_application(request):
    if "error" in request.params:
        raise TestException("this is a test")
    return {"test": "success"}

def local_parent_trace_within_context(request):
    # For testing embedded tracing contexts
    #  See `TracingTests.test_local_tracing_embedded`
    with request.trace.make_child('local-req',
                                  local=True,
                                  component_name='in-context') as span:
        with span.make_child('local-req',
                             local=True,
                             component_name='in-context') as child_span:
            pass

class TracingTests(unittest.TestCase):

    def _register_server_mock(self, context, server_span):
        server_span_observer = TraceServerSpanObserver('test-service',
                                                       'test-hostname',
                                                       server_span,
                                                       NullRecorder())
        server_span.register(server_span_observer)
        self.server_span_observer = server_span_observer

    def _register_local_mock(self, span):
        local_span_observer = TraceLocalSpanObserver('test-service',
                                                     'test-component',
                                                     'test-hostname',
                                                     span,
                                                     NullRecorder())
        self.local_span_ids.append(span.id)
        self.local_span_observers.append(local_span_observer)
        span.register(local_span_observer)

    def setUp(self):
        thread_patch = mock.patch("threading.Thread", autospec=True)
        thread_patch.start()
        self.addCleanup(thread_patch.stop)
        configurator = Configurator()
        configurator.add_route("example", "/example", request_method="GET")
        configurator.add_view(
            example_application, route_name="example", renderer="json")

        configurator.add_route("local_test", "/local_test", request_method="GET")
        configurator.add_view(
            local_parent_trace_within_context, route_name="local_test", renderer="json")

        self.client = make_client("test-service")
        self.observer = TraceBaseplateObserver(self.client)

        self.baseplate = Baseplate()
        self.baseplate.register(self.observer)

        self.baseplate_configurator = BaseplateConfigurator(
            self.baseplate,
            trust_trace_headers=True,
        )
        configurator.include(self.baseplate_configurator.includeme)
        app = configurator.make_wsgi_app()
        self.local_span_ids = []
        self.local_span_observers = []
        self.test_app = webtest.TestApp(app)

    def test_trace_on_inbound_request(self):
        with mock.patch.object(TraceBaseplateObserver, 'on_server_span_created',
                               side_effect=self._register_server_mock) as mocked:
            self.test_app.get('/example')
            span = self.server_span_observer._serialize()
            self.assertEqual(span['name'], 'example')
            self.assertEqual(len(span['annotations']), 2)
            self.assertEqual(span['parentId'], 0)

    def test_local_tracing_embedded(self):
        with mock.patch.object(TraceBaseplateObserver, 'on_server_span_created',
                               side_effect=self._register_server_mock) as mocked, \
             mock.patch.object(TraceServerSpanObserver, 'on_child_span_created',
                               side_effect=self._register_local_mock) as server_child_mocked, \
             mock.patch.object(TraceLocalSpanObserver, 'on_child_span_created',
                               side_effect=self._register_local_mock) as local_child_mocked:

            self.test_app.get('/local_test')
            # Verify that child span can be created within a local span context
            #  and parent IDs are inherited accordingly.
            span = self.local_span_observers[-1]._serialize()
            self.assertEqual(span['name'], 'local-req')
            self.assertEqual(len(span['annotations']), 0)
            self.assertEqual(span['parentId'], self.local_span_ids[-2])

    def test_configure_tracing_with_defaults_legacy_style(self):
        baseplate = Baseplate()
        self.assertEqual(0, len(baseplate.observers))
        baseplate.configure_tracing('test')
        self.assertEqual(1, len(baseplate.observers))
        tracing_observer = baseplate.observers[0]
        self.assertEqual('test',tracing_observer.service_name)

    def test_configure_tracing_with_defaults_new_style(self):
        baseplate = Baseplate()
        self.assertEqual(0, len(baseplate.observers))
        client = make_client("test")
        baseplate.configure_tracing(client)
        self.assertEqual(1, len(baseplate.observers))
        tracing_observer = baseplate.observers[0]
        self.assertEqual('test',tracing_observer.service_name)

    def test_configure_tracing_with_args(self):
        baseplate = Baseplate()
        self.assertEqual(0, len(baseplate.observers))
        baseplate.configure_tracing('test',
                                    None,
                                    max_span_queue_size=500,
                                    num_span_workers=5,
                                    span_batch_interval=0.5,
                                    num_conns=100,
                                    sample_rate=0.1)
        self.assertEqual(1, len(baseplate.observers))
        tracing_observer = baseplate.observers[0]
        self.assertEqual('test', tracing_observer.service_name)
