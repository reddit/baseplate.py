import unittest

from unittest import mock

from baseplate import Baseplate
from baseplate import BaseplateObserver
from baseplate import LocalSpan
from baseplate import ParentSpanAlreadyFinishedError
from baseplate import RequestContext
from baseplate import ReusedContextObjectError
from baseplate import ServerSpan
from baseplate import ServerSpanObserver
from baseplate import Span
from baseplate import SpanObserver
from baseplate import TraceInfo
from baseplate.clients import ContextFactory
from baseplate.lib import config


def make_test_server_span(context=None):
    if not context:
        context = mock.Mock()
    return ServerSpan(1, 2, 3, None, 0, "name", context)


def make_test_span(context=None, local=False):
    if not context:
        context = mock.Mock()
    span = Span(1, 2, 3, None, 0, "name", context)
    if local:
        span = LocalSpan(1, 2, 3, None, 0, "name", context)
    return span


class BaseplateTests(unittest.TestCase):
    def test_server_observer_made(self):
        baseplate = Baseplate()
        mock_context = baseplate.make_context_object()
        mock_observer = mock.Mock(spec=BaseplateObserver)
        baseplate.register(mock_observer)
        server_span = baseplate.make_server_span(mock_context, "name", TraceInfo(1, 2, 3, None, 0))

        self.assertEqual(baseplate.observers, [mock_observer])
        self.assertEqual(mock_observer.on_server_span_created.call_count, 1)
        self.assertEqual(
            mock_observer.on_server_span_created.call_args, mock.call(mock_context, server_span)
        )

    def test_null_server_observer(self):
        baseplate = Baseplate()
        mock_context = baseplate.make_context_object()
        mock_observer = mock.Mock(spec=BaseplateObserver)
        mock_observer.on_server_span_created.return_value = None
        baseplate.register(mock_observer)
        server_span = baseplate.make_server_span(mock_context, "name", TraceInfo(1, 2, 3, None, 0))

        self.assertEqual(server_span.observers, [])

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
        mock_observer = mock.Mock(spec=SpanObserver)

        span = make_test_span()
        span.register(mock_observer)

        span.start()
        self.assertEqual(mock_observer.on_start.call_count, 1)

        span.set_tag("key", "value")
        mock_observer.on_set_tag("key", "value")

        span.log("name", "payload")
        mock_observer.on_log("name", "payload")

        span.finish()
        mock_observer.on_finish(exc_info=None)

    def test_context(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = make_test_span()
        span.register(mock_observer)

        with span:
            self.assertEqual(mock_observer.on_start.call_count, 1)
        self.assertEqual(mock_observer.on_finish.call_count, 1)

    def test_context_with_tags(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = make_test_span()
        span.register(mock_observer)

        tags = {
            "k1": "v1",
            "k2": "v2",
        }
        with span.with_tags(tags) as span:
            self.assertEqual(mock_observer.on_start.call_count, 1)
            mock_observer.on_set_tag.assert_has_calls(
                [
                    mock.call("k1", "v1"),
                    mock.call("k2", "v2"),
                ]
            )
        self.assertEqual(mock_observer.on_finish.call_count, 1)

    def test_context_with_exception(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        # span = Span(1, 2, 3, None, 0, "name")
        span = make_test_span()
        span.register(mock_observer)

        class TestException(Exception):
            pass

        exc = TestException()
        with self.assertRaises(TestException):
            with span:
                raise exc
        self.assertEqual(mock_observer.on_finish.call_count, 1)
        _, captured_exc, _ = mock_observer.on_finish.call_args[0][0]
        self.assertEqual(captured_exc, exc)


class ServerSpanTests(unittest.TestCase):
    @mock.patch("random.getrandbits", autospec=True)
    def test_make_child(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE

        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_context = mock.Mock()

        server_span = ServerSpan("trace", "parent", "id", None, 0, "name", mock_context)
        server_span.register(mock_observer)
        child_span = server_span.make_child("child_name")

        self.assertEqual(child_span.name, "child_name")
        self.assertEqual(child_span.id, "51966")
        self.assertEqual(child_span.trace_id, "trace")
        self.assertEqual(child_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args, mock.call(child_span))

    def test_null_child(self):
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_observer.on_child_span_created.return_value = None

        server_span = make_test_server_span()
        # server_span = ServerSpan("trace", "parent", "id", None, 0, "name")
        server_span.register(mock_observer)
        child_span = server_span.make_child("child_name")

        self.assertEqual(child_span.observers, [])

    @mock.patch("random.getrandbits", autospec=True)
    def test_make_local_span(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_context = mock.Mock()
        mock_cloned_context = mock.Mock()
        mock_context.clone.return_value = mock_cloned_context

        server_span = ServerSpan("trace", "parent", "id", None, 0, "name", mock_context)
        server_span.register(mock_observer)
        local_span = server_span.make_child("test_op", local=True, component_name="test_component")

        self.assertEqual(local_span.name, "test_op")
        self.assertEqual(local_span.id, "51966")
        self.assertEqual(local_span.trace_id, "trace")
        self.assertEqual(local_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args, mock.call(local_span))

    @mock.patch("random.getrandbits", autospec=True)
    def test_make_local_span_copies_context(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE
        mock_observer = mock.Mock(spec=ServerSpanObserver)
        mock_context = mock.Mock()
        mock_cloned_context = mock.Mock()
        mock_context.clone.return_value = mock_cloned_context

        server_span = ServerSpan("trace", "parent", "id", None, 0, "name", mock_context)
        server_span.register(mock_observer)
        local_span = server_span.make_child("test_op", local=True, component_name="test_component")
        self.assertNotEqual(local_span.context, mock_context)


class LocalSpanTests(unittest.TestCase):
    def test_events(self):
        mock_observer = mock.Mock(spec=SpanObserver)

        span = make_test_span(local=True)
        span.register(mock_observer)

        span.start()
        self.assertEqual(mock_observer.on_start.call_count, 1)

        span.set_tag("key", "value")
        mock_observer.on_set_tag("key", "value")

        span.log("name", "payload")
        mock_observer.on_log("name", "payload")

        span.make_child("local_child")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        span.finish()
        mock_observer.on_finish(exc_info=None)

    @mock.patch("random.getrandbits", autospec=True)
    def test_make_child(self, mock_getrandbits):
        mock_getrandbits.return_value = 0xCAFE

        mock_observer = mock.Mock(spec=SpanObserver)
        mock_context = mock.Mock()

        local_span = LocalSpan("trace", "parent", "id", None, 0, "name", mock_context)
        local_span.register(mock_observer)
        child_span = local_span.make_child("child_name")

        self.assertEqual(child_span.name, "child_name")
        self.assertEqual(child_span.id, "51966")
        self.assertEqual(child_span.trace_id, "trace")
        self.assertEqual(child_span.parent_id, "id")
        self.assertEqual(mock_observer.on_child_span_created.call_count, 1)
        self.assertEqual(mock_observer.on_child_span_created.call_args, mock.call(child_span))

    def test_context_object_reused(self):
        baseplate = Baseplate()
        context = baseplate.make_context_object()

        with baseplate.make_server_span(context, "foo"):
            pass

        with self.assertRaises(ReusedContextObjectError):
            with baseplate.make_server_span(context, "bar"):
                pass

    def test_parent_already_finished(self):
        mock_context = mock.Mock()
        local_span = LocalSpan("trace", "parent", "id", None, 0, "name", mock_context)
        local_span.start()
        local_span.finish()

        with self.assertRaises(ParentSpanAlreadyFinishedError):
            local_span.make_child("foo")


class TraceInfoTests(unittest.TestCase):
    def test_new_does_not_have_parent_id(self):
        new_trace_info = TraceInfo.new()
        self.assertIsNone(new_trace_info.parent_id)

    def test_new_does_not_set_flags(self):
        new_trace_info = TraceInfo.new()
        self.assertIsNone(new_trace_info.flags)

    def test_new_does_not_set_sampled(self):
        new_trace_info = TraceInfo.new()
        self.assertIsNone(new_trace_info.sampled)

    def test_from_upstream_fails_on_invalid_sampled(self):
        with self.assertRaises(ValueError) as e:
            TraceInfo.from_upstream(1, 2, 3, "True", None)
        self.assertEqual(str(e.exception), "invalid sampled value")

    def test_from_upstream_fails_on_invalid_flags(self):
        with self.assertRaises(ValueError) as e:
            TraceInfo.from_upstream(1, 2, 3, True, -1)
        self.assertEqual(str(e.exception), "invalid flags value")

    def test_from_upstream_handles_no_sampled_or_flags(self):
        span = TraceInfo.from_upstream(1, 2, 3, None, None)
        self.assertIsNone(span.sampled)
        self.assertIsNone(span.flags)
