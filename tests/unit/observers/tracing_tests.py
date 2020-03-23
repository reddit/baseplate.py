import json
import unittest

from unittest import mock

from baseplate import ServerSpan
from baseplate import Span
from baseplate.lib.config import Endpoint
from baseplate.observers.tracing import ANNOTATIONS
from baseplate.observers.tracing import LoggingRecorder
from baseplate.observers.tracing import make_client
from baseplate.observers.tracing import NullRecorder
from baseplate.observers.tracing import RemoteRecorder
from baseplate.observers.tracing import TraceBaseplateObserver
from baseplate.observers.tracing import TraceLocalSpanObserver
from baseplate.observers.tracing import TraceServerSpanObserver
from baseplate.observers.tracing import TraceSpanObserver


class TraceTestBase(unittest.TestCase):
    def setUp(self):
        thread_patch = mock.patch("threading.Thread", autospec=True)
        thread_patch.start()
        self.addCleanup(thread_patch.stop)


class TraceObserverTests(TraceTestBase):
    def setUp(self):
        super().setUp()
        self.mock_context = mock.Mock()

    def test_null_recorder_setup(self):
        client = make_client("test-service", log_if_unconfigured=False)
        baseplate_observer = TraceBaseplateObserver(client)
        self.assertEqual(type(baseplate_observer.recorder), NullRecorder)

    def test_logging_recorder_setup(self):
        client = make_client("test-service")
        baseplate_observer = TraceBaseplateObserver(client)
        self.assertEqual(type(baseplate_observer.recorder), LoggingRecorder)

    def test_sets_hostname(self):
        client = make_client("test-service")
        baseplate_observer = TraceBaseplateObserver(client)
        self.assertIsNotNone(baseplate_observer.hostname)

    def test_remote_recorder_setup(self):
        client = make_client("test-service", tracing_endpoint=Endpoint("test:1111"))
        baseplate_observer = TraceBaseplateObserver(client)
        self.assertTrue(isinstance(baseplate_observer.recorder, RemoteRecorder))

    def test_register_server_span_observer(self):
        client = make_client("test-service")
        baseplate_observer = TraceBaseplateObserver(client)
        context_mock = mock.Mock()
        span = ServerSpan(
            "test-id", "test-parent-id", "test-span-id", True, 0, "test", self.mock_context
        )
        baseplate_observer.on_server_span_created(context_mock, span)
        self.assertEqual(len(span.observers), 1)
        self.assertEqual(type(span.observers[0]), TraceServerSpanObserver)

    def test_force_sampling(self):
        span_with_debug_flag = Span(
            "test-id", "test-parent", "test-span-id", True, 1, "test", self.mock_context
        )
        span_without_debug_flag = Span(
            "test-id", "test-parent", "test-span-id", True, 0, "test", self.mock_context
        )
        self.assertTrue(TraceBaseplateObserver.force_sampling(span_with_debug_flag))
        self.assertFalse(TraceBaseplateObserver.force_sampling(span_without_debug_flag))

    def test_should_sample_utilizes_sampled_setting(self):
        client = make_client("test-service", sample_rate=0)
        baseplate_observer = TraceBaseplateObserver(client)
        span_with_sampled_flag = Span(
            "test-id", "test-parent", "test-span-id", True, 0, "test", self.mock_context
        )
        self.assertTrue(baseplate_observer.should_sample(span_with_sampled_flag))

    def test_should_sample_utilizes_force_sampling(self):
        client = make_client("test-service", sample_rate=0)
        baseplate_observer = TraceBaseplateObserver(client)
        span_with_forced = Span(
            "test-id", "test-parent", "test-span-id", False, 1, "test", self.mock_context
        )
        span_without_forced = Span(
            "test-id", "test-parent", "test-span-id", False, 0, "test", self.mock_context
        )
        self.assertTrue(baseplate_observer.should_sample(span_with_forced))
        self.assertFalse(baseplate_observer.should_sample(span_without_forced))

    def test_should_sample_utilizes_sample_rate(self):
        client = make_client("test-service", sample_rate=1)
        baseplate_observer = TraceBaseplateObserver(client)
        span = Span("test-id", "test-parent", "test-span-id", None, 0, "test", self.mock_context)
        self.assertTrue(baseplate_observer.should_sample(span))
        baseplate_observer.sample_rate = 0
        self.assertFalse(baseplate_observer.should_sample(span))

    def test_no_tracing_without_sampling(self):
        client = make_client("test-service", sample_rate=0)
        baseplate_observer = TraceBaseplateObserver(client)
        context_mock = mock.Mock()
        span = ServerSpan(
            "test-id", "test-parent-id", "test-span-id", False, 0, "test", self.mock_context
        )
        baseplate_observer.on_server_span_created(context_mock, span)
        self.assertEqual(len(span.observers), 0)


class TraceSpanObserverTests(TraceTestBase):
    def setUp(self):
        super().setUp()
        self.recorder = NullRecorder()
        self.mock_context = mock.Mock()

        self.span = Span(
            "test-id", "test-parent-id", "test-span-id", None, 0, "test", self.mock_context
        )

        self.debug_span = Span(
            "test-id", "test-parent-id", "test-span-id", None, 1, "test", self.mock_context
        )

        self.test_span_observer = TraceSpanObserver(
            "test-service", "test-hostname", self.span, self.recorder
        )

        self.test_debug_span_observer = TraceSpanObserver(
            "test-service", "test-hostname", self.debug_span, self.recorder
        )

    def test_component_set_on_initialization(self):
        component_set = False
        for annotation in self.test_span_observer.binary_annotations:
            if annotation["key"] == ANNOTATIONS["COMPONENT"] and annotation["value"] == "baseplate":
                component_set = True
                break
        self.assertTrue(component_set)

    def test_debug_span_tag_set_on_initialization(self):

        for annotation in self.test_debug_span_observer.binary_annotations:
            if annotation["key"] == ANNOTATIONS["DEBUG"]:
                self.assertTrue(annotation["value"])
                break

    def test_serialize_uses_span_info(self):
        serialized_span = self.test_span_observer._serialize()
        self.assertEqual(serialized_span["traceId"], self.span.trace_id)
        self.assertEqual(serialized_span["name"], self.span.name)
        self.assertEqual(serialized_span["id"], self.span.id)

    def test_serialize_adds_cs(self):
        serialized_span = self.test_span_observer._serialize()
        cs_in_annotation = False
        for annotation in serialized_span["annotations"]:
            if annotation["value"] == "cs":
                cs_in_annotation = True
        self.assertTrue(cs_in_annotation)

    def test_serialize_adds_cr(self):
        serialized_span = self.test_span_observer._serialize()
        cr_in_annotation = False
        for annotation in serialized_span["annotations"]:
            if annotation["value"] == "cr":
                cr_in_annotation = True
        self.assertTrue(cr_in_annotation)

    def test_serialize_adds_binary_annotations(self):
        # Reset in case initialization annotations exist.
        self.test_span_observer.binary_annotations = []

        self.test_span_observer.on_set_tag("test-key", "test-value")
        serialized_span = self.test_span_observer._serialize()
        self.assertEqual(len(serialized_span["binaryAnnotations"]), 1)
        annotation = serialized_span["binaryAnnotations"][0]
        self.assertEqual(annotation["key"], "test-key")
        self.assertEqual(annotation["value"], "test-value")

    def test_on_start_sets_start_timestamp(self):
        # on-start should set start time
        self.assertIsNone(self.test_span_observer.start)
        self.test_span_observer.on_start()
        self.assertIsNotNone(self.test_span_observer.start)

    def test_on_finish_sets_end_timestamp_and_duration(self):
        self.assertIsNone(self.test_span_observer.end)
        self.test_span_observer.on_start()
        self.test_span_observer.on_finish(None)
        self.assertIsNotNone(self.test_span_observer.end)

    def test_on_finish_records(self):
        self.assertIsNone(self.test_span_observer.end)
        self.test_span_observer.on_start()
        self.test_span_observer.on_finish(None)
        self.assertIsNotNone(self.test_span_observer.end)

    def test_on_finish_sets_debug_annotation(self):
        self.assertIsNone(self.test_debug_span_observer.end)
        self.test_debug_span_observer.on_start()
        self.test_debug_span_observer.on_finish(None)
        debug_annotation = None
        for annotation in self.test_debug_span_observer.binary_annotations:
            if annotation["key"] == ANNOTATIONS["DEBUG"]:
                debug_annotation = annotation
                break
        self.assertIsNotNone(debug_annotation)
        self.assertEqual(debug_annotation["value"], "true")

    def test_on_finish_sets_error_annotation(self):
        self.assertIsNone(self.test_span_observer.end)
        self.test_span_observer.on_start()
        self.test_span_observer.on_finish((ValueError, ValueError(), None))
        error_annotation = None
        for annotation in self.test_span_observer.binary_annotations:
            if annotation["key"] == ANNOTATIONS["ERROR"]:
                error_annotation = annotation
                break
        self.assertIsNotNone(error_annotation)
        self.assertEqual(error_annotation["value"], "true")

    def test_create_binary_annotation(self):
        annotation = self.test_span_observer._create_binary_annotation("test-key", "test-value")
        self.assertEquals(annotation["key"], "test-key")
        self.assertEquals(annotation["value"], "test-value")
        self.assertTrue(annotation["endpoint"])

    def test_create_binary_annotation_coerces_string(self):
        annotation = self.test_span_observer._create_binary_annotation("test-key", 1)
        self.assertEquals(annotation["key"], "test-key")
        self.assertEquals(annotation["value"], "1")
        self.assertTrue(annotation["endpoint"])

    def test_on_set_tag_adds_binary_annotation(self):
        self.test_span_observer.binary_annotations = []
        self.assertFalse(self.test_span_observer.binary_annotations)
        self.test_span_observer.on_set_tag("test-key", "test-value")
        annotation = self.test_span_observer.binary_annotations[0]
        self.assertEquals(annotation["key"], "test-key")
        self.assertEquals(annotation["value"], "test-value")

    def test_to_span_obj_sets_parent_id(self):
        span_obj = self.test_span_observer._to_span_obj([], [])
        self.assertEqual(span_obj["parentId"], self.span.parent_id)

    def test_to_span_obj_sets_default_parent_id(self):
        self.span.parent_id = None
        span_obj = self.test_span_observer._to_span_obj([], [])
        self.assertEqual(span_obj["parentId"], 0)

    def test_incr_tag_adds_binary_annotation(self):
        self.test_span_observer.binary_annotations = []
        self.test_span_observer.on_start()
        self.test_span_observer.on_incr_tag("test-key", 3)
        self.test_span_observer.on_incr_tag("test-key", 5)
        self.test_span_observer.on_finish(None)
        annotation = self.test_span_observer.binary_annotations[0]
        self.assertEquals(annotation["key"], "counter.test-key")
        self.assertEquals(annotation["value"], "8.0")


class TraceServerSpanObserverTests(TraceTestBase):
    def setUp(self):
        super().setUp()
        self.recorder = NullRecorder()
        self.mock_context = mock.Mock()
        self.span = ServerSpan(
            "test-id", "test-parent-id", "test-span-id", None, 0, "test", self.mock_context
        )
        self.test_server_span_observer = TraceServerSpanObserver(
            "test-service", "test-hostname", self.span, self.recorder
        )

    def test_server_span_observer_inherits_span_observer(self):
        # in case of future refactoring
        self.assertTrue(isinstance(self.test_server_span_observer, TraceSpanObserver))
        self.assertTrue(issubclass(TraceServerSpanObserver, TraceSpanObserver))

    def test_serialize_uses_span_info(self):
        serialized_span = self.test_server_span_observer._serialize()
        self.assertEqual(serialized_span["traceId"], self.span.trace_id)
        self.assertEqual(serialized_span["name"], self.span.name)
        self.assertEqual(serialized_span["id"], self.span.id)

    def test_serialize_adds_ss(self):
        serialized_span = self.test_server_span_observer._serialize()
        ss_in_annotation = False
        for annotation in serialized_span["annotations"]:
            if annotation["value"] == "ss":
                ss_in_annotation = True
        self.assertTrue(ss_in_annotation)

    def test_serialize_adds_sr(self):
        serialized_span = self.test_server_span_observer._serialize()
        sr_in_annotation = False
        for annotation in serialized_span["annotations"]:
            if annotation["value"] == "sr":
                sr_in_annotation = True
        self.assertTrue(sr_in_annotation)

    def test_on_child_span_created(self):
        child_span = Span(
            "child-id", "test-parent-id", "test-span-id", None, 0, "test-child", self.mock_context
        )
        self.test_server_span_observer.on_child_span_created(child_span)
        # Make sure new trace observer is added in the child span
        #  and the trace observer's span is that child span
        self.assertEqual(len(child_span.observers), 1)
        self.assertEqual(child_span.observers[0].span, child_span)

    def test_on_child_span_created_for_debug_span(self):
        child_span = Span(
            "child-id", "test-parent-id", "test-span-id", None, 1, "test-child", self.mock_context
        )
        self.test_server_span_observer.on_child_span_created(child_span)
        # Make sure new trace observer is added in the child span
        #  and the trace observer's span is that child span
        self.assertEqual(len(child_span.observers), 1)
        self.assertEqual(child_span.observers[0].span, child_span)


class TraceLocalSpanObserverTests(TraceTestBase):
    def setUp(self):
        super().setUp()
        self.recorder = NullRecorder()
        self.mock_context = mock.Mock()
        self.span = ServerSpan(
            "test-id", "test-parent-id", "test-span-id", None, 0, "test", self.mock_context
        )

    def test_init_local_component(self):
        local_trace_observer = TraceLocalSpanObserver(
            "test-service", "test-component", "test-host", self.span, self.recorder
        )
        self.assertEqual(local_trace_observer.component_name, "test-component")

    def test_local_component_structure(self):
        local_trace_observer = TraceLocalSpanObserver(
            "test-service", "test-component", "test-host", self.span, self.recorder
        )
        self.assertListEqual(
            local_trace_observer.binary_annotations,
            [
                {
                    "key": ANNOTATIONS["COMPONENT"],
                    "value": "baseplate",
                    "endpoint": {"ipv4": "test-host", "serviceName": "test-service"},
                },
                {
                    "key": ANNOTATIONS["LOCAL_COMPONENT"],
                    "value": "test-component",
                    "endpoint": {"ipv4": "test-host", "serviceName": "test-service"},
                },
            ],
        )

    def test_serialize(self):
        local_trace_observer = TraceLocalSpanObserver(
            "test-service", "test-component", "test-host", self.span, self.recorder
        )
        local_trace_observer.on_start()
        local_trace_observer.on_finish(None)
        serialized_span = local_trace_observer._serialize()
        self.assertIsNotNone(serialized_span["duration"])
        self.assertEqual(serialized_span["name"], self.span.name)
        annotations = serialized_span["binaryAnnotations"]
        for annotation in annotations:
            self.assertTrue("key" in annotation)
            self.assertTrue("value" in annotation)
            self.assertTrue("endpoint" in annotation)


class NullRecorderTests(TraceTestBase):
    def setUp(self):
        super().setUp()
        self.recorder = NullRecorder()
        self.mock_context = mock.Mock()

    def test_null_recorder_flush(self):
        span = Span("test-id", "test-parent-id", "test-span-id", None, 0, "test", self.mock_context)
        self.recorder.flush_func([span])


class RemoteRecorderTests(TraceTestBase):
    def setUp(self):
        super().setUp()
        self.endpoint = "test:1111"

    def test_init(self):
        recorder = RemoteRecorder(self.endpoint, 5)
        self.assertEqual(recorder.endpoint, "http://test:1111/api/v1/spans")

    def test_remote_recorder_flush(self):
        recorder = RemoteRecorder(self.endpoint, 5)
        serialized_span = {
            "traceId": "test-id",
            "name": "test-span",
            "id": "span-id",
            "timestamp": 0,
            "duration": 0,
            "annotations": [],
            "binaryAnnotations": [],
        }
        func_mock = mock.Mock()
        with mock.patch.object(recorder.session, "post", func_mock):
            recorder.flush_func([serialized_span])
            func_mock.assert_called_with(
                recorder.endpoint,
                data=json.dumps([serialized_span]).encode("utf8"),
                headers={"Content-Type": "application/json"},
                timeout=1,
            )
