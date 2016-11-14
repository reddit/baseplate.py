from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.core import Span, ServerSpan
from baseplate.diagnostics.tracing import (
    TraceBaseplateObserver,
    TraceServerSpanObserver,
    TraceSpanObserver,
    RemoteRecorder,
    NullRecorder,
)

from ... import mock


class TraceObserverTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_null_recorder_setup(self):
        baseplate_observer = TraceBaseplateObserver('test-service')
        self.assertEqual(type(baseplate_observer.recorder), NullRecorder)

    def test_register_server_span_observer(self):
        baseplate_observer = TraceBaseplateObserver('test-service')
        context_mock = mock.Mock()
        span = ServerSpan('test-id', 'test-parent-id', 'test-span-id', 'test')
        baseplate_observer.on_server_span_created(context_mock, span)
        self.assertTrue(len(span.observers), 1)
        self.assertEqual(type(span.observers[0]), TraceServerSpanObserver)


class TraceSpanObserverTests(unittest.TestCase):
    def setUp(self):
        self.recorder = NullRecorder()
        self.span = Span('test-id',
                         'test-parent-id',
                         'test-span-id',
                         'test')
        self.test_span_observer = TraceSpanObserver('test-service',
                                                    self.span,
                                                    self.recorder)

    def test_serialize_uses_span_info(self):
        serialized_span = self.test_span_observer._serialize()
        self.assertEqual(serialized_span['traceId'], self.span.trace_id)
        self.assertEqual(serialized_span['name'], self.span.name)
        self.assertEqual(serialized_span['id'], self.span.id)

    def test_serialize_adds_cs(self):
        serialized_span = self.test_span_observer._serialize()
        cs_in_annotation = False
        for annotation in serialized_span['annotations']:
            if annotation['value'] == 'cs':
                cs_in_annotation = True
        self.assertTrue(cs_in_annotation)

    def test_serialize_adds_cr(self):
        serialized_span = self.test_span_observer._serialize()
        cr_in_annotation = False
        for annotation in serialized_span['annotations']:
            if annotation['value'] == 'cr':
                cr_in_annotation = True
        self.assertTrue(cr_in_annotation)

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

    def test_to_span_obj_sets_parent_id(self):
        span_obj = self.test_span_observer._to_span_obj([], [])
        self.assertEqual(span_obj['parentId'], self.span.parent_id)

    def test_to_span_obj_sets_default_parent_id(self):
        self.span.parent_id = None
        span_obj = self.test_span_observer._to_span_obj([], [])
        self.assertEqual(span_obj['parentId'], 0)


class TraceServerSpanObserverTests(unittest.TestCase):
    def setUp(self):
        self.recorder = NullRecorder()
        self.span = ServerSpan('test-id',
                               'test-parent-id',
                               'test-span-id',
                               'test')
        self.test_server_span_observer = TraceServerSpanObserver('test-service',
                                                                 self.span,
                                                                 self.recorder)

    def test_server_span_observer_inherits_span_observer(self):
        # in case of future refactoring
        self.assertTrue(isinstance(self.test_server_span_observer,
                                   TraceSpanObserver))
        self.assertTrue(issubclass(TraceServerSpanObserver,
                                   TraceSpanObserver))

    def test_serialize_uses_span_info(self):
        serialized_span = self.test_server_span_observer._serialize()
        self.assertEqual(serialized_span['traceId'], self.span.trace_id)
        self.assertEqual(serialized_span['name'], self.span.name)
        self.assertEqual(serialized_span['id'], self.span.id)

    def test_serialize_adds_ss(self):
        serialized_span = self.test_server_span_observer._serialize()
        ss_in_annotation = False
        for annotation in serialized_span['annotations']:
            if annotation['value'] == 'ss':
                ss_in_annotation = True
        self.assertTrue(ss_in_annotation)

    def test_serialize_adds_sr(self):
        serialized_span = self.test_server_span_observer._serialize()
        sr_in_annotation = False
        for annotation in serialized_span['annotations']:
            if annotation['value'] == 'sr':
                sr_in_annotation = True
        self.assertTrue(sr_in_annotation)
