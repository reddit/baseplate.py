import logging
import unittest

import gevent
from gevent import select
from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from baseplate.lib.tracing import patch_greenlet_tracing, unpatch_greenlet_tracing

logger = logging.getLogger(__name__)


class PatchedTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        patch_greenlet_tracing()

    def tearDown(self):
        super().tearDown()
        unpatch_greenlet_tracing()


class TestGevent(PatchedTestCase, TestBase):
    def test_context_with_patch(self):
        """Trace context is passed to greenlets"""

        def gr1():
            with trace.get_tracer("gr1").start_as_current_span("child"):
                select.select([], [], [], 2)

        with trace.get_tracer(__name__).start_as_current_span("parent"):
            gevent.joinall(
                [
                    gevent.spawn(gr1),
                ]
            )

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].parent.span_id, finished_spans[1].context.span_id)

    def test_context_without_patch(self):
        """Trace context is not passed if we explicitly don't patch"""
        unpatch_greenlet_tracing()

        def gr1():
            with trace.get_tracer("gr1").start_as_current_span("child"):
                select.select([], [], [], 2)

        with trace.get_tracer(__name__).start_as_current_span("parent"):
            gevent.joinall(
                [
                    gevent.spawn(gr1),
                ]
            )

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertIsNone(finished_spans[0].parent)
