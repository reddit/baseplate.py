import unittest

from opentelemetry import propagate
from opentelemetry import trace
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from baseplate.lib import log_formatter
from baseplate.lib.propagator_redditb3 import RedditB3Format

tracer = trace.get_tracer(__name__)

propagate.set_global_textmap(
    CompositePropagator([RedditB3Format(), TraceContextTextMapPropagator()])
)


class CustomJSONFormatterTests(TestBase):
    def test_dict_with_correct_key(self):
        formatter = log_formatter.CustomJsonFormatter("")
        data = {"levelname": "foo"}
        assert formatter.process_log_record(data) == {"level": "foo"}

    def test_dict_without_correct_key(self):
        formatter = log_formatter.CustomJsonFormatter("")
        data = {"levelno": 1}
        assert formatter.process_log_record(data) == {"level": None, "levelno": 1}

    def test_wrong_type(self):
        formatter = log_formatter.CustomJsonFormatter("")
        with self.assertRaises(AttributeError):
            formatter.process_log_record("foo")

    def test_dict_recording_span(self):
        formatter = log_formatter.CustomJsonFormatter("")
        data = {"levelname": "INFO"}

        with tracer.start_as_current_span("testing trace ID logging") as otelspan:
            assert otelspan.is_recording()
            trace_id = hex(otelspan.get_span_context().trace_id)[2:]
            assert formatter.process_log_record(data) == {"level": "INFO", "traceID": trace_id}
