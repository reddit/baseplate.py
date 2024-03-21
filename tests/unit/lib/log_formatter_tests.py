from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from baseplate.lib import log_formatter

tracer = trace.get_tracer(__name__)


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

        context = {}
        ctx = trace.SpanContext(
            trace_id=0x00C2ACCDF122E659ABEC55FA2DE925D3,
            span_id=0x6E0C63257DE34C92,
            trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
            is_remote=False,
        )
        parent = trace.set_span_in_context(trace.NonRecordingSpan(ctx), context)

        with tracer.start_as_current_span("testing trace ID logging", context=parent) as otelspan:
            assert otelspan.is_recording()
            assert otelspan.get_span_context().trace_id == 0xC2ACCDF122E659ABEC55FA2DE925D3
            assert formatter.process_log_record(data) == {
                "level": "INFO",
                "traceID": "00c2accdf122e659abec55fa2de925d3",
            }
