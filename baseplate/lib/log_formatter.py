from opentelemetry import trace
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def process_log_record(self, log_record: dict) -> dict:
        log_record["level"] = log_record.pop("levelname", None)
        try:
            span = trace.get_current_span()
            if span.is_recording():
                log_record["traceID"] = trace.format_trace_id(span.get_span_context().trace_id)
        except (KeyError, ValueError, TypeError):
            pass
        return super().process_log_record(log_record)
