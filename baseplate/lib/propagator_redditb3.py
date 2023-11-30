import typing
from re import compile as re_compile

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import (
    CarrierT,
    Getter,
    Setter,
    TextMapPropagator,
    default_getter,
    default_setter,
)
from opentelemetry.trace import format_span_id, format_trace_id


class RedditB3Format(TextMapPropagator):
    """Propagator for the Reddit B3 HTTP header format."""

    TRACE_ID_KEY = "X-Trace"
    SPAN_ID_KEY = "X-Span"
    SAMPLED_KEY = "X-Sampled"
    FLAGS_KEY = "X-Flags"
    _SAMPLE_PROPAGATE_VALUES = {"1", "True", "true", "d"}
    # Although Reddit B3 uses 64bit TraceId's (16 char) we will accept 32 and truncate only when writing client headers.
    _trace_id_regex = re_compile(r"[\da-fA-F]{16}|[\da-fA-F]{32}")
    _span_id_regex = re_compile(r"[\da-fA-F]{16}")

    def extract(
        self,
        carrier: CarrierT,
        context: typing.Optional[Context] = None,
        getter: Getter = default_getter,
    ) -> Context:
        if context is None:
            context = Context()
        trace_id = trace.INVALID_TRACE_ID
        span_id = trace.INVALID_SPAN_ID
        sampled = "0"
        flags = None

        trace_id = (
            _extract_first_element(getter.get(carrier, self.TRACE_ID_KEY))
            or trace_id
        )
        span_id = (
            _extract_first_element(getter.get(carrier, self.SPAN_ID_KEY))
            or span_id
        )
        sampled = (
            _extract_first_element(getter.get(carrier, self.SAMPLED_KEY))
            or sampled
        )
        flags = (
            _extract_first_element(getter.get(carrier, self.FLAGS_KEY))
            or flags
        )

        if (
            trace_id == trace.INVALID_TRACE_ID
            or span_id == trace.INVALID_SPAN_ID
            or self._trace_id_regex.fullmatch(trace_id) is None
            or self._span_id_regex.fullmatch(span_id) is None
        ):
            return context

        trace_id = int(trace_id, 16)
        span_id = int(span_id, 16)
        options = 0
        # The b3 spec provides no defined behavior for both sample and
        # flag values set. Since the setting of at least one implies
        # the desire for some form of sampling, propagate if either
        # header is set to allow.
        if sampled in self._SAMPLE_PROPAGATE_VALUES or flags == "1":
            options |= trace.TraceFlags.SAMPLED

        return trace.set_span_in_context(
            trace.NonRecordingSpan(
                trace.SpanContext(
                    # trace an span ids are encoded in hex, so must be converted
                    trace_id=trace_id,
                    span_id=span_id,
                    is_remote=True,
                    trace_flags=trace.TraceFlags(options),
                    trace_state=trace.TraceState(),
                )
            ),
            context,
        )

    def inject(
        self,
        carrier: CarrierT,
        context: typing.Optional[Context] = None,
        setter: Setter = default_setter,
    ) -> None:
        span = trace.get_current_span(context=context)

        span_context = span.get_span_context()
        if span_context == trace.INVALID_SPAN_CONTEXT:
            return

        sampled = (trace.TraceFlags.SAMPLED & span_context.trace_flags) != 0
        setter.set(
            carrier,
            self.TRACE_ID_KEY,
            # The Reddit B3 format expects a 64bit TraceId not a 128bit ID. This is truncated for compatibility.
            format_trace_id(span_context.trace_id)[16:],
        )
        setter.set(
            carrier, self.SPAN_ID_KEY, format_span_id(span_context.span_id)
        )
        setter.set(carrier, self.SAMPLED_KEY, "1" if sampled else "0")

    @property
    def fields(self) -> typing.Set[str]:
        return {
            self.TRACE_ID_KEY,
            self.SPAN_ID_KEY,
            self.SAMPLED_KEY,
        }


def _extract_first_element(
    items: typing.Iterable[CarrierT],
    default: Optional[Any] = None,
) -> typing.Optional[CarrierT]:
    if items is None:
        return default
    return next(iter(items), default)