import logging
import typing

from re import compile as re_compile

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import CarrierT
from opentelemetry.propagators.textmap import default_getter
from opentelemetry.propagators.textmap import default_setter
from opentelemetry.propagators.textmap import Getter
from opentelemetry.propagators.textmap import Setter
from opentelemetry.propagators.textmap import TextMapPropagator
from opentelemetry.trace import format_span_id
from opentelemetry.trace import format_trace_id

logger = logging.getLogger(__name__)


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

        trace_id = _extract_first_element(getter.get(carrier, self.TRACE_ID_KEY), default=trace_id)
        logger.debug(f"Extracted trace_id from carrier. [{carrier=}, {context=}, {trace_id=}]")
        span_id = _extract_first_element(getter.get(carrier, self.SPAN_ID_KEY), default=span_id)
        logger.debug(f"Extracted span_id from carrier. [{carrier=}, {context=}, {span_id=}]")
        sampled = _extract_first_element(getter.get(carrier, self.SAMPLED_KEY), default=sampled)
        logger.debug(f"Extracted sampled from carrier. [{carrier=}, {context=}, {sampled=}]")
        flags = _extract_first_element(getter.get(carrier, self.FLAGS_KEY), default=flags)
        logger.debug(f"Extracted flags from carrier. [{carrier=}, {context=}, {flags=}]")

        # If we receive an invalid `trace_id` according to the w3 spec we return an empty context.
        if (
            trace_id == trace.INVALID_TRACE_ID
            or span_id == trace.INVALID_SPAN_ID
            or self._trace_id_regex.fullmatch(trace_id) is None
            or self._span_id_regex.fullmatch(span_id) is None
        ):
            logger.debug(
                f"No valid b3 traces headers in request. Aborting. [{carrier=}, {context=}, {trace_id=}, {span_id=}]"
            )
            return context

        # trace and span ids are encoded in hex, so must be converted
        trace_id = int(trace_id, 16)
        span_id = int(span_id, 16)
        logger.debug(
            f"Converted IDs to integers. [{carrier=}, {context=}, {trace_id=}, {span_id=}]"
        )
        options = 0
        # The b3 spec provides no defined behavior for both sample and
        # flag values set. Since the setting of at least one implies
        # the desire for some form of sampling, propagate if either
        # header is set to allow.
        if sampled in self._SAMPLE_PROPAGATE_VALUES or flags == "1":
            options |= trace.TraceFlags.SAMPLED
            logger.debug(f"Set trace to sampled. [{carrier=}, {context=}]")

        return trace.set_span_in_context(
            trace.NonRecordingSpan(
                trace.SpanContext(
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
            format_trace_id(span_context.trace_id)[-16:],
        )
        setter.set(carrier, self.SPAN_ID_KEY, format_span_id(span_context.span_id))
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
    default: typing.Optional[typing.Any] = None,
) -> typing.Optional[CarrierT]:
    if items is None:
        return default
    return next(iter(items), default)
