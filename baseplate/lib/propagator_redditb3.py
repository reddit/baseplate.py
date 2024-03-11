import logging

from re import compile as re_compile
from typing import Any
from typing import Iterable
from typing import Optional
from typing import Set

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import CarrierT
from opentelemetry.propagators.textmap import default_getter
from opentelemetry.propagators.textmap import default_setter
from opentelemetry.propagators.textmap import Getter
from opentelemetry.propagators.textmap import Setter
from opentelemetry.propagators.textmap import TextMapPropagator
from opentelemetry.trace import format_trace_id

logger = logging.getLogger(__name__)


class RedditB3Format(TextMapPropagator):
    """Propagator for the Reddit B3 HTTP header format."""

    TRACE_ID_KEY = "X-Trace"
    SPAN_ID_KEY = "X-Span"
    SAMPLED_KEY = "X-Sampled"
    FLAGS_KEY = "X-Flags"
    _SAMPLE_PROPAGATE_VALUES = {"1", "True", "true", "d"}
    # Reddit B3 uses integers.
    _trace_id_regex = re_compile(r"\d+")
    _span_id_regex = re_compile(r"\d+")

    def extract(
        self,
        carrier: CarrierT,
        context: Optional[Context] = None,
        getter: Getter = default_getter,
    ) -> Context:
        if context is None:
            context = Context()

        extracted_trace_id = _extract_first_element(getter.get(carrier, self.TRACE_ID_KEY))
        logger.debug(
            "Extracted trace_id from carrier. [carrier=%s, context=%s, trace_id=%s]",
            carrier,
            context,
            extracted_trace_id,
        )
        extracted_span_id = _extract_first_element(getter.get(carrier, self.SPAN_ID_KEY))
        logger.debug(
            "Extracted span_id from carrier. [carrier=%s, context=%s, span_id=%s]",
            carrier,
            context,
            extracted_span_id,
        )
        sampled = _extract_first_element(getter.get(carrier, self.SAMPLED_KEY), default="0")
        logger.debug(
            "Extracted sampled from carrier. [carrier=%s, context=%s, sampled=%s]",
            carrier,
            context,
            sampled,
        )
        flags = _extract_first_element(getter.get(carrier, self.FLAGS_KEY))
        logger.debug(
            "Extracted flags from carrier. [carrier=%s, context=%s, flags=%s]",
            carrier,
            context,
            flags,
        )

        # If we receive an invalid `trace_id` according to the w3 spec we return an empty context.
        if (
            extracted_trace_id is None
            or self._trace_id_regex.fullmatch(extracted_trace_id) is None
            or extracted_span_id is None
            or self._span_id_regex.fullmatch(extracted_span_id) is None
        ):
            logger.debug(
                "No valid b3 traces headers in request. Aborting. [carrier=%s, context=%s, trace_id=%s, span_id=%s]",
                carrier,
                context,
                extracted_trace_id,
                extracted_span_id,
            )
            return context

        trace_id = int(extracted_trace_id)
        span_id = int(extracted_span_id)
        logger.debug(
            "Converted IDs to integers. [carrier=%s, context=%s, trace_id=%s, span_id=%s]",
            carrier,
            context,
            trace_id,
            span_id,
        )
        options = 0
        # The b3 spec provides no defined behavior for both sample and
        # flag values set. Since the setting of at least one implies
        # the desire for some form of sampling, propagate if either
        # header is set to allow.
        if sampled in self._SAMPLE_PROPAGATE_VALUES or flags == "1":
            options |= trace.TraceFlags.SAMPLED
            logger.debug("Set trace to sampled. [carrier=%s, context=%s]", carrier, context)

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
        context: Optional[Context] = None,
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
            str(int(format_trace_id(span_context.trace_id)[-16:], 16)),
        )
        setter.set(carrier, self.SPAN_ID_KEY, str(span_context.span_id))
        setter.set(carrier, self.SAMPLED_KEY, "1" if sampled else "0")

    @property
    def fields(self) -> Set[str]:
        return {
            self.TRACE_ID_KEY,
            self.SPAN_ID_KEY,
            self.SAMPLED_KEY,
        }


def _extract_first_element(
    items: Optional[Iterable[CarrierT]],
    default: Optional[Any] = None,
) -> Optional[CarrierT]:
    if items is None:
        return default
    return next(iter(items), default)
