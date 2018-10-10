"""Thrift integration for Baseplate.

This module provides an implementation of :py:class:`TProcessorEventHandler`
which integrates Baseplate's facilities into the Thrift request lifecycle.

An abbreviated example of it in use::

    def make_processor(app_config):
        baseplate = Baseplate()

        handler = MyHandler()
        processor = my_thrift.MyService.ContextProcessor(handler)
        return baseplateify_processor(processor, baseplate)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from thrift.Thrift import TProcessorEventHandler

from ...core import TraceInfo


TRACE_HEADER_NAMES = {
    "trace_id": (b"Trace", b"B3-TraceId"),
    "span_id": (b"Span", b"B3-SpanId"),
    "parent_span_id": (b"Parent", b"B3-ParentSpanId"),
    "sampled": (b"Sampled", b"B3-Sampled"),
    "flags": (b"Flags", b"B3-Flags"),
}


class RequestContext(object):
    pass


class ContextAwareHandler(object):
    def __init__(self, handler, context):
        self.handler = handler
        self.context = context

    def __getattr__(self, fn_name):
        def call_with_context(self, *args, **kwargs):
            handler_fn = getattr(self.handler, fn_name)
            with self.context.trace:
                return handler_fn(self.context, *args, **kwargs)
        return call_with_context


def _extract_trace_info(headers):
    extracted_values = TraceInfo.extract_upstream_header_values(TRACE_HEADER_NAMES, headers)
    flags = extracted_values.get("flags", None)
    return TraceInfo.from_upstream(
        int(extracted_values["trace_id"]),
        int(extracted_values["parent_span_id"]),
        int(extracted_values["span_id"]),
        True if extracted_values["sampled"].decode("utf-8") == "1" else False,
        int(flags) if flags is not None else None,
    )


def baseplateify_processor(processor, baseplate, edge_context_factory=None):
    instrumented_process_map = {}
    for fn_name, processor_fn in processor._processMap.items():
        def call_processor_with_span_context(self, seqid, iprot, oprot):
            context = RequestContext()

            headers = iprot.get_headers()
            try:
                trace_info = _extract_trace_info(headers)
                edge_payload = headers.get(b"Edge-Request", None)
                if edge_context_factory:
                    edge_context = edge_context_factory.from_upstream(edge_payload)
                    edge_context.attach_context(context)
                else:
                    # just attach the raw context so it gets passed on
                    # downstream even if we don't know how to handle it.
                    context.raw_request_context = edge_payload
            except (KeyError, ValueError):
                pass

            server_span = baseplate.make_server_span(
                context,
                name=fn_name,
                trace_info=trace_info,
            )

            context.headers = headers
            context.trace = server_span

            handler = processor._handler
            context_aware_handler = ContextAwareHandler(handler, context)
            context_aware_processor = processor.__class__(context_aware_handler)
            return processor_fn(context_aware_processor, seqid, iprot, oprot)

        instrumented_process_map[fn_name] = call_processor_with_span_context
    processor._processMap = instrumented_process_map
    processor.baseplate = baseplate
    return processor
