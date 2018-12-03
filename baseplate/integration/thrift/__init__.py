"""Thrift integration for Baseplate.

This module provides a wrapper for a :py:class:`TProcessor` which integrates
Baseplate's facilities into the Thrift request lifecycle.

An abbreviated example of it in use::

    def make_processor(app_config):
        baseplate = Baseplate()

        handler = MyHandler()
        processor = my_thrift.MyService.Processor(handler)
        return baseplateify_processor(processor, baseplate)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys

from thrift.Thrift import TException, TApplicationException
from thrift.transport.TTransport import TTransportException
from thrift.protocol.TProtocol import TProtocolException

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


class _ContextAwareHandler(object):
    def __init__(self, handler, context, logger):
        self.handler = handler
        self.context = context
        self.logger = logger

    def __getattr__(self, fn_name):
        def call_with_context(*args, **kwargs):
            self.logger.debug("Handling: %r", fn_name)

            handler_fn = getattr(self.handler, fn_name)

            span = self.context.trace
            span.start()
            try:
                result = handler_fn(self.context, *args, **kwargs)
            except (TApplicationException, TProtocolException, TTransportException):
                # these are subclasses of TException but aren't ones that
                # should be expected in the protocol
                span.finish(exc_info=sys.exc_info())
                raise
            except TException:
                # this is an expected exception, as defined in the IDL
                span.finish()
                raise
            except Exception:
                # the handler crashed!
                span.finish(exc_info=sys.exc_info())
                raise
            else:
                # a normal result
                span.finish()
                return result
        return call_with_context


def _extract_trace_info(headers):
    extracted_values = TraceInfo.extract_upstream_header_values(TRACE_HEADER_NAMES, headers)
    sampled = bool(extracted_values.get("sampled") == b"1")
    flags = extracted_values.get("flags", None)
    return TraceInfo.from_upstream(
        int(extracted_values["trace_id"]),
        int(extracted_values["parent_span_id"]),
        int(extracted_values["span_id"]),
        sampled,
        int(flags) if flags is not None else None,
    )


def baseplateify_processor(processor, logger, baseplate, edge_context_factory=None):
    """Wrap a Thrift Processor with Baseplate's span lifecycle.

    :param thrift.Thrift.TProcessor processor: The service's processor to wrap.
    :param logging.Logger logger: The logger to use for error and debug
        logging.
    :param baseplate.core.Baseplate baseplate: The baseplate instance for your
        application.
    :param baseplate.core.EdgeRequestContextFactory edge_context_factory: A
        configured factory for handling edge request context.

    """
    def make_processor_fn(fn_name, processor_fn):
        def call_processor_with_span_context(self, seqid, iprot, oprot):
            context = RequestContext()

            headers = iprot.get_headers()
            try:
                trace_info = _extract_trace_info(headers)
            except (KeyError, ValueError):
                trace_info = None

            edge_payload = headers.get(b"Edge-Request", None)
            if edge_context_factory:
                edge_context = edge_context_factory.from_upstream(edge_payload)
                edge_context.attach_context(context)
            else:
                # just attach the raw context so it gets passed on
                # downstream even if we don't know how to handle it.
                context.raw_request_context = edge_payload

            server_span = baseplate.make_server_span(
                context,
                name=fn_name,
                trace_info=trace_info,
            )

            context.headers = headers
            context.trace = server_span

            handler = processor._handler
            context_aware_handler = _ContextAwareHandler(handler, context, logger)
            context_aware_processor = processor.__class__(context_aware_handler)
            return processor_fn(context_aware_processor, seqid, iprot, oprot)
        return call_processor_with_span_context

    instrumented_process_map = {}
    for fn_name, processor_fn in processor._processMap.items():
        context_aware_processor_fn = make_processor_fn(fn_name, processor_fn)
        instrumented_process_map[fn_name] = context_aware_processor_fn
    processor._processMap = instrumented_process_map
    processor.baseplate = baseplate
    return processor
