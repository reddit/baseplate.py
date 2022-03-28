import sys

from logging import Logger
from typing import Any
from typing import Callable
from typing import Mapping
from typing import Optional

from requests.structures import CaseInsensitiveDict
from thrift.protocol.TProtocol import TProtocolBase
from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
from thrift.Thrift import TProcessor
from thrift.transport.TTransport import TTransportException

from baseplate import Baseplate
from baseplate import RequestContext
from baseplate import TraceInfo
from baseplate.lib.edgecontext import EdgeContextFactory
from baseplate.thrift.ttypes import Error as bp_error
from baseplate.thrift.ttypes import ErrorCode


class _ContextAwareHandler:
    def __init__(self, handler: Any, context: RequestContext, logger: Logger):
        self.handler = handler
        self.context = context
        self.logger = logger

    def __getattr__(self, fn_name: str) -> Callable[..., Any]:
        def call_with_context(*args: Any, **kwargs: Any) -> Any:
            self.logger.debug("Handling: %r", fn_name)

            handler_fn = getattr(self.handler, fn_name)

            span = self.context.span
            span.set_tag("thrift.method", fn_name)
            try:
                span.start()
                result = handler_fn(self.context, *args, **kwargs)
            except (TApplicationException, TProtocolException, TTransportException):
                # these are subclasses of TException but aren't ones that
                # should be expected in the protocol
                span.finish(exc_info=sys.exc_info())
                raise
            except TException:
                name, code, status = processException(sys.exc_info())
                span.set_tag("exception_type", name)
                span.set_tag("thrift.status_code", code)
                span.set_tag("thrift.status", status)
                span.set_tag("success", "false")
                # this is an expected exception, as defined in the IDL
                span.finish()
                raise
            except:  # noqa: E722
                # the handler crashed (or timed out)!
                span.finish(exc_info=sys.exc_info())
                raise
            else:
                # a normal result
                span.finish()
                return result

        return call_with_context


def processException(exc_info):
    """
    processException attempts to get additional information from the
    exception info. If the exception is a baseplate thrift Error type
    then the status code and status is also returned.
    The code is the numeric value from a baseplate.Error, for example: "404".
    The status is the human-readable status, for example: "NOT_FOUND".
    """
    exc_class = exc_info[0]
    exc = exc_info[1]
    code, status = "", ""
    name = exc_class.__name__

    if issubclass(exc_class, bp_error):
        code = exc.code
        c = ErrorCode()
        status = c._VALUES_TO_NAMES.get(code, "")

    return name, code, status


def baseplateify_processor(
    processor: TProcessor,
    logger: Logger,
    baseplate: Baseplate,
    edge_context_factory: Optional[EdgeContextFactory] = None,
) -> TProcessor:
    """Wrap a Thrift Processor with Baseplate's span lifecycle.

    :param processor: The service's processor to wrap.
    :param logger: The logger to use for error and debug logging.
    :param baseplate: The baseplate instance for your application.
    :param edge_context_factory: A configured factory for handling edge request
        context.

    """

    def make_processor_fn(fn_name: str, processor_fn: Callable[..., Any]) -> Callable[..., Any]:
        def call_processor_with_span_context(
            self: Any, seqid: int, iprot: TProtocolBase, oprot: TProtocolBase
        ) -> Any:
            context = baseplate.make_context_object()

            # Allow case-insensitivity for THeader headers
            headers: Mapping[bytes, bytes] = CaseInsensitiveDict(  # type: ignore
                data=iprot.get_headers()
            )

            trace_info: Optional[TraceInfo]
            try:
                sampled = bool(headers.get(b"Sampled") == b"1")
                flags = headers.get(b"Flags", None)
                trace_info = TraceInfo.from_upstream(
                    headers[b"Trace"].decode(),
                    headers[b"Parent"].decode(),
                    headers[b"Span"].decode(),
                    sampled,
                    int(flags) if flags is not None else None,
                )
            except (KeyError, ValueError):
                trace_info = None

            edge_payload = headers.get(b"Edge-Request", None)
            context.raw_edge_context = edge_payload
            if edge_context_factory:
                context.edge_context = edge_context_factory.from_upstream(edge_payload)

            try:
                raw_deadline_budget = headers[b"Deadline-Budget"].decode()
                context.deadline_budget = float(raw_deadline_budget) / 1000
            except (KeyError, ValueError):
                context.deadline_budget = None

            span = baseplate.make_server_span(context, name=fn_name, trace_info=trace_info)
            span.set_tag("protocol", "thrift")
            try:
                service_name = headers[b"User-Agent"].decode()
            except (KeyError, UnicodeDecodeError):
                pass
            else:
                span.set_tag("peer.service", service_name)

            context.headers = headers

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
