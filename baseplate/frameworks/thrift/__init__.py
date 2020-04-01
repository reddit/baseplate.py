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
from baseplate.lib.edge_context import EdgeRequestContextFactory


class _ContextAwareHandler:
    def __init__(self, handler: Any, context: RequestContext, logger: Logger):
        self.handler = handler
        self.context = context
        self.logger = logger

    def __getattr__(self, fn_name: str) -> Callable[..., Any]:
        def call_with_context(*args: Any, **kwargs: Any) -> Any:
            self.logger.debug("Handling: %r", fn_name)

            handler_fn = getattr(self.handler, fn_name)

            span = self.context.trace
            try:
                span.start()
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
            except:  # noqa: E722
                # the handler crashed (or timed out)!
                span.finish(exc_info=sys.exc_info())
                raise
            else:
                # a normal result
                span.finish()
                return result

        return call_with_context


def baseplateify_processor(
    processor: TProcessor,
    logger: Logger,
    baseplate: Baseplate,
    edge_context_factory: Optional[EdgeRequestContextFactory] = None,
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
                    int(headers[b"Trace"]),
                    int(headers[b"Parent"]),
                    int(headers[b"Span"]),
                    sampled,
                    int(flags) if flags is not None else None,
                )
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

            span = baseplate.make_server_span(context, name=fn_name, trace_info=trace_info)

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
