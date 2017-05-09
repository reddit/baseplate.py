"""Thrift integration for Baseplate.

This module provides an implementation of :py:class:`TProcessorEventHandler`
which integrates Baseplate's facilities into the Thrift request lifecycle.

An abbreviated example of it in use::

    def make_processor(app_config):
        baseplate = Baseplate()

        handler = MyHandler()
        processor = my_thrift.MyService.ContextProcessor(handler)

        event_handler = BaseplateProcessorEventHandler(logger, baseplate)
        processor.setEventHandler(event_handler)

        return processor

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys

from thrift.Thrift import TProcessorEventHandler

from ...core import TraceInfo


class RequestContext(object):
    pass


# TODO: exceptions in the event handler cause the connection to be abruptly
# closed with no diagnostics sent to the client. that should be more obvious.
class BaseplateProcessorEventHandler(TProcessorEventHandler):
    """Processor event handler for Baseplate.

    :param logging.Logger logger: The logger to use for error and debug logging.
    :param baseplate.core.Baseplate baseplate: The baseplate instance for your
        application.

    """
    def __init__(self, logger, baseplate):
        self.logger = logger
        self.baseplate = baseplate

    def getHandlerContext(self, fn_name, server_context):
        context = RequestContext()

        trace_info = None
        headers = server_context.iprot.trans.get_headers()
        try:
            sampled = headers.get(b"Sampled", None)
            if sampled is not None:
                sampled = True if sampled.decode('utf-8') == "1" else False
            flags = headers.get(b"Flags", None)
            if flags is not None:
                flags = int(flags)

            trace_info = TraceInfo.from_upstream(
                trace_id=int(headers[b"Trace"]),
                parent_id=int(headers[b"Parent"]),
                span_id=int(headers[b"Span"]),
                sampled=sampled,
                flags=flags,
            )
        except (KeyError, ValueError):
            pass

        trace = self.baseplate.make_server_span(
            context,
            name=fn_name,
            trace_info=trace_info,
        )

        try:
            peer_address, peer_port = server_context.getPeerName()
        except AttributeError:
            # the client transport is not a socket
            pass
        else:
            trace.set_tag("peer.ipv4", peer_address)
            trace.set_tag("peer.port", peer_port)

        context.headers = headers
        context.trace = trace
        return context

    def postRead(self, handler_context, fn_name, args):
        self.logger.debug("Handling: %r", fn_name)
        handler_context.trace.start()

    def handlerDone(self, handler_context, fn_name, result):
        if not getattr(handler_context.trace, "is_finished", False):
            # for unexpected exceptions, we call trace.finish() in handlerError
            handler_context.trace.finish()

    def handlerError(self, handler_context, fn_name, exception):
        handler_context.trace.finish(exc_info=sys.exc_info())
        handler_context.trace.is_finished = True
        self.logger.exception("Unexpected exception in %r.", fn_name)
