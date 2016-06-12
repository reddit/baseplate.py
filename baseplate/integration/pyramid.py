"""Pyramid integration for Baseplate.

This module provides a configuration extension for Pyramid which integrates
Baseplate's facilities into the Pyramid WSGI request lifecycle.

An abbreviated example of it in use::

    def make_app(app_config):
        configurator = Configurator()

        baseplate = Baseplate()
        baseplate_config = BaseplateConfigurator(
            baseplate,
            trust_trace_headers=True,
        )
        configurator.include(baseplate_config.includeme)

        return configurator.make_wsgi_app()

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from pyramid.events import ContextFound, NewResponse

from ..core import TraceInfo


class BaseplateConfigurator(object):
    """Config extension to integrate Baseplate into Pyramid.

    :param baseplate.core.Baseplate baseplate: The Baseplate instance for your
        application.
    :param bool trust_trace_headers: Should this app trust trace headers from
        the client? If ``False``, trace IDs will be generated for each request.

    .. warning::

        Do not set ``trust_trace_headers`` to ``True`` unless you are sure your
        application is only accessible by trusted sources (usually backend-only
        services).

    """

    # TODO: remove the default on trust_trace_headers once apps are updated
    def __init__(self, baseplate, trust_trace_headers=False):
        self.baseplate = baseplate
        self.trust_trace_headers = trust_trace_headers

    def _on_new_request(self, event):
        request = event.request

        # this request didn't match a route we know
        if not request.matched_route:
            # TODO: some metric for 404s would be good
            return

        trace_info = None
        if self.trust_trace_headers:
            try:
                trace_info = TraceInfo.from_upstream(
                    trace_id=int(request.headers["X-Trace"]),
                    parent_id=int(request.headers["X-Parent"]),
                    span_id=int(request.headers["X-Span"]),
                )
            except (KeyError, ValueError):
                pass

        request.trace = self.baseplate.make_root_span(
            request,
            name=request.matched_route.name,
            trace_info=trace_info,
        )
        request.trace.start()

    def _on_new_response(self, event):
        if not event.request.matched_route:
            return

        event.request.trace.stop()

    def includeme(self, config):
        config.add_subscriber(self._on_new_request, ContextFound)
        config.add_subscriber(self._on_new_response, NewResponse)
