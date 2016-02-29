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

import random

from pyramid.events import ContextFound, NewResponse


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

        if self.trust_trace_headers:
            trace_id = request.headers.get("X-Trace", "no-trace")
            parent_id = request.headers.get("X-Parent", "no-parent")
            span_id = request.headers.get("X-Span", "no-span")
        else:
            trace_id = random.getrandbits(64)
            parent_id = None
            span_id = trace_id

        request.trace = self.baseplate.make_root_span(
            request,
            trace_id=trace_id,
            parent_id=parent_id,
            span_id=span_id,
            name=request.matched_route.name,
        )
        request.trace.start()

    def _on_new_response(self, event):
        if not event.request.matched_route:
            return

        event.request.trace.stop()

    def includeme(self, config):
        config.add_subscriber(self._on_new_request, ContextFound)
        config.add_subscriber(self._on_new_response, NewResponse)
