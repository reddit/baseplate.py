"""Pyramid integration for Baseplate.

This module provides a configuration extension for Pyramid which integrates
Baseplate's facilities into the Pyramid WSGI request lifecycle.

An abbreviated example of it in use::

    def make_app(app_config):
        configurator = Configurator()

        baseplate = Baseplate()
        baseplate_config = BaseplateConfigurator(baseplate)
        configurator.include(baseplate_config.includeme)

        return configurator.make_wsgi_app()

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from pyramid.events import ContextFound, NewResponse


class BaseplateConfigurator(object):
    """Config extension to integrate Baseplate into Pyramid.

    :param baseplate.core.Baseplate baseplate: The Baseplate instance for your
        application.

    """

    def __init__(self, baseplate):
        self.baseplate = baseplate

    def _on_new_request(self, event):
        request = event.request

        # this request didn't match a route we know
        if not request.matched_route:
            # TODO: some metric for 404s would be good
            return

        trace_id = request.headers.get("X-Trace", "no-trace")
        parent_id = request.headers.get("X-Parent", "no-parent")
        span_id = request.headers.get("X-Span", "no-span")

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
