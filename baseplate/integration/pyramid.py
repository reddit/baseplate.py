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

.. warning::

    Because of how Baseplate instruments Pyramid, you should not make an
    `exception view`_ that handles the base :py:exc:`Exception` as this will
    prevent Baseplate from seeing the unhandled error and reporting it
    appropriately.

    .. _exception view: https://docs.pylonsproject.org/projects/pyramid_cookbook/en/latest/pylons/exceptions.html#exception-views

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys

import pyramid.events
import pyramid.tweens

from ..core import TraceInfo
from ..server import make_app


def _make_baseplate_tween(handler, registry):
    def baseplate_tween(request):
        try:
            response = handler(request)
        except:
            if hasattr(request, "trace"):
                request.trace.finish(exc_info=sys.exc_info())
            raise
        else:
            if hasattr(request, "trace"):
                request.trace.set_tag("http.status_code", response.status_code)
                request.trace.finish()
        return response
    return baseplate_tween


class BaseplateEvent(object):

    def __init__(self, request):
        self.request = request


class ServerSpanInitialized(BaseplateEvent):
    """Event that Baseplate fires after creating the ServerSpan for a Request.

    This event will be emitted before the Request is passed along to it's
    handler.  Baseplate initializes the ServerSpan in response to a
    :py:class:`pyramid.events.ContextFound` event emitted by Pyramid so while
    we can guarantee what Baseplate has done when this event is emitted, we
    cannot guarantee that any other subscribers to
    :py:class:`pyramid.events.ContextFound` have been called or not.
    """
    pass


# pylint: disable=abstract-class-not-used
class BaseplateConfigurator(object):
    """Config extension to integrate Baseplate into Pyramid.

    :param baseplate.core.Baseplate baseplate: The Baseplate instance for your
        application.
    :param bool trust_trace_headers: Should this application trust trace
        headers from the client? If ``True``, trace headers in inbound requests
        will be used for the server span. If ``False``, new random trace IDs
        will be generated for each request.
    :param baseplate.core.EdgeRequestContextFactory edge_context_factory: A
        configured factory for handling edge request context.

    .. warning::

        Do not set ``trust_trace_headers`` to ``True`` unless you are sure your
        application is only accessible by trusted sources (usually backend-only
        services).

    """

    # TODO: remove the default on trust_trace_headers once apps are updated
    def __init__(self, baseplate, trust_trace_headers=False,
                 edge_context_factory=None):
        self.baseplate = baseplate
        self.trust_trace_headers = trust_trace_headers
        self.edge_context_factory = edge_context_factory

    def _on_new_request(self, event):
        request = event.request

        # this request didn't match a route we know
        if not request.matched_route:
            # TODO: some metric for 404s would be good
            return

        trace_info = None
        if self.trust_trace_headers:
            try:
                sampled = request.headers.get("X-Sampled", None)
                if sampled is not None:
                    sampled = True if sampled == "1" else False
                flags = request.headers.get("X-Flags", None)
                if flags is not None:
                    flags = int(flags)
                trace_info = TraceInfo.from_upstream(
                    trace_id=int(request.headers["X-Trace"]),
                    parent_id=int(request.headers["X-Parent"]),
                    span_id=int(request.headers["X-Span"]),
                    sampled=sampled,
                    flags=flags,
                )

                edge_payload = request.headers.get("X-Edge-Request", None)
                if self.edge_context_factory:
                    edge_context = self.edge_context_factory.from_upstream(
                        edge_payload)
                    edge_context.attach_context(request)
                else:
                    # just attach the raw context so it gets passed on
                    # downstream even if we don't know how to handle it.
                    request.raw_request_context = edge_payload
            except (KeyError, ValueError):
                pass

        request.start_server_span(request.matched_route.name, trace_info)
        request.trace.set_tag("http.url", request.url)
        request.trace.set_tag("http.method", request.method)
        request.trace.set_tag("peer.ipv4", request.remote_addr)

    def _start_server_span(self, request, name, trace_info=None):
        request.trace = self.baseplate.make_server_span(
            request,
            name=name,
            trace_info=trace_info,
        )
        request.trace.start()
        request.registry.notify(ServerSpanInitialized(request))

    def includeme(self, config):
        config.add_subscriber(self._on_new_request, pyramid.events.ContextFound)

        # Position of the tween is important. We need it to cover all code
        # that can written in the app. This means that it should be above
        # (wrap) both the exception view handling tween (EXCVIEW) and the
        # request handling code (MAIN).
        #
        # The final order ends up being:
        # 1. Request ingress (tweens.INGRESS)
        # 2. baseplate tween
        # 3. Exception view handler (tweens.EXCVIEW)
        # 4. App handler code (tweens.MAIN)
        config.add_tween("baseplate.integration.pyramid._make_baseplate_tween",
                         over=pyramid.tweens.EXCVIEW)

        # the pyramid "scripting context" (e.g. pshell) sets up a
        # psuedo-request environment but does not call NewRequest. it does,
        # however, set up request methods. so, we attach this method to the
        # request so we can access it in both pshell_setup and _on_new_request
        # for the different context we can be running in.
        # see: Pylons/pyramid#520
        #
        # pyramid gets all cute with descriptors and will pass the request
        # object as the first ("self") param to bound methods. wrapping
        # the bound method in a simple function prevents that behavior
        def start_server_span(*args, **kwargs):
            return self._start_server_span(*args, **kwargs)
        config.add_request_method(start_server_span, "start_server_span")


def paste_make_app(_, **local_config):
    """Make an application object, PasteDeploy style.

    This is a compatibility shim to adapt the baseplate app entrypoint to
    PasteDeploy-style so tools like Pyramid's pshell work.

    To use it, add a single line to your app's section in its INI file:

        [app:your_app]
        use = egg:baseplate

    """
    return make_app(local_config)


def pshell_setup(env):
    # pylint: disable=line-too-long
    """Start a server span when pshell starts up.

    This simply starts a server span after the shell initializes, which
    gives shell users access to all the :term:`context object` goodness.

    To use it, add configuration to your app's INI file like so:

        [pshell]
        setup = baseplate.integration.pyramid:pshell_setup

    See http://docs.pylonsproject.org/projects/pyramid/en/latest/narr/commandline.html#extending-the-shell

    """
    env["request"].start_server_span("shell")
