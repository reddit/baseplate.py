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
from ..server import make_app


# pylint: disable=abstract-class-not-used
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

        request.start_root_span(request.matched_route.name, trace_info)

    def _start_root_span(self, request, name, trace_info=None):
        request.trace = self.baseplate.make_root_span(
            request,
            name=name,
            trace_info=trace_info,
        )
        request.trace.start()

    # pylint: disable=no-self-use
    def _on_new_response(self, event):
        if not event.request.matched_route:
            return

        event.request.trace.stop()

    def includeme(self, config):
        config.add_subscriber(self._on_new_request, ContextFound)
        config.add_subscriber(self._on_new_response, NewResponse)

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
        def start_root_span(*args, **kwargs):
            return self._start_root_span(*args, **kwargs)
        config.add_request_method(start_root_span, "start_root_span")


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
    """Start a root span when pshell starts up.

    This simply starts a root span after the shell initializes, which
    gives shell users access to all the :term:`context object` goodness.

    To use it, add configuration to your app's INI file like so:

        [pshell]
        setup = baseplate.integration.pyramid:pshell_setup

    See http://docs.pylonsproject.org/projects/pyramid/en/latest/narr/commandline.html#extending-the-shell

    """
    env["request"].start_root_span("shell")
