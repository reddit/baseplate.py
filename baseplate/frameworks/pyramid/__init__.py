import base64
import logging
import sys

from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import Mapping
from typing import Optional

import pyramid.events
import pyramid.request
import pyramid.tweens
import webob.request

from pyramid.config import Configurator
from pyramid.registry import Registry
from pyramid.request import Request
from pyramid.response import Response

from baseplate import Baseplate
from baseplate import RequestContext
from baseplate import Span
from baseplate import TraceInfo
from baseplate.lib import warn_deprecated
from baseplate.lib.edge_context import EdgeRequestContextFactory
from baseplate.server import make_app
from baseplate.thrift.ttypes import IsHealthyProbe


logger = logging.getLogger(__name__)


class SpanFinishingAppIterWrapper:
    """Wrapper for Response.app_iter that finishes the span when the iterator is done.

    The WSGI spec expects applications to return an iterable object. In the
    common case, the iterable is a single-item list containing a byte string of
    the full response. However, if the application wants to stream a response
    back to the client (e.g. it's sending a lot of data, or it wants to get
    some bytes on the wire really quickly before some database calls finish)
    the iterable can take a while to finish iterating.

    This wrapper allows us to keep the server span open until the iterable is
    finished even though our view callable returned long ago.

    """

    def __init__(self, span: Span, app_iter: Iterable[bytes]) -> None:
        self.span = span
        self.app_iter = iter(app_iter)

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        try:
            return next(self.app_iter)
        except StopIteration:
            self.span.finish()
            raise
        except:  # noqa: E722
            self.span.finish(exc_info=sys.exc_info())
            raise

    def close(self) -> None:
        if hasattr(self.app_iter, "close"):
            self.app_iter.close()  # type: ignore


def _make_baseplate_tween(
    handler: Callable[[Request], Response], _registry: Registry
) -> Callable[[Request], Response]:
    def baseplate_tween(request: Request) -> Response:
        try:
            response = handler(request)
        except:  # noqa: E722
            if hasattr(request, "trace") and request.trace:
                request.trace.finish(exc_info=sys.exc_info())
            raise
        else:
            if request.trace:
                request.trace.set_tag("http.status_code", response.status_code)
                response.app_iter = SpanFinishingAppIterWrapper(request.trace, response.app_iter)
        finally:
            # avoid a reference cycle
            request.start_server_span = None
        return response

    return baseplate_tween


class BaseplateEvent:
    def __init__(self, request: Request):
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


class HeaderTrustHandler:
    """Abstract class used by :py:class:`BaseplateConfigurator` to validate headers.

    See :py:class:`StaticTrustHandler` for the default implementation.
    """

    def should_trust_trace_headers(self, request: Request) -> bool:
        """Return whether baseplate should parse the trace headers from the inbound request.

        :param request: The request

        :returns: Whether baseplate should parse the trace headers from the inbound request.
        """
        raise NotImplementedError

    def should_trust_edge_context_payload(self, request: Request) -> bool:
        """Return whether baseplate should trust the edge context headers from the inbound request.

        :param request: The request

        :returns: Whether baseplate should trust the inbound edge context headers
        """
        raise NotImplementedError


class StaticTrustHandler(HeaderTrustHandler):
    """Default implementation for handling headers.

    This class is created automatically by BaseplateConfigurator unless you
    supply your own HeaderTrustHandler

    :param trust_headers:
        Whether or not to trust trace and edge context headers from
        inbound requests. This value will be returned by should_trust_trace_headers and
        should_trust_edge_context_payload.

    .. warning::

        Do not set ``trust_headers`` to ``True`` unless you are sure your
        application is only accessible by trusted sources (usually backend-only
        services).
    """

    def __init__(self, trust_headers: bool = False):
        self.trust_headers = trust_headers

    def should_trust_trace_headers(self, request: Request) -> bool:
        return self.trust_headers

    def should_trust_edge_context_payload(self, request: Request) -> bool:
        return self.trust_headers


# pylint: disable=too-many-ancestors
class BaseplateRequest(RequestContext, pyramid.request.Request):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        context_config = kwargs.pop("context_config", None)
        RequestContext.__init__(self, context_config=context_config)
        pyramid.request.Request.__init__(self, *args, **kwargs)


class RequestFactory:
    def __init__(self, baseplate: Baseplate):
        self.baseplate = baseplate

    def __call__(self, environ: Dict[str, str]) -> BaseplateRequest:
        return BaseplateRequest(environ, context_config=self.baseplate._context_config)

    def blank(self, path: str) -> BaseplateRequest:
        environ = webob.request.environ_from_url(path)
        return BaseplateRequest(environ, context_config=self.baseplate._context_config)


class BaseplateConfigurator:
    """Configuration extension to integrate Baseplate into Pyramid.

    :param baseplate: The Baseplate instance for your application.
    :param edge_context_factory: A configured factory for handling edge request
        context.
    :param header_trust_handler: An object which will be used to verify whether
        baseplate should parse the request context headers, for example trace ids.
        See StaticTrustHandler for the default implementation.
    """

    def __init__(
        self,
        baseplate: Baseplate,
        trust_trace_headers: Optional[bool] = None,
        edge_context_factory: Optional[EdgeRequestContextFactory] = None,
        header_trust_handler: Optional[HeaderTrustHandler] = None,
    ):
        self.baseplate = baseplate
        self.trust_trace_headers = bool(trust_trace_headers)
        if trust_trace_headers is not None:
            warn_deprecated(
                "setting trust_trace_headers is deprecated in favor of using"
                " a header trust handler."
            )
        self.edge_context_factory = edge_context_factory

        if header_trust_handler:
            self.header_trust_handler = header_trust_handler
        else:
            self.header_trust_handler = StaticTrustHandler(trust_headers=self.trust_trace_headers)

    def _on_application_created(self, event: pyramid.events.ApplicationCreated) -> None:
        # attach the baseplate object to the application the server gets
        event.app.baseplate = self.baseplate

    def _on_new_request(self, event: pyramid.events.ContextFound) -> None:
        request = event.request

        # this request didn't match a route we know
        if not request.matched_route:
            # TODO: some metric for 404s would be good
            return

        trace_info = None
        if self.header_trust_handler.should_trust_trace_headers(request):
            try:
                trace_info = self._get_trace_info(request.headers)
            except (KeyError, ValueError):
                pass

        if self.header_trust_handler.should_trust_edge_context_payload(request):
            edge_payload: Optional[bytes]
            try:
                edge_payload_str = request.headers["X-Edge-Request"]
                edge_payload = base64.b64decode(edge_payload_str.encode())
            except (KeyError, ValueError):
                edge_payload = None

            if self.edge_context_factory and edge_payload:
                edge_context = self.edge_context_factory.from_upstream(edge_payload)
                edge_context.attach_context(request)
            else:
                # just attach the raw context so it gets passed on
                # downstream even if we don't know how to handle it.
                request.raw_request_context = edge_payload

        request.start_server_span(request.matched_route.name, trace_info)
        request.trace.set_tag("http.url", request.url)
        request.trace.set_tag("http.method", request.method)
        request.trace.set_tag("peer.ipv4", request.remote_addr)

    def _start_server_span(
        self, request: BaseplateRequest, name: str, trace_info: Optional[TraceInfo] = None
    ) -> None:
        span = self.baseplate.make_server_span(request, name=name, trace_info=trace_info)
        span.start()
        request.registry.notify(ServerSpanInitialized(request))

    def _get_trace_info(self, headers: Mapping[str, str]) -> TraceInfo:
        sampled = bool(headers.get("X-Sampled") == "1")
        flags = headers.get("X-Flags", None)
        return TraceInfo.from_upstream(
            int(headers["X-Trace"]),
            int(headers["X-Parent"]),
            int(headers["X-Span"]),
            sampled,
            int(flags) if flags is not None else None,
        )

    def includeme(self, config: Configurator) -> None:
        config.set_request_factory(RequestFactory(self.baseplate))
        config.add_subscriber(self._on_new_request, pyramid.events.ContextFound)
        config.add_subscriber(self._on_application_created, pyramid.events.ApplicationCreated)

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
        config.add_tween(
            "baseplate.frameworks.pyramid._make_baseplate_tween", over=pyramid.tweens.EXCVIEW
        )

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
        def start_server_span(
            request: BaseplateRequest, name: str, trace_info: Optional[TraceInfo] = None
        ) -> None:
            return self._start_server_span(request, name, trace_info)

        config.add_request_method(start_server_span, "start_server_span")


def paste_make_app(_: Dict[str, str], **local_config: str) -> Any:
    """Make an application object, PasteDeploy style.

    This is a compatibility shim to adapt the baseplate app entrypoint to
    PasteDeploy-style so tools like Pyramid's pshell work.

    To use it, add a single line to your app's section in its INI file:

        [app:your_app]
        use = egg:baseplate

    """
    return make_app(local_config)


def pshell_setup(env: Dict[str, Any]) -> None:
    r"""Start a server span when pshell starts up.

    This simply starts a server span after the shell initializes, which gives
    shell users access to all the :py:class:`~baseplate.RequestContext`
    goodness.

    To use it, add configuration to your app's INI file like so:

        [pshell]
        setup = baseplate.frameworks.pyramid:pshell_setup

    See the :ref:`Pyramid documentation <extending_pshell>`.

    """
    env["request"].start_server_span("shell")


def get_is_healthy_probe(request: Request) -> int:
    """Get the thrift enum value of the probe used in http is_healthy request."""
    code = request.params.get("type", str(IsHealthyProbe.READINESS))
    try:
        return int(code)
    except ValueError:
        pass
    # If it's not an int, try to find it in the enum map instead.
    try:
        return IsHealthyProbe._NAMES_TO_VALUES[code.upper()]
    except KeyError:
        logger.warning(
            "Unrecognized health check type %s, fallback to READINESS", code,
        )
        return IsHealthyProbe.READINESS
