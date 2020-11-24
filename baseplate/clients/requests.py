import base64
import ipaddress

from typing import Any
from typing import Type
from typing import Union

from advocate import AddrValidator
from advocate import ValidatingHTTPAdapter
from requests import PreparedRequest
from requests import Request
from requests import Response
from requests import Session
from requests.adapters import HTTPAdapter

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config


def http_adapter_from_config(
    app_config: config.RawConfig, prefix: str, **kwargs: Any
) -> HTTPAdapter:
    """Make an HTTPAdapter from a configuration dictionary.

    The keys useful to :py:func:`http_adapter_from_config` should be prefixed,
    e.g. ``http.pool_connections``, ``http.max_retries``, etc. The ``prefix``
    argument specifies the prefix used. Each key is mapped to a corresponding
    keyword argument on the :py:class:`~requests.adapters.HTTPAdapter`
    constructor.

    Supported keys:

    * ``pool_connections``: The number of connections to cache (default: 10).
    * ``pool_maxsize``: The maximum number of connections to keep in the pool
      (default: 10).
    * ``max_retries``: How many times to retry DNS lookups or connection
      attempts, but never sending data (default: 0).
    * ``pool_block``: Whether the connection pool will block when trying to get
      a connection (default: false).

    Additionally, the rules for Advocate's address filtering can be configured
    with the ``filter`` sub-keys:

    * ``filter.ip_allowlist``: A comma-delimited list of IP addresses (1.2.3.4)
        or CIDR-notation (1.2.3.0/24) ranges that the client can always connect to
        (default: anything not on the local network).
    * ``filter.ip_denylist``: A comma-delimited list of IP addresses or
        CIDR-notation ranges the client may never connect to (default: the local network).
    * ``filter.port_allowlist``: A comma-delimited list of TCP port numbers
        that the client can connect to (default: 80, 8080, 443, 8443, 8000).
    * ``filter.port_denylist``: A comma-delimited list of TCP port numbers that
        the client may never connect to (default: none).
    * ``filter.hostname_denylist``: A comma-delimited list of hostnames that
        the client may never connect to (default: none).
    * ``filter.allow_ipv6``: Should the client be allowed to connect to IPv6
        hosts? (default: false, note: IPv6 is tricky to apply filtering rules
        comprehensively to).

    """

    assert prefix.endswith(".")
    parser = config.SpecParser(
        {
            "pool_connections": config.Optional(config.Integer, default=10),
            "pool_maxsize": config.Optional(config.Integer, default=10),
            "max_retries": config.Optional(config.Integer, default=0),
            "pool_block": config.Optional(config.Boolean, default=False),
            "filter": {
                "ip_allowlist": config.Optional(config.TupleOf(ipaddress.ip_network)),
                "ip_denylist": config.Optional(config.TupleOf(ipaddress.ip_network)),
                "port_allowlist": config.Optional(config.TupleOf(int)),
                "port_denylist": config.Optional(config.TupleOf(int)),
                "hostname_denylist": config.Optional(config.TupleOf(config.String)),
                "allow_ipv6": config.Optional(config.Boolean, default=False),
            },
        }
    )
    options = parser.parse(prefix[:-1], app_config)

    if options.pool_connections is not None:
        kwargs.setdefault("pool_connections", options.pool_connections)
    if options.pool_maxsize is not None:
        kwargs.setdefault("pool_maxsize", options.pool_maxsize)
    if options.max_retries is not None:
        kwargs.setdefault("max_retries", options.max_retries)
    if options.pool_block is not None:
        kwargs.setdefault("pool_block", options.pool_block)

    kwargs.setdefault(
        "validator",
        AddrValidator(
            ip_whitelist=options.filter.ip_allowlist,
            ip_blacklist=options.filter.ip_denylist,
            port_whitelist=options.filter.port_allowlist,
            port_blacklist=options.filter.port_denylist,
            hostname_blacklist=options.filter.hostname_denylist,
            allow_ipv6=options.filter.allow_ipv6,
        ),
    )
    return ValidatingHTTPAdapter(**kwargs)


class BaseplateSession:
    """A proxy for :py:class:`requests.Session`.

    Requests sent with this client will be instrumented automatically.

    """

    def __init__(self, adapter: HTTPAdapter, name: str, span: Span) -> None:
        self.adapter = adapter
        self.name = name
        self.span = span

    def delete(self, url: str, **kwargs: Any) -> Response:
        """Send a DELETE request.

        See :py:func:`requests.request` for valid keyword arguments.

        """
        return self.request("DELETE", url, **kwargs)

    def get(self, url: str, **kwargs: Any) -> Response:
        """Send a GET request.

        See :py:func:`requests.request` for valid keyword arguments.

        """
        return self.request("GET", url, **kwargs)

    def head(self, url: str, **kwargs: Any) -> Response:
        """Send a HEAD request.

        See :py:func:`requests.request` for valid keyword arguments.

        """
        return self.request("HEAD", url, **kwargs)

    def options(self, url: str, **kwargs: Any) -> Response:
        """Send an OPTIONS request.

        See :py:func:`requests.request` for valid keyword arguments.

        """
        return self.request("OPTIONS", url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> Response:
        """Send a PATCH request.

        See :py:func:`requests.request` for valid keyword arguments.

        """
        return self.request("PATCH", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Response:
        """Send a POST request.

        See :py:func:`requests.request` for valid keyword arguments.

        """
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> Response:
        """Send a PUT request.

        See :py:func:`requests.request` for valid keyword arguments.

        """
        return self.request("PUT", url, **kwargs)

    def prepare_request(self, request: Request) -> PreparedRequest:
        """Construct a :py:class:`~requests.PreparedRequest` for later use.

        The prepared request can be stored or manipulated and then used with
        :py:meth:`send`.

        """
        return request.prepare()

    def request(self, method: str, url: Union[str, bytes], **kwargs: Any) -> Response:
        """Send a request.

        :param method: The HTTP method of the request, e.g. ``GET``, ``PUT``, etc.
        :param url: The URL to send the request to.

        See :py:func:`requests.request` for valid keyword arguments.

        """
        send_kwargs = {
            "timeout": kwargs.pop("timeout", None),
            "allow_redirects": kwargs.pop("allow_redirects", None),
            "verify": kwargs.pop("verify", True),
        }
        request = Request(method=method.upper(), url=url, **kwargs)
        prepared = self.prepare_request(request)
        return self.send(prepared, **send_kwargs)

    def _add_span_context(self, span: Span, request: PreparedRequest) -> None:
        pass

    def send(self, request: PreparedRequest, **kwargs: Any) -> Response:
        """Send a :py:class:`~requests.PreparedRequest`."""

        with self.span.make_child(f"{self.name}.request") as span:
            span.set_tag("http.method", request.method)
            span.set_tag("http.url", request.url)

            self._add_span_context(span, request)

            # we cannot re-use the same session every time because sessions re-use the same
            # CookieJar and so we'd muddle cookies cross-request. if the application wants
            # to keep track of cookies, it should do so itself.
            #
            # note: we're still getting connection pooling because we're re-using the adapter.
            session = Session()
            session.mount("http://", self.adapter)
            session.mount("https://", self.adapter)
            response = session.send(request, **kwargs)

            span.set_tag("http.status_code", response.status_code)
        return response


class InternalBaseplateSession(BaseplateSession):
    def _add_span_context(self, span: Span, request: PreparedRequest) -> None:
        request.headers["X-Trace"] = str(span.trace_id)
        request.headers["X-Parent"] = str(span.parent_id)
        request.headers["X-Span"] = str(span.id)
        if span.sampled:
            request.headers["X-Sampled"] = "1"
        if span.flags is not None:
            request.headers["X-Flags"] = str(span.flags)

        try:
            edge_context = span.context.raw_request_context
        except AttributeError:
            pass
        else:
            request.headers["X-Edge-Request"] = base64.b64encode(edge_context).decode()


class RequestsContextFactory(ContextFactory):
    """Requests client context factory.

    This factory will attach a
    :py:class:`~baseplate.clients.requests.BaseplateSession` to an attribute
    on the :py:class:`~baseplate.RequestContext`. When HTTP requests are sent
    via this session, they will use connections from the provided
    :py:class:`~requests.adapters.HTTPAdapter` connection pools and
    automatically record diagnostic information.

    Note that though the connection pool is shared across calls, a new
    :py:class:`~requests.Session` is created for each request so that cookies
    and other state are not accidentally shared between requests. If you do
    want to persist state, you will need to do it in your application.

    :param adapter: A transport adapter for making HTTP requests. See
        :py:func:`http_adapter_from_config`.
    :param session_cls: The type for the actual session object to put on the
        request context.

    """

    def __init__(self, adapter: HTTPAdapter, session_cls: Type[BaseplateSession]) -> None:
        self.adapter = adapter
        self.session_cls = session_cls

    def make_object_for_context(self, name: str, span: Span) -> BaseplateSession:
        return self.session_cls(self.adapter, name, span)


class InternalRequestsClient(config.Parser):
    """Configure a Requests client for use with internal Baseplate HTTP services.

    Requests made with this client **will** include trace context and
    :doc:`edge context </api/baseplate/lib/edge_context>`. This client should
    only be used to speak to trusted internal services.  URLs that resolve to
    public addresses will be rejected.  It is not possible to override the
    Advocate address validator used by this client.

    .. warning:: Requesting user-specified URLs with this client could lead to
        `Server-Side Request Forgery`_. Ensure that you only request trusted URLs
        e.g. hard-coded or from a local configuration file.

    .. _`Server-Side Request Forgery`: https://en.wikipedia.org/wiki/Server-side_request_forgery

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`http_adapter_from_config` for available configuration settings.

    """

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

        if "validator" in kwargs:
            raise Exception("validator is hard-coded for internal clients")

    def parse(self, key_path: str, raw_config: config.RawConfig) -> RequestsContextFactory:
        # use advocate to ensure this client only ever gets used
        # with internal services over the internal network.
        #
        # the allowlist takes precedence. allow loopback and 10./8 private
        # addresses, deny the rest.
        validator = AddrValidator(
            ip_whitelist={ipaddress.ip_network("127.0.0.0/8"), ipaddress.ip_network("10.0.0.0/8")},
            ip_blacklist={ipaddress.ip_network("0.0.0.0/0")},
            port_blacklist=[0],  # disable the default allowlist by giving an explicit denylist
            allow_ipv6=False,
        )

        adapter = http_adapter_from_config(
            raw_config, prefix=f"{key_path}.", validator=validator, **self.kwargs
        )
        return RequestsContextFactory(adapter, session_cls=InternalBaseplateSession)


class ExternalRequestsClient(config.Parser):
    """Configure a Requests client for use with external HTTP services.

    Requests made with this client **will not** include trace context and
    :doc:`edge context </api/baseplate/lib/edge_context>`. This client is
    suitable for use with third party or untrusted services.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`http_adapter_from_config` for available configuration settings.

    """

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def parse(self, key_path: str, raw_config: config.RawConfig) -> RequestsContextFactory:
        adapter = http_adapter_from_config(raw_config, f"{key_path}.", **self.kwargs)
        return RequestsContextFactory(adapter, session_cls=BaseplateSession)
