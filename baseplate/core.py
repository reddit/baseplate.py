from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import logging
import random

import jwt
from thrift import TSerialization
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory

from .integration.wrapped_context import WrappedRequestContext
from ._utils import warn_deprecated, cached_property


logger = logging.getLogger(__name__)


class BaseplateObserver(object):
    """Interface for an observer that watches Baseplate."""

    def on_server_span_created(self, context, server_span):
        """Called when a server span is created.

        :py:class:`Baseplate` calls this when a new request begins.

        :param context: The :term:`context object` for this request.
        :param baseplate.core.ServerSpan server_span: The span representing
            this request.

        """
        raise NotImplementedError


class SpanObserver(object):
    """Interface for an observer that watches a span."""

    def on_start(self):
        """Called when the observed span is started."""
        pass

    def on_set_tag(self, key, value):
        """Called when a tag is set on the observed span."""
        pass

    def on_log(self, name, payload):
        """Called when a log entry is added to the span."""
        pass

    def on_finish(self, exc_info):
        """Called when the observed span is finished.

        :param exc_info: If the span ended because of an exception, the
            exception info. Otherwise, :py:data:`None`.

        """
        pass

    def on_child_span_created(self, span):
        """Called when a child span is created.

        :py:class:`SpanObserver` objects call this when a new child span is
        created.

        :param baseplate.core.Span span: The new child span.

        """
        pass


class ServerSpanObserver(SpanObserver):
    """Interface for an observer that watches the server span."""
    pass


_TraceInfo = collections.namedtuple("_TraceInfo",
                                    "trace_id parent_id span_id sampled flags")


class NoAuthenticationError(Exception):
    """Raised when trying to use an invalid or missing authentication token."""
    pass


class TraceInfo(_TraceInfo):
    """Trace context for a span.

    If this request was made at the behest of an upstream service, the upstream
    service should have passed along trace information. This class is used for
    collecting the trace context and passing it along to the server span.

    """

    @classmethod
    def new(cls):
        """Generate IDs for a new initial server span.

        This span has no parent and has a random ID. It cannot be correlated
        with any upstream requests.

        """
        trace_id = random.getrandbits(64)
        return cls(trace_id=trace_id, parent_id=None,
                   span_id=trace_id, sampled=None, flags=None)

    @classmethod
    def from_upstream(cls, trace_id, parent_id, span_id, sampled, flags):
        """Build a TraceInfo from individual headers.

        :param int trace_id: The ID of the trace.
        :param int parent_id: The ID of the parent span.
        :param int span_id: The ID of this span within the tree.
        :param bool sampled: Boolean flag to determine request sampling.
        :param int flags: Bit flags for communicating feature flags downstream

        :raises: :py:exc:`ValueError` if any of the values are inappropriate.

        """
        if trace_id is None or not 0 <= trace_id < 2**64:
            raise ValueError("invalid trace_id")

        if span_id is None or not 0 <= span_id < 2**64:
            raise ValueError("invalid span_id")

        if parent_id is None or not 0 <= parent_id < 2**64:
            raise ValueError("invalid parent_id")

        if sampled is not None and not isinstance(sampled, bool):
            raise ValueError("invalid sampled value")

        if flags is not None:
            if not 0 <= flags < 2**64:
                raise ValueError("invalid flags value")

        return cls(trace_id, parent_id, span_id, sampled, flags)

    @classmethod
    def extract_upstream_header_values(cls, upstream_header_names, headers):
        """Extract values from upstream headers.

        This method thinks about upstream headers by a general name as oppposed to the header
        name, i.e. "trace_id" instead of "X-Trace". These general names are "trace_id",
        "span_id", "parent_span_id", "sampled" and "flags".

        A dict mapping these general names to corresponding header names is expected.

        For example:

            {
                "trace_id": ("X-Trace", "X-B3-TraceId"),
                "span_id": ("X-Span", "X-B3-SpanId"),
                "parent_span_id": ("X-Parent", "X-B3-ParentSpanId"),
                "sampled": ("X-Sampled", "X-B3-Sampled"),
                "flags": ("X-Flags", "X-B3-Flags"),
            }

        This structure is used to extract relevant values from the request headers resulting
        in a dict mapping general names to values.

        For example:

            {
                "trace_id": "2391921232992245445",
                "span_id": "7638783876913511395",
                "parent_span_id": "3383915029748331832",
                "sampled": "1",
            }

        :param dict upstream_headers_name: Map of general upstream value labels to header names
        :param dict headers: Headers sent with a request
        :return: Values found in upstream trace headers
        :rtype: dict

        :raises: :py:exc:`ValueError` if conflicting values are found for the same header category

        """
        extracted_values = {}
        for name, header_names in upstream_header_names.items():
            values = []
            for header_name in header_names:
                if header_name in headers:
                    values.append(headers[header_name])

            if not values:
                continue
            elif not all(value == values[0] for value in values):
                raise ValueError("Conflicting values found for %s header(s)".format(header_names))
            else:
                # All the values are the same
                extracted_values[name] = values[0]
        return extracted_values


class AuthenticationTokenValidator(object):
    """Factory that knows how to validate raw authentication tokens."""

    def __init__(self, secrets):
        self.secrets = secrets

    def validate(self, token):
        """Validate a raw authentication token and return an object.

        :param token: token value originating from the Authentication service
            either directly or from an upstream service
        :rtype: :py:class:`AuthenticationToken`

        """
        if not token:
            return InvalidAuthenticationToken()

        secret = self.secrets.get_versioned("secret/authentication/public-key")
        for public_key in secret.all_versions:
            try:
                decoded = jwt.decode(token, public_key, algorithms="RS256")
                return ValidatedAuthenticationToken(decoded)
            except jwt.ExpiredSignatureError:
                pass
            except jwt.DecodeError:
                pass

        return InvalidAuthenticationToken()


class AuthenticationToken(object):
    """Information about the authenticated user.

    :py:class:`EdgeRequestContext` provides high-level helpers for extracting
    data from authentication tokens. Use those instead of direct access through
    this class.

    """

    @property
    def subject(self):
        """The raw `subject` that is authenticated."""
        raise NotImplementedError

    @property
    def user_roles(self):
        raise NotImplementedError

    @property
    def oauth_client_id(self):
        raise NotImplementedError

    @property
    def oauth_client_type(self):
        raise NotImplementedError


class ValidatedAuthenticationToken(AuthenticationToken):
    def __init__(self, payload):
        self.payload = payload

    @property
    def subject(self):
        return self.payload.get("sub")

    @cached_property
    def user_roles(self):
        return set(self.payload.get("roles", []))

    @property
    def oauth_client_id(self):
        return self.payload.get("client_id")

    @property
    def oauth_client_type(self):
        return self.payload.get("client_type")


class InvalidAuthenticationToken(AuthenticationToken):
    @property
    def subject(self):
        raise NoAuthenticationError

    @property
    def user_roles(self):
        raise NoAuthenticationError

    @property
    def oauth_client_id(self):
        raise NoAuthenticationError

    @property
    def oauth_client_type(self):
        raise NoAuthenticationError


_User = collections.namedtuple(
    "_User", ["authentication_token", "loid", "cookie_created_ms"])
_OAuthClient = collections.namedtuple(
    "_OAuthClient", ["authentication_token"])
Session = collections.namedtuple("Session", ["id"])
_Service = collections.namedtuple("_Service", ["authentication_token"])


class User(_User):
    """Wrapper for the user values in AuthenticationToken and the LoId cookie.
    """

    @property
    def id(self):
        """Authenticated account_id for the current User.

        :type: account_id string or None if context authentication is invalid
        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token, it was invalid, or the subject is not an
            account.

        """
        subject = self.authentication_token.subject
        if not (subject and subject.startswith("t2_")):
            raise NoAuthenticationError
        return subject

    @property
    def is_logged_in(self):
        """Does the User have a valid, authenticated id?"""
        try:
            return self.id is not None
        except NoAuthenticationError:
            return False

    @property
    def roles(self):
        """Authenticated roles for the current User.

        :type: set(string)
        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token or it was invalid

        """
        return self.authentication_token.user_roles

    def has_role(self, role):
        """Does the authenticated user have the specified role?

        :param str client_types: Case-insensitive sequence role name to check.

        :type: bool
        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token defined for the current context

        """
        return role.lower() in self.roles

    def event_fields(self):
        """Return fields to be added to events."""
        if self.is_logged_in:
            user_id = self.id
        else:
            user_id = self.loid

        return {
            "user_id": user_id,
            "logged_in": self.is_logged_in,
            "cookie_created_timestamp": self.cookie_created_ms,
        }


class OAuthClient(_OAuthClient):
    """Wrapper for the OAuth2 client values in AuthenticationToken."""

    @property
    def id(self):
        """Authenticated id for the current client

        :type: string or None if context authentication is invalid
        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token defined for the current context

        """
        return self.authentication_token.oauth_client_id

    def is_type(self, *client_types):
        """Is the authenticated client type one of the given types?

        When checking the type of the current OauthClient, you should check
        that the type "is" one of the allowed types rather than checking that
        it "is not" a disallowed type.

        For example::

            if oauth_client.is_type("third_party"):
                ...

        not::

            if not oauth_client.is_type("first_party"):
                ...


        :param str client_types: Case-insensitive sequence of client type
            names that you want to check.

        :type: bool
        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token defined for the current context

        """
        lower_types = (client_type.lower() for client_type in client_types)
        return self.authentication_token.oauth_client_type in lower_types

    def event_fields(self):
        """Return fields to be added to events."""
        try:
            oauth_client_id = self.id
        except NoAuthenticationError:
            oauth_client_id = None

        return {
            "oauth_client_id": oauth_client_id,
        }


class Service(_Service):
    """Wrapper for the Service values in AuthenticationToken."""

    @property
    def name(self):
        """Authenticated Service name.

        :type: name string or None if context authentication is invalid
        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token, it was invalid, or the subject is not a
            servce.

        """
        subject = self.authentication_token.subject
        if not (subject and subject.startswith("service/")):
            raise NoAuthenticationError

        name = subject[len("service/"):]
        return name


class EdgeRequestContextFactory(object):
    """Factory for creating :py:class:`EdgeRequestContext` objects.

    Every application should set one of these up. Edge services that talk
    directly with clients should use :py:meth:`new` directly. For internal
    services, pass the object off to Baseplate's framework integration
    (Thrift/Pyramid) for automatic use.

    :param baseplate.secrets.SecretsStore secrets: A configured secrets
        store.

    """
    def __init__(self, secrets):
        self.authn_token_validator = AuthenticationTokenValidator(secrets)

    def new(self,
            authentication_token=None,
            loid_id=None, loid_created_ms=None,
            session_id=None):
        """Return a new EdgeRequestContext object made from scratch.

        Services at the edge that communicate directly with clients should use
        this to pass on the information they get to downstream services. They
        can then use this information to check authentication, run experiments,
        etc.

        To use this, create and attach the context early in your request flow:

        .. code-block:: python

            auth_cookie = request.cookies["authentication"]
            token = request.authentication_service.authenticate_cookie(cookie)
            loid = parse_loid(request.cookies["loid"])
            session = parse_session(request.cookies["session"])

            edge_context = self.edgecontext_factory.new(
                authentication_token=token,
                loid_id=loid.id,
                loid_created_ms=loid.created,
                session_id=session.id,
            )
            edge_context.attach_context(request)

        :param authentication_token: (Optional) A raw authentication token
            as returned by the authentication service.
        :param str loid_id: (Optional) ID for the current LoID in fullname
            format.
        :param int loid_created_ms: (Optional) Epoch milliseconds when the
            current LoID cookie was created.
        :param str session_id: (Optional) ID for the current session cookie.

        """
        # Importing the Thrift models inline so that building them is not a
        # hard, import-time dependency for tasks like building the docs.
        from .thrift.ttypes import Loid as TLoid
        from .thrift.ttypes import Request as TRequest
        from .thrift.ttypes import Session as TSession

        if loid_id is not None and not loid_id.startswith("t2_"):
            raise ValueError(
                "loid_id <%s> is not in a valid format, it should be in the "
                "fullname format with the '0' padding removed: 't2_loid_id'",
                loid_id
            )

        t_request = TRequest(
            loid=TLoid(id=loid_id, created_ms=loid_created_ms),
            session=TSession(id=session_id),
            authentication_token=authentication_token,
        )
        header = TSerialization.serialize(
            t_request, EdgeRequestContext._HEADER_PROTOCOL_FACTORY)

        context = EdgeRequestContext(self.authn_token_validator, header)
        # Set the _t_request property so we can skip the deserialization step
        # since we already have the thrift object.
        context._t_request = t_request
        return context

    def from_upstream(self, edge_header):
        """Create and return an EdgeRequestContext from an upstream header.

        This is generally used internally to Baseplate by framework
        integrations that automatically pick up context from inbound requests.

        :param edge_header: Raw payload of Edge-Request header from upstream
            service.

        """
        return EdgeRequestContext(self.authn_token_validator, edge_header)


class EdgeRequestContext(object):
    """Contextual information about the initial request to an edge service

    Construct this using an
    :py:class:`~baseplate.core.EdgeRequestContextFactory`.

    """

    _HEADER_PROTOCOL_FACTORY = TBinaryProtocolAcceleratedFactory()

    def __init__(self, authn_token_validator, header):
        self._authn_token_validator = authn_token_validator
        self._header = header

    def attach_context(self, context):
        """Attach this to the provided :term:`context object`.

        :param context: request context to attach this to

        """
        context.request_context = self
        context.raw_request_context = self._header

    def event_fields(self):
        """Return fields to be added to events."""
        fields = {
            "session_id": self.session.id,
        }
        fields.update(self.user.event_fields())
        fields.update(self.oauth_client.event_fields())
        return fields

    @cached_property
    def authentication_token(self):
        return self._authn_token_validator.validate(self._t_request.authentication_token)

    @cached_property
    def user(self):
        """:py:class:`~baseplate.core.User` object for the current context"""
        return User(
            authentication_token=self.authentication_token,
            loid=self._t_request.loid.id,
            cookie_created_ms=self._t_request.loid.created_ms,
        )

    @cached_property
    def oauth_client(self):
        """:py:class:`~baseplate.core.OAuthClient` object for the current context
        """
        return OAuthClient(self.authentication_token)

    @cached_property
    def session(self):
        """:py:class:`~baseplate.core.Session` object for the current context"""
        return Session(id=self._t_request.session.id)

    @cached_property
    def service(self):
        """:py:class:`~baseplate.core.Service` object for the current context"""
        return Service(self.authentication_token)

    @cached_property
    def _t_request(self):  # pylint: disable=method-hidden
        # Importing the Thrift models inline so that building them is not a
        # hard, import-time dependency for tasks like building the docs.
        from .thrift.ttypes import Loid as TLoid
        from .thrift.ttypes import Request as TRequest
        from .thrift.ttypes import Session as TSession
        _t_request = TRequest()
        _t_request.loid = TLoid()
        _t_request.session = TSession()
        if self._header:
            try:
                TSerialization.deserialize(
                    _t_request,
                    self._header,
                    self._HEADER_PROTOCOL_FACTORY,
                )
            except Exception:
                logger.debug(
                    "Invalid Edge-Request header. %s",
                    self._header,
                )
        return _t_request


class Baseplate(object):
    """The core of the Baseplate diagnostics framework.

    This class coordinates monitoring and tracing of service calls made to
    and from this service. See :py:mod:`baseplate.integration` for how to
    integrate it with the application framework you are using.

    """
    def __init__(self):
        self.observers = []
        self._metrics_client = None

    def register(self, observer):
        """Register an observer.

        :param baseplate.core.BaseplateObserver observer: An observer.

        """
        self.observers.append(observer)

    def configure_logging(self):
        """Add request context to the logging system."""
        from .diagnostics.logging import LoggingBaseplateObserver
        self.register(LoggingBaseplateObserver())

    def configure_metrics(self, metrics_client):
        """Send timing metrics to the given client.

        This also adds a :py:class:`baseplate.metrics.Batch` object to the
        ``metrics`` attribute on the :term:`context object` where you can add
        your own application-specific metrics. The batch is automatically
        flushed at the end of the request.

        :param baseplate.metrics.Client metrics_client: Metrics client to send
            request metrics to.

        """
        from .diagnostics.metrics import MetricsBaseplateObserver
        self._metrics_client = metrics_client
        self.register(MetricsBaseplateObserver(metrics_client))

    def configure_tracing(self, tracing_client, *args, **kwargs):
        """Collect and send span information for request tracing.

        When configured, this will send tracing information automatically
        collected by Baseplate to the configured distributed tracing service.

        :param baseplate.diagnostics.tracing.TracingClient tracing_client: Tracing
            client to send request traces to.

        """
        from .diagnostics.tracing import (
            make_client,
            TraceBaseplateObserver,
            TracingClient,
        )

        # the first parameter was service_name before, so if it's not a client
        # object we'll act like this is the old-style invocation and use the
        # first parameter as service_name instead, passing on the old arguments
        if not isinstance(tracing_client, TracingClient):
            warn_deprecated("Passing tracing configuration directly to "
                            "configure_tracing is deprecated in favor of "
                            "using baseplate.tracing_client_from_config and "
                            "passing the constructed client on.")
            tracing_client = make_client(tracing_client, *args, **kwargs)

        self.register(TraceBaseplateObserver(tracing_client))

    def configure_error_reporting(self, client):
        """Send reports for unexpected exceptions to the given client.

        This also adds a :py:class:`raven.Client` object to the ``sentry``
        attribute on the :term:`context object` where you can send your own
        application-specific events.

        :param raven.Client client: A configured raven client.

        """
        from .diagnostics.sentry import (
            SentryBaseplateObserver,
            SentryUnhandledErrorReporter,
        )

        from gevent import get_hub
        hub = get_hub()
        hub.print_exception = SentryUnhandledErrorReporter(hub, client)

        self.register(SentryBaseplateObserver(client))

    def add_to_context(self, name, context_factory):
        """Add an attribute to each request's context object.

        On each request, the factory will be asked to create an appropriate
        object to attach to the :term:`context object`.

        :param str name: The attribute on the context object to attach the
            created object to. This may also be used for metric/tracing
            purposes so it should be descriptive.
        :param baseplate.context.ContextFactory context_factory: A factory.

        """
        from .context import ContextObserver
        self.register(ContextObserver(name, context_factory))

    def make_server_span(self, context, name, trace_info=None):
        """Return a server span representing the request we are handling.

        In a server, a server span represents the time spent on a single
        incoming request. Any calls made to downstream services will be new
        child spans of the server span, and the server span will in turn be the
        child span of whatever upstream request it is part of, if any.

        :param context: The :term:`context object` for this request.
        :param str name: A name to identify the type of this request, e.g.
            a route or RPC method name.
        :param baseplate.core.TraceInfo trace_info: The trace context of this
            request as passed in from upstream. If :py:data:`None`, a new trace
            context will be generated.

        """

        if trace_info is None:
            trace_info = TraceInfo.new()

        server_span = ServerSpan(trace_info.trace_id, trace_info.parent_id,
                                 trace_info.span_id, trace_info.sampled,
                                 trace_info.flags, name, WrappedRequestContext(context))

        for observer in self.observers:
            observer.on_server_span_created(context, server_span)
        return server_span


class Span(object):
    """A span represents a single RPC within a system."""

    def __init__(self, trace_id, parent_id, span_id, sampled, flags, name, context):
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.id = span_id
        self.sampled = sampled
        self.flags = flags
        self.name = name
        self.context = context
        self.observers = []

    def register(self, observer):
        """Register an observer to receive events from this span."""
        self.observers.append(observer)

    def start(self):
        """Record the start of the span.

        This notifies any observers that the span has started, which indicates
        that timers etc. should start ticking.

        Spans also support the `context manager protocol`_, for use with
        Python's ``with`` statement. When the context is entered, the span
        calls :py:meth:`start` and when the context is exited it automatically
        calls :py:meth:`finish`.

        .. _context manager protocol:
            https://docs.python.org/3/reference/datamodel.html#context-managers

        """
        for observer in self.observers:
            observer.on_start()

    def set_tag(self, key, value):
        """Set a tag on the span.

        Tags are arbitrary key/value pairs that add context and meaning to the
        span, such as a hostname or query string. Observers may interpret or
        ignore tags as they desire.

        :param str key: The name of the tag.
        :param value: The value of the tag, must be a string/boolean/number.

        """
        for observer in self.observers:
            observer.on_set_tag(key, value)

    def log(self, name, payload=None):
        """Add a log entry to the span.

        Log entries are timestamped events recording notable moments in the
        lifetime of a span.

        :param str name: The name of the log entry. This should be a stable
            identifier that can apply to multiple span instances.
        :param payload: Optional log entry payload. This can be arbitrary data.

        """
        for observer in self.observers:
            observer.on_log(name, payload)

    def finish(self, exc_info=None):
        """Record the end of the span.

        :param exc_info: If the span ended because of an exception, this is
            the exception information. The default is :py:data:`None` which
            indicates normal exit.

        """
        for observer in self.observers:
            observer.on_finish(exc_info)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            self.finish(exc_info=(exc_type, value, traceback))
        else:
            self.finish()

    def make_child(self, name, local=False, component_name=None):
        """Return a child Span whose parent is this Span."""
        raise NotImplementedError


class LocalSpan(Span):
    def make_child(self, name, local=False, component_name=None):
        """Return a child Span whose parent is this Span.

        The child span can either be a local span representing an in-request
        operation or a span representing an outbound service call.

        In a server, a local span represents the time spent within a
        local component performing an operation or set of operations.
        The local component is some grouping of business logic,
        which is then split up into operations which could each be wrapped
        in local spans.

        :param str name: Name to identify the operation this span
            is recording.
        :param bool local: Make this span a LocalSpan if True, otherwise
            make this span a base Span.
        :param str component_name: Name to identify local component
            this span is recording in if it is a local span.
        """
        span_id = random.getrandbits(64)

        if local:
            context_copy = self.context.clone()
            span = LocalSpan(self.trace_id, self.id, span_id, self.sampled,
                             self.flags, name, context_copy)
            if component_name is None:
                raise ValueError("Cannot create local span without component name.")
            span.component_name = component_name
            context_copy.shadow_context_attr('trace', span)
        else:
            span = Span(
                self.trace_id, self.id, span_id, self.sampled,
                self.flags, name, self.context)
        for observer in self.observers:
            observer.on_child_span_created(span)
        return span


class ServerSpan(LocalSpan):
    """A server span represents a request this server is handling.

    The server span is available on the :term:`context object` during requests
    as the ``trace`` attribute.

    """
    pass
