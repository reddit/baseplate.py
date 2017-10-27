from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import logging
import random

import jwt
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.util import Serializer

from .integration.wrapped_context import WrappedRequestContext
from ._compat import string_types
from ._utils import warn_deprecated


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


class AuthenticationContextFactory(object):
    """Factory for consistent AuthenticationContext creation.

    This factory should be passed into the constructor of any upstream
    :doc:`integrations <integration/index>` so that it is aware of the
    application's secret store and can create the authentication context for
    the incoming requests.

    :param baseplate.secrets.SecretsStore secrets_store: the application's
        defined secrets store, where application secret lookups should be made
    """
    def __init__(self, secrets_store=None):
        self.secrets = secrets_store

    def make_context(self, token):
        """Builds :py:class:`AuthenticationContext` using stored values

        :param token: token value originating from the Authentication service
            either directly or from an upstream service
        :rtype: :py:class:`AuthenticationContext`
        """
        return AuthenticationContext(token=token, secrets=self.secrets)


class AuthenticationContext(object):
    """Wrapper for the contextual authentication information

    In general, this object should not be used directly but will rather be
    wrapped by an :py:class:`EdgeRequestContext` object.

    :param str token: the value for the currently propagated authentication
        context
    :param baseplate.secrets.SecretsStore secrets_store: the application's
        defined secrets store, where application secret lookups should be made
    """
    def __init__(self, token=None, secrets=None):
        self._secret_store = secrets
        # Ensure that self._token is str (Python 2) or bytes (Python 3).  While
        # jwt.decode works correctly if self._token is a str, unicode, or bytes
        # the Thrift THeaderProtocol that is used to pass this value through
        # RPC calls expects this value to be a str (or bytes) and breaks if the
        # length of the value being set as a header is over 128 characters and
        # the value is of type unicode (Python 2 only).  Rather than coverting
        # before passing as a header, we encode in the constructor so we know
        # that self._token is always the same type.
        if isinstance(token, string_types):
            token = token.encode()
        self._token = token
        self._payload = None
        self._valid = None
        self.defined = self._token is not None

    def _secret(self):
        if not self._secret_store:
            raise UndefinedSecretsException

        return self._secret_store.get_simple("jwt/authentication/secret")

    def attach_context(self, context):
        """.. deprecated:: 0.23.0

        Attaching the AuthenticationContext object to the context directly is
        considered deprecated.  You should be passing the AuthenticationContext
        object to the EdgeRequestContext constructor and attaching that object
        to the context.  Baseplate will only forward the AuthenticationContext
        if it is included in the EdgeRequestContext object that is attached to
        the context object.

        :param context: request context to attach this authentication to

        """
        context.authentication = self

    @property
    def valid(self):
        """Validity of the current authentication context token

        :type: bool
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and validated)

        """
        if self._valid is not None:
            return self._valid
        elif not self.defined:
            return False

        try:
            self._payload = jwt.decode(self._token, self._secret(),
                                       algorithms='RS256')
            self._valid = True
        except jwt.ExpiredSignatureError:  # when the token has expired
            self._valid = False
        except jwt.DecodeError:  # When the token is malformed
            self._valid = False

        return self._valid

    @property
    def payload(self):
        """Decrypted payload of the authentication token.

        :type: dict
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)

        """
        if not self.valid:
            return {}

        return self._payload

    @property
    def account_id(self):
        """Authenticated account_id for the current authenticated context

        :type: account_id string or None if context authentication is invalid
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)
        :raises: :py:class:`WithheldAuthenticationError` if there was no
            authentication token defined for the current context

        """
        if not self.defined:
            raise WithheldAuthenticationError

        return self.payload.get("sub", None)

    @property
    def user_roles(self):
        """User roles for the current authenticated context

        :type: set(string)
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)
        :raises: :py:class:`WithheldAuthenticationError` if there was no
            authentication token defined for the current context

        """
        if not self.defined:
            raise WithheldAuthenticationError

        roles = self.payload.get("user_roles", None)
        return set(roles) if roles else set()

    @property
    def oauth_client_id(self):
        """ID for the OAuth client used with the current authenticated context

        :type: string or None if context authentication is invalid or the user
            did not authenticate with OAuth2
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)
        :raises: :py:class:`WithheldAuthenticationError` if there was no
            authentication token defined for the current context

        """
        if not self.defined:
            raise WithheldAuthenticationError

        return self.payload.get("client_id", None)

    @property
    def oauth_client_type(self):
        """Type of OAuth client used with the current authenticated context

        :type: string or None if context authentication is invalid or the user
            did not authenticate with OAuth2
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)
        :raises: :py:class:`WithheldAuthenticationError` if there was no
            authentication token defined for the current context

        """
        if not self.defined:
            raise WithheldAuthenticationError

        return self.payload.get("client_type", None)


class UndefinedSecretsException(Exception):
    """Exception raised when no SecretsStore is defined during token parsing

    Occurs in the :py:class:`AuthenticationContext` object when it attempts to
    parse an authentication payload without having a :py:class:`SecretsStore`
    defined to provide the necessary secrets values to correctly decrypt and
    parse the token.
    """
    def __init__(self):
        super(UndefinedSecretsException, self).__init__(
            "No SecretsStore defined for Authentication token parsing.")


class WithheldAuthenticationError(Exception):
    """Error raised when attempting to read from an unset authentication token

    Occurs either because of a badly instantiated context or missing header
    coming from upstream requests.
    """
    def __init__(self):
        super(WithheldAuthenticationError, self).__init__(
            "No Authentication token provided for this context.")


_User = collections.namedtuple(
    "_User", ["authentication_context", "loid", "cookie_created_ms"])
_OAuthClient = collections.namedtuple(
    "_OAuthClient", ["authentication_context"])
Session = collections.namedtuple("Session", ["id"])


class User(_User):
    """Wrapper for the user values in AuthenticationContext and the LoId cookie.
    """

    def event_fields(self):
        """Dictionary of values to be added to events in the current context
        """
        if self.is_logged_in:
            user_id = self.id
        else:
            user_id = self.loid
        return {
            "user_id": user_id,
            "user_logged_in": self.is_logged_in,
            "cookie_created": self.cookie_created_ms,
        }

    @property
    def id(self):
        """Authenticated account_id for the current User

        :type: account_id string or None if context authentication is invalid
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)
        :raises: :py:class:`WithheldAuthenticationError` if there was no
            authentication token defined for the current context

        """
        return self.authentication_context.account_id

    @property
    def is_logged_in(self):
        """Does the User have a valid, authenticated id"""
        try:
            return self.id is not None
        except (WithheldAuthenticationError, UndefinedSecretsException):
            return False

    @property
    def roles(self):
        """Authenticated roles for the current User

        :type: set(string)
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)
        :raises: :py:class:`WithheldAuthenticationError` if there was no
            authentication token defined for the current context

        """
        return self.authentication_context.user_roles


class OAuthClient(_OAuthClient):
    """Wrapper for the OAuth2 client values in AuthenticationContext."""

    @property
    def id(self):
        """Authenticated id for the current client

        :type: string or None if context authentication is invalid
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)
        :raises: :py:class:`WithheldAuthenticationError` if there was no
            authentication token defined for the current context

        """
        return self.authentication_context.oauth_client_id

    def is_type(self, *client_types):
        """Is the authenticated client type one of the given types

        When checking the type of the current OauthClient, you should check
        that the type "is" one of the allowed types rather than checking that
        it "is not" a disallowed type.

        For example::

            if oauth_client.is_type("third_party"):
                ...

        not::

            if not oauth_client.is_type("first_party"):
                ...


        :param str *client_types: Case-insensitive sequence of client type
            names that you want to check.

        :type: bool
        :raises: :py:class:`UndefinedSecretsException` if the
            :py:class:`SecretsStore` has not been bound to the context handling
            class (means that the authentication payload could not be decrypted
            and the user_id returned)
        :raises: :py:class:`WithheldAuthenticationError` if there was no
            authentication token defined for the current context

        """
        lower_types = (client_type.lower() for client_type in client_types)
        return self.authentication_context.oauth_client_type in lower_types


class EdgeRequestContext(object):
    """Contextual information about the initial request to an edge service

    :param bytes header: Serialized "Edge-Request" header.
    :param baseplate.core.AuthenticationContext authentication_context:
        Authentication context for the current request if it is authenticated.
    """

    _HEADER_PROTOCOL_FACTORY = TBinaryProtocolAcceleratedFactory()

    def __init__(self, header, authentication_context):
        self._header = header
        self._authentication_context = authentication_context
        self._t_request = None
        self._user = None
        self._oauth_client = None
        self._session = None

    def attach_context(self, context):
        """Attach this to the provided Baseplate context.

        :param context: request context to attach this to
        """
        context.request_context = self

    def header_values(self):
        """Dictionary of the serialized headers with the request context data

        Used to get the values to forward with upstream service calls.
        """
        return {
            'Edge-Request': self._header,
            'Authentication': self._authentication_context._token,
        }

    def event_fields(self):
        """Dictionary of values to be added to events in the current context
        """
        try:
            oauth_client_id = self.oauth_client.id
        except (WithheldAuthenticationError, UndefinedSecretsException):
            oauth_client_id = None

        fields = {
            'session_id': self.session.id,
            'oauth_client_id': oauth_client_id,
        }
        fields.update(self.user.event_fields())
        return fields

    @classmethod
    def create(cls, authentication_context=None, loid_id=None,
               loid_created_ms=None, session_id=None):
        """Factory method to create a new EdgeRequestContext object.

        Builds a new EdgeRequestContext object with the given parameters and
        serializes the "Edge-Request" header.

        :param baseplate.core.AuthenticationContext authentication_context:
            (Optional) AuthenticationContext for the current request if it is
            authenticated.
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

        loid = TLoid(id=loid_id, created_ms=loid_created_ms)
        session = TSession(id=session_id)
        request = TRequest(loid=loid, session=session)
        header = Serializer.serialize(cls._HEADER_PROTOCOL_FACTORY, request)
        if authentication_context is None:
            authentication_context = AuthenticationContext()
        request_context = cls(header, authentication_context)
        # Set the _t_request property so we can skip the deserialization step
        # since we already have the thrift object.
        request_context._t_request = request
        return request_context

    @property
    def user(self):
        """:py:class:`baseplate.core.User` object for the current context"""
        if self._user is None:
            t_context = self._thrift_request_context()
            self._user = User(
                authentication_context=self._authentication_context,
                loid=t_context.loid.id,
                cookie_created_ms=t_context.loid.created_ms,
            )
        return self._user

    @property
    def oauth_client(self):
        """:py:class:`baseplate.core.OAuthClient` object for the current context
        """
        if self._oauth_client is None:
            self._oauth_client = OAuthClient(self._authentication_context)
        return self._oauth_client

    @property
    def session(self):
        """:py:class:`baseplate.core.Session` object for the current context"""
        if self._session is None:
            t_context = self._thrift_request_context()
            self._session = Session(id=t_context.session.id)
        return self._session

    def _thrift_request_context(self):
        # Importing the Thrift models inline so that building them is not a
        # hard, import-time dependency for tasks like building the docs.
        from .thrift.ttypes import Loid as TLoid
        from .thrift.ttypes import Request as TRequest
        from .thrift.ttypes import Session as TSession
        if self._t_request is None:
            self._t_request = TRequest()
            self._t_request.loid = TLoid()
            self._t_request.session = TSession()
            if self._header:
                try:
                    Serializer.deserialize(
                        self._HEADER_PROTOCOL_FACTORY,
                        self._header,
                        self._t_request,
                    )
                except Exception:
                    logger.debug(
                        "Invalid Edge-Request header. %s",
                        self._header,
                    )
        return self._t_request


class Baseplate(object):
    """The core of the Baseplate diagnostics framework.

    This class coordinates monitoring and tracing of service calls made to
    and from this service. See :py:mod:`baseplate.integration` for how to
    integrate it with the application framework you are using.

    """
    def __init__(self):
        self.observers = []

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
                            "using baseplate.make_tracing_client and passing "
                            "the constructed client on.")
            tracing_client = make_client(tracing_client, *args, **kwargs)

        self.register(TraceBaseplateObserver(tracing_client))

    def configure_error_reporting(self, client):
        """Send reports for unexpected exceptions to the given client.

        This also adds a :py:class:`raven.Client` object to the ``sentry``
        attribute on the :term:`context object` where you can send your own
        application-specific events.

        :param raven.Client client: A configured raven client.

        """
        from .diagnostics.sentry import SentryBaseplateObserver
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
            span = Span(self.trace_id, self.id, span_id, self.sampled, self.flags, name, self.context)
        for observer in self.observers:
            observer.on_child_span_created(span)
        return span


class ServerSpan(LocalSpan):
    """A server span represents a request this server is handling.

    The server span is available on the :term:`context object` during requests
    as the ``trace`` attribute.

    """
    pass
