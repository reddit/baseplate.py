import logging
import re

from typing import Any
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Set

import jwt

from jwt.algorithms import get_default_algorithms
from thrift import TSerialization
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory

from baseplate import RequestContext
from baseplate.lib import cached_property
from baseplate.lib.secrets import SecretsStore
from baseplate.thrift.ttypes import Device as TDevice
from baseplate.thrift.ttypes import Geolocation as TGeolocation
from baseplate.thrift.ttypes import Loid as TLoid
from baseplate.thrift.ttypes import OriginService as TOriginService
from baseplate.thrift.ttypes import Request as TRequest
from baseplate.thrift.ttypes import Session as TSession


logger = logging.getLogger(__name__)


COUNTRY_CODE_RE = re.compile(r"^[A-Z]{2}$")


class NoAuthenticationError(Exception):
    """Raised when trying to use an invalid or missing authentication token."""


class AuthenticationTokenValidator:
    """Factory that knows how to validate raw authentication tokens."""

    def __init__(self, secrets: SecretsStore):
        self.secrets = secrets

        self._algorithm_name = "RS256"
        self._algorithm = get_default_algorithms()[self._algorithm_name]
        self._cache_mtime = 0.0
        self._public_keys: List[Any] = []

    def validate(self, token: bytes) -> "AuthenticationToken":
        """Validate a raw authentication token and return an object.

        :param token: token value originating from the Authentication service
            either directly or from an upstream service

        """
        if not token:
            return InvalidAuthenticationToken()

        secret, mtime = self.secrets.get_versioned_and_mtime("secret/authentication/public-key")
        if mtime > self._cache_mtime:
            self._public_keys = [self._algorithm.prepare_key(key) for key in secret.all_versions]
            self._cache_mtime = mtime

        for public_key in self._public_keys:
            try:
                decoded = jwt.decode(token, public_key, algorithms=self._algorithm_name)
                return ValidatedAuthenticationToken(decoded)
            except jwt.ExpiredSignatureError:
                return InvalidAuthenticationToken()
            except jwt.DecodeError:
                pass

        return InvalidAuthenticationToken()


class AuthenticationToken:
    """Information about the authenticated user.

    :py:class:`EdgeRequestContext` provides high-level helpers for extracting
    data from authentication tokens. Use those instead of direct access through
    this class.

    """

    @property
    def subject(self) -> Optional[str]:
        """Return the raw `subject` that is authenticated."""
        raise NotImplementedError

    @property
    def user_roles(self) -> Set[str]:
        raise NotImplementedError

    @property
    def oauth_client_id(self) -> Optional[str]:
        raise NotImplementedError

    @property
    def oauth_client_type(self) -> Optional[str]:
        raise NotImplementedError

    @property
    def scopes(self) -> Set[str]:
        raise NotImplementedError

    @property
    def loid(self) -> Optional[str]:
        raise NotImplementedError

    @property
    def loid_created_ms(self) -> Optional[int]:
        raise NotImplementedError


class ValidatedAuthenticationToken(AuthenticationToken):
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload

    @property
    def subject(self) -> Optional[str]:
        return self.payload.get("sub")

    @cached_property
    def user_roles(self) -> Set[str]:  # type: ignore # pylint: disable=W0236
        return set(self.payload.get("roles", []))

    @property
    def oauth_client_id(self) -> Optional[str]:
        return self.payload.get("client_id")

    @property
    def oauth_client_type(self) -> Optional[str]:
        return self.payload.get("client_type")

    @property
    def scopes(self) -> Set[str]:
        return set(self.payload.get("scopes") or [])

    @property
    def loid(self) -> Optional[str]:
        return (self.payload.get("loid") or {}).get("id")

    @property
    def loid_created_ms(self) -> Optional[int]:
        return (self.payload.get("loid") or {}).get("created_ms")


class InvalidAuthenticationToken(AuthenticationToken):
    @property
    def subject(self) -> Optional[str]:
        raise NoAuthenticationError

    @property
    def user_roles(self) -> Set[str]:
        raise NoAuthenticationError

    @property
    def oauth_client_id(self) -> Optional[str]:
        raise NoAuthenticationError

    @property
    def oauth_client_type(self) -> Optional[str]:
        raise NoAuthenticationError

    @property
    def scopes(self) -> Set[str]:
        raise NoAuthenticationError

    @property
    def loid(self) -> Optional[str]:
        raise NoAuthenticationError

    @property
    def loid_created_ms(self) -> Optional[int]:
        raise NoAuthenticationError


class Session(NamedTuple):
    """Wrapper for the session values in the EdgeRequestContext."""

    id: str


class Device(NamedTuple):
    """Wrapper for the device values in the EdgeRequestContext."""

    id: str


class OriginService(NamedTuple):
    """Wrapper for the origin values in the EdgeRequestContext."""

    name: str


class Geolocation(NamedTuple):
    """Wrapper for the geolocation values in the EdgeRequestContext."""

    country_code: str


class User(NamedTuple):
    """Wrapper for the user values in AuthenticationToken and the LoId cookie."""

    authentication_token: AuthenticationToken
    loid: str
    cookie_created_ms: int

    @property
    def id(self) -> Optional[str]:
        """Return the authenticated account_id for the current User.

        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token, it was invalid, or the subject is not an
            account.

        """
        subject = self.authentication_token.subject
        if not (subject and subject.startswith("t2_")):
            raise NoAuthenticationError
        return subject

    @property
    def is_logged_in(self) -> bool:
        """Return if the User has a valid, authenticated id."""
        try:
            return self.id is not None
        except NoAuthenticationError:
            return False

    @property
    def roles(self) -> Set[str]:
        """Return the authenticated roles for the current User.

        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token or it was invalid

        """
        return self.authentication_token.user_roles

    def has_role(self, role: str) -> bool:
        """Return if the authenticated user has the specified role.

        :param client_types: Case-insensitive sequence role name to check.

        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token defined for the current context

        """
        return role.lower() in self.roles

    def event_fields(self) -> Dict[str, Any]:
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


class OAuthClient(NamedTuple):
    """Wrapper for the OAuth2 client values in AuthenticationToken."""

    authentication_token: AuthenticationToken

    @property
    def id(self) -> Optional[str]:
        """Return the authenticated id for the current client.

        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token defined for the current context

        """
        return self.authentication_token.oauth_client_id

    def is_type(self, *client_types: str) -> bool:
        """Return if the authenticated client type is one of the given types.

        When checking the type of the current OauthClient, you should check
        that the type "is" one of the allowed types rather than checking that
        it "is not" a disallowed type.

        For example::

            if oauth_client.is_type("third_party"):
                ...

        not::

            if not oauth_client.is_type("first_party"):
                ...


        :param client_types: Case-insensitive sequence of client type
            names that you want to check.

        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token defined for the current context

        """
        lower_types = (client_type.lower() for client_type in client_types)
        if not self.authentication_token.oauth_client_type:
            return False
        return self.authentication_token.oauth_client_type in lower_types

    def event_fields(self) -> Dict[str, Any]:
        """Return fields to be added to events."""
        try:
            oauth_client_id = self.id
        except NoAuthenticationError:
            oauth_client_id = None

        return {"oauth_client_id": oauth_client_id}


class Service(NamedTuple):
    """Wrapper for the Service values in AuthenticationToken."""

    authentication_token: AuthenticationToken

    @property
    def name(self) -> str:
        """Return the authenticated service name.

        :type: name string or None if context authentication is invalid
        :raises: :py:class:`NoAuthenticationError` if there was no
            authentication token, it was invalid, or the subject is not a
            service.

        """
        subject = self.authentication_token.subject
        if not (subject and subject.startswith("service/")):
            raise NoAuthenticationError

        name = subject[len("service/") :]
        return name


class EdgeRequestContextFactory:
    """Factory for creating :py:class:`EdgeRequestContext` objects.

    Every application should set one of these up. Edge services that talk
    directly with clients should use :py:meth:`new` directly. For internal
    services, pass the object off to Baseplate's framework integration
    (Thrift/Pyramid) for automatic use.

    :param baseplate.lib.secrets.SecretsStore secrets: A configured secrets
        store.

    """

    def __init__(self, secrets: SecretsStore):
        self.authn_token_validator = AuthenticationTokenValidator(secrets)

    def new(
        self,
        authentication_token: Optional[bytes] = None,
        loid_id: Optional[str] = None,
        loid_created_ms: Optional[int] = None,
        session_id: Optional[str] = None,
        device_id: Optional[str] = None,
        origin_service_name: Optional[str] = None,
        country_code: Optional[str] = None,
    ) -> "EdgeRequestContext":
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
            device_id = request.headers["x-device-id"]

            edge_context = self.edgecontext_factory.new(
                authentication_token=token,
                loid_id=loid.id,
                loid_created_ms=loid.created,
                session_id=session.id,
                device_id=device_id,
            )
            edge_context.attach_context(request)

        :param authentication_token: A raw authentication token as returned by
            the authentication service.
        :param loid_id: ID for the current LoID in fullname format.
        :param loid_created_ms: Epoch milliseconds when the current LoID cookie
            was created.
        :param session_id: ID for the current session cookie.
        :param device_id: ID for the device where the request originated from.
        :param origin_service_name: Name for the "origin" service handling the
            request from the client.
        :param country_code: two-character ISO 3166-1 country code where the
            request orginated from.

        """
        if loid_id is not None and not loid_id.startswith("t2_"):
            raise ValueError(
                "loid_id <%s> is not in a valid format, it should be in the "
                "fullname format with the '0' padding removed: 't2_loid_id'" % loid_id
            )

        if country_code is not None and not COUNTRY_CODE_RE.match(country_code):
            raise ValueError(
                "country_code <%s> is not in a valid format, it should be in "
                "ISO 3166-1 alpha-2 format: 'US'" % country_code
            )

        t_request = TRequest(
            loid=TLoid(id=loid_id, created_ms=loid_created_ms),
            session=TSession(id=session_id),
            authentication_token=authentication_token,
            device=TDevice(id=device_id),
            origin_service=TOriginService(name=origin_service_name),
            geolocation=TGeolocation(country_code=country_code),
        )
        header = TSerialization.serialize(t_request, EdgeRequestContext._HEADER_PROTOCOL_FACTORY)

        context = EdgeRequestContext(self.authn_token_validator, header)
        # Set the _t_request property so we can skip the deserialization step
        # since we already have the thrift object.
        context._t_request = t_request
        return context

    def from_upstream(self, edge_header: Optional[bytes]) -> "EdgeRequestContext":
        """Create and return an EdgeRequestContext from an upstream header.

        This is generally used internally to Baseplate by framework
        integrations that automatically pick up context from inbound requests.

        :param edge_header: Raw payload of Edge-Request header from upstream
            service.

        """
        return EdgeRequestContext(self.authn_token_validator, edge_header)


class EdgeRequestContext:
    """Contextual information about the initial request to an edge service.

    Construct this using an
    :py:class:`~baseplate.lib.edge_context.EdgeRequestContextFactory`.

    """

    _HEADER_PROTOCOL_FACTORY = TBinaryProtocolAcceleratedFactory()

    def __init__(
        self, authn_token_validator: AuthenticationTokenValidator, header: Optional[bytes]
    ):
        self._authn_token_validator = authn_token_validator
        self._header = header

    def attach_context(self, context: RequestContext) -> None:
        """Attach this to the provided :py:class:`~baseplate.RequestContext`.

        :param context: request context to attach this to

        """
        context.request_context = self
        context.raw_request_context = self._header

    def event_fields(self) -> Dict[str, Any]:
        """Return fields to be added to events."""
        fields = {"session_id": self.session.id}
        if self.device.id:
            fields["device_id"] = self.device.id
        fields.update(self.user.event_fields())
        fields.update(self.oauth_client.event_fields())
        return fields

    @cached_property
    def authentication_token(self) -> AuthenticationToken:
        return self._authn_token_validator.validate(self._t_request.authentication_token)

    @cached_property
    def user(self) -> User:
        """:py:class:`~baseplate.lib.edge_context.User` object for the current context."""
        return User(
            authentication_token=self.authentication_token,
            loid=self._t_request.loid.id,
            cookie_created_ms=self._t_request.loid.created_ms,
        )

    @cached_property
    def oauth_client(self) -> OAuthClient:
        """:py:class:`~baseplate.lib.edge_context.OAuthClient` object for the current context."""
        return OAuthClient(self.authentication_token)

    @cached_property
    def device(self) -> Device:
        """:py:class:`~baseplate.lib.edge_context.Device` object for the current context."""
        return Device(id=self._t_request.device.id)

    @cached_property
    def session(self) -> Session:
        """:py:class:`~baseplate.lib.edge_context.Session` object for the current context."""
        return Session(id=self._t_request.session.id)

    @cached_property
    def service(self) -> Service:
        """:py:class:`~baseplate.lib.edge_context.Service` object for the current context."""
        return Service(self.authentication_token)

    @cached_property
    def origin_service(self) -> OriginService:
        """:py:class:`~baseplate.lib.edge_context.Origin` object for the current context."""
        return OriginService(self._t_request.origin_service.name)

    @cached_property
    def geolocation(self) -> Geolocation:
        """:py:class:`~baseplate.core.Geolocation` object for the current context."""
        return Geolocation(country_code=self._t_request.geolocation.country_code)

    @cached_property
    def _t_request(self) -> TRequest:  # pylint: disable=method-hidden
        _t_request = TRequest()
        _t_request.loid = TLoid()
        _t_request.session = TSession()
        _t_request.device = TDevice()
        _t_request.origin_service = TOriginService()
        _t_request.geolocation = TGeolocation()
        if self._header:
            try:
                TSerialization.deserialize(_t_request, self._header, self._HEADER_PROTOCOL_FACTORY)
            except Exception:
                logger.debug("Invalid Edge-Request header. %s", self._header)
        return _t_request
