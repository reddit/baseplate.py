"""Application integration with the secret fetcher daemon."""

import base64
import binascii
import json
import logging

from typing import NamedTuple, Optional, Iterator, Dict, Any

from baseplate.lib import config
from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib.file_watcher import FileWatcher, WatchedFileNotAvailableError
from baseplate.lib import cached_property


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger(__name__)


class SecretNotFoundError(Exception):
    """Raised when the requested secret is not in the local vault."""

    def __init__(self, name: str):
        super().__init__(f"secret not found: {repr(name)}")
        self.name = name


class CorruptSecretError(Exception):
    """Raised when the requested secret does not match the expected format."""

    def __init__(self, path: str, message: str):
        super().__init__(f"{path}: {message}")
        self.path = path
        self.message = message


class SecretsNotAvailableError(Exception):
    """Raised when the secrets store was not accessible."""

    def __init__(self, inner: Exception):
        super().__init__(f"could not load secrets: {inner}")
        self.inner = inner


class VersionedSecret(NamedTuple):
    """A versioned secret.

    Versioned secrets allow for seamless rotation of keys. When using the
    secret to generate tokens (e.g. signing a message) always use the
    ``current`` value. When validating tokens, check against all the versions
    in ``all_versions``. This will allow keys to rotate smoothly even if not
    done instantly across all users of the secret.

    """

    previous: Optional[bytes]
    current: bytes
    next: Optional[bytes]

    @property
    def all_versions(self) -> Iterator[bytes]:
        """Return an iterator over the available versions of this secret."""
        yield self.current

        if self.previous is not None:
            yield self.previous

        if self.next is not None:
            yield self.next

    @classmethod
    def from_simple_secret(cls, value: bytes) -> "VersionedSecret":
        """Make a fake versioned secret from a single value.

        This is a backwards compatibility shim for use with APIs that take
        versioned secrets. Try to use proper versioned secrets fetched from the
        secrets store instead.

        """
        return cls(previous=None, current=value, next=None)


class CredentialSecret(NamedTuple):
    """A secret for storing username/password pairs.

    Credential secrets allow us to store usernames and passwords together in a
    single secret.  Note that they are not versioned since the general pattern
    for rotating credenitals like this would be to generate a new username/password
    pair.  This object has two properties:

    """

    username: str
    password: str


def _decode_secret(path: str, encoding: str, value: str) -> bytes:
    if encoding == "identity":
        # encode to bytes for consistency with the base64 path. utf-8 because
        # that undoes json encoding.
        return value.encode("utf-8")

    if encoding == "base64":
        try:
            return base64.b64decode(value)
        except (TypeError, binascii.Error) as exc:
            raise CorruptSecretError(path, "unable to decode base64: %s" % exc)

    raise CorruptSecretError(path, "unknown encoding: %r" % encoding)


class SecretsStore(ContextFactory):
    """Access to secret tokens with automatic refresh when changed.

    This local vault allows access to the secrets cached on disk by the fetcher
    daemon. It will automatically reload the cache when it is changed. Do not
    cache or store the values returned by this class's methods but rather get
    them from this class each time you need them. The secrets are served from
    memory so there's little performance impact to doing so and you will be
    sure to always have the current version in the face of key rotation etc.

    """

    def __init__(self, path: str, timeout: Optional[int] = None):
        self._filewatcher = FileWatcher(path, json.load, timeout=timeout)

    def _get_data(self) -> Any:
        try:
            return self._filewatcher.get_data()
        except WatchedFileNotAvailableError as exc:
            raise SecretsNotAvailableError(exc)

    def get_raw(self, path: str) -> Dict[str, str]:
        """Return a dictionary of key/value pairs for the given secret path.

        This is the raw representation of the secret in the underlying store.

        """
        data = self._get_data()

        try:
            return data["secrets"][path]
        except KeyError:
            raise SecretNotFoundError(path)

    def get_credentials(self, path: str) -> CredentialSecret:
        """Decode and return a credential secret.

        Credential secrets are a convention of username/password pairs stored as
        separate values in the raw secret payload.

        The following keys are significant:

        ``type``
            This must always be ``credential`` for this method.
        ``encoding``
            This must be un-set or set to ``identity``.
        ``username``
            This contains the raw username.
        ``password``
            This contains the raw password.

        """
        secret_attributes = self.get_raw(path)

        if secret_attributes.get("type") != "credential":
            raise CorruptSecretError(path, "secret does not have type=credential")

        encoding = secret_attributes.get("encoding", "identity")

        if encoding != "identity":
            raise CorruptSecretError(
                path, "secret has encoding=%s rather than " "encoding=identity" % encoding
            )

        values = {}
        for key in ("username", "password"):
            try:
                val = secret_attributes[key]
                if not isinstance(val, str):
                    raise CorruptSecretError(path, "secret value '%s' is not a string" % key)
                values[key] = val
            except KeyError:
                raise CorruptSecretError(path, "secret does not have key '%s'" % key)

        return CredentialSecret(**values)

    def get_simple(self, path: str) -> bytes:
        """Decode and return a simple secret.

        Simple secrets are a convention of key/value pairs in the raw secret
        payload.  The following keys are significant:

        ``type``
            This must always be ``simple`` for this method.
        ``value``
            This contains the raw value of the secret token.
        ``encoding``
            (Optional) If present, how to decode the value from how it's
            encoded at rest (only ``base64`` currently supported).

        """
        secret_attributes = self.get_raw(path)

        if secret_attributes.get("type") != "simple":
            raise CorruptSecretError(path, "secret does not have type=simple")

        try:
            value = secret_attributes["value"]
        except KeyError:
            raise CorruptSecretError(path, "secret does not have value")

        encoding = secret_attributes.get("encoding", "identity")
        return _decode_secret(path, encoding, value)

    def get_versioned(self, path: str) -> VersionedSecret:
        """Decode and return a versioned secret.

        Versioned secrets are a convention of key/value pairs in the raw secret
        payload. The following keys are significant:

        ``type``
            This must always be ``versioned`` for this method.
        ``current``, ``next``, and ``previous``
            The raw secret value's versions. ``current`` is the "active"
            version, which is used for new creation/signing operations.
            ``previous`` and ``next`` are only used for validation (e.g.
            checking signatures) to ensure continuity when keys rotate. Both
            ``previous`` and ``next`` are optional.
        ``encoding``
            (Optional) If present, how to decode the values from how they are
            encoded at rest (only ``base64`` currently supported).

        """
        secret_attributes = self.get_raw(path)

        if secret_attributes.get("type") != "versioned":
            raise CorruptSecretError(path, "secret does not have type=versioned")

        previous_value = secret_attributes.get("previous")
        next_value = secret_attributes.get("next")

        try:
            current_value = secret_attributes["current"]
        except KeyError:
            raise CorruptSecretError(path, "secret does not have 'current' value")

        encoding = secret_attributes.get("encoding", "identity")
        return VersionedSecret(
            previous=_decode_secret(path, encoding, previous_value) if previous_value else None,
            current=_decode_secret(path, encoding, current_value),
            next=_decode_secret(path, encoding, next_value) if next_value else None,
        )

    def get_vault_url(self) -> str:
        """Return the URL for accessing Vault directly.

        .. seealso:: The :py:mod:`baseplate.clients.hvac` module provides
            integration with HVAC, a Vault client.

        """
        data = self._get_data()
        return data["vault"]["url"]

    def get_vault_token(self) -> str:
        """Return a Vault authentication token.

        The token will have policies attached based on the current EC2 server's
        Vault role. This is only necessary if talking directly to Vault.

        .. seealso:: The :py:mod:`baseplate.clients.hvac` module provides
            integration with HVAC, a Vault client.

        """
        data = self._get_data()
        return data["vault"]["token"]

    def make_object_for_context(self, name: str, span: Span) -> "SecretsStore":
        """Return an object that can be added to the context object.

        This allows the secret store to be used with
        :py:meth:`~baseplate.Baseplate.add_to_context`::

           secrets = SecretsStore("/var/local/secrets.json")
           baseplate.add_to_context("secrets", secrets)

        """
        return _CachingSecretsStore(self._filewatcher)


class _CachingSecretsStore(SecretsStore):
    """Lazily load and cache the parsed data until the server span ends."""

    def __init__(self, filewatcher: FileWatcher):  # pylint: disable=super-init-not-called
        self._filewatcher = filewatcher

    @cached_property
    def _data(self):
        return super()._get_data()

    def _get_data(self) -> Dict:
        return self._data


def secrets_store_from_config(
    app_config: config.RawConfig, timeout: Optional[int] = None, prefix: str = "secrets."
) -> SecretsStore:
    """Configure and return a secrets store.

    The keys useful to :py:func:`secrets_store_from_config` should be prefixed, e.g.
    ``secrets.url``, etc.

    Supported keys:

    ``path``: the path to the secrets file generated by the secrets fetcher daemon.

    :param app_config: The application configuration which should have
        settings for the secrets store.
    :param timeout: How long, in seconds, to block instantiation waiting
        for the secrets data to become available (defaults to not blocking).
    :param prefix: Specifies the prefix used to filter keys. Defaults
        to "secrets."

    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]

    cfg = config.parse_config(
        app_config,
        {
            config_prefix: {
                "path": config.Optional(config.String, default="/var/local/secrets.json")
            }
        },
    )
    options = getattr(cfg, config_prefix)

    # pylint: disable=maybe-no-member
    return SecretsStore(options.path, timeout=timeout)
