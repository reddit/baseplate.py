"""Application integration with the secret fetcher daemon."""

import base64
import binascii
import json
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple, Optional, Protocol

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import cached_property, config, warn_deprecated
from baseplate.lib.file_watcher import FileWatcher, WatchedFileNotAvailableError

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
    for rotating credentials like this would be to generate a new username/password
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
            raise CorruptSecretError(path, f"unable to decode base64: {exc}")

    raise CorruptSecretError(path, f"unknown encoding: {encoding!r}")


class SecretParser(Protocol):
    def __call__(self, data: dict[str, Any], secret_path: str = "") -> dict[str, str]: ...


def parse_secrets_fetcher(data: dict[str, Any], secret_path: str = "") -> dict[str, str]:
    try:
        return data["secrets"][secret_path]
    except KeyError:
        raise SecretNotFoundError(secret_path)


# pylint: disable=unused-argument
def parse_vault_csi(data: dict[str, Any], secret_path: str = "") -> dict[str, str]:
    return data["data"]


class SecretsStore(ContextFactory):
    """Access to secret tokens with automatic refresh when changed.

    This local vault allows access to the secrets cached on disk by the fetcher
    daemon. It will automatically reload the cache when it is changed. Do not
    cache or store the values returned by this class's methods but rather get
    them from this class each time you need them. The secrets are served from
    memory so there's little performance impact to doing so and you will be
    sure to always have the current version in the face of key rotation etc.

    """

    def __init__(
        self,
        path: str,
        timeout: Optional[int] = None,
        backoff: Optional[float] = None,
        parser: Optional[SecretParser] = None,
    ):  # pylint: disable=super-init-not-called
        self.parser = parser or parse_secrets_fetcher
        self._filewatcher = FileWatcher(path, json.load, timeout=timeout, backoff=backoff)

    def _get_data(self) -> tuple[Any, float]:
        try:
            return self._filewatcher.get_data_and_mtime()
        except WatchedFileNotAvailableError as exc:
            raise SecretsNotAvailableError(exc)

    def get_raw(self, path: str) -> dict[str, str]:
        """Return a dictionary of key/value pairs for the given secret path.

        This is the raw representation of the secret in the underlying store.

        """
        return self.get_raw_and_mtime(path)[0]

    def get_credentials(self, path: str) -> CredentialSecret:
        """Decode and return a credential secret.

        Credential secrets are a convention of username/password pairs stored as
        separate values in the raw secret payload.

        The following keys are significant:

        ``type``
            This must always be ``credential`` for this method.
        ``encoding``
            This must be unset or set to ``identity``.
        ``username``
            This contains the raw username.
        ``password``
            This contains the raw password.

        """
        return self.get_credentials_and_mtime(path)[0]

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
        return self.get_simple_and_mtime(path)[0]

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
        return self.get_versioned_and_mtime(path)[0]

    def get_vault_url(self) -> str:
        """Return the URL for accessing Vault directly."""
        warn_deprecated("get_vault_url is deprecated and will be removed in v3.0.")
        data, _ = self._get_data()
        return data["vault"]["url"]

    def get_vault_token(self) -> str:
        """Return a Vault authentication token.

        The token will have policies attached based on the current EC2 server's
        Vault role. This is only necessary if talking directly to Vault.

        """
        warn_deprecated("get_vault_token is deprecated and will be removed in v3.0.")
        data, _ = self._get_data()
        return data["vault"]["token"]

    def get_raw_and_mtime(self, secret_path: str) -> tuple[dict[str, str], float]:
        """Return raw secret and modification time.

        This returns the same data as :py:meth:`get_raw` as well as a UNIX
        epoch timestamp indicating the last time the secrets data was updated.
        This modification time can be used to know when to invalidate
        downstream caching.

        .. versionadded:: 1.5

        """
        data, mtime = self._get_data()
        return self.parser(data, secret_path), mtime

    def get_credentials_and_mtime(self, path: str) -> tuple[CredentialSecret, float]:
        """Return credentials secret and modification time.

        This returns the same data as :py:meth:`get_credentials` as well as a
        UNIX epoch timestamp indicating the last time the secrets data was
        updated.  This modification time can be used to know when to invalidate
        downstream caching.

        .. versionadded:: 1.5

        """
        secret_attributes, mtime = self.get_raw_and_mtime(path)

        if secret_attributes.get("type") != "credential":
            raise CorruptSecretError(path, "secret does not have type=credential")

        encoding = secret_attributes.get("encoding", "identity")

        if encoding != "identity":
            raise CorruptSecretError(
                path, f"secret has encoding={encoding} rather than encoding=identity"
            )

        values = {}
        for key in ("username", "password"):
            try:
                val = secret_attributes[key]
                if not isinstance(val, str):
                    raise CorruptSecretError(path, f"secret value '{key}' is not a string")
                values[key] = val
            except KeyError:
                raise CorruptSecretError(path, f"secret does not have key '{key}'")

        return CredentialSecret(**values), mtime

    def get_simple_and_mtime(self, path: str) -> tuple[bytes, float]:
        """Return simple secret and modification time.

        This returns the same data as :py:meth:`get_simple` as well as a UNIX
        epoch timestamp indicating the last time the secrets data was updated.
        This modification time can be used to know when to invalidate
        downstream caching.

        .. versionadded:: 1.5

        """
        secret_attributes, mtime = self.get_raw_and_mtime(path)

        if secret_attributes.get("type") != "simple":
            raise CorruptSecretError(path, "secret does not have type=simple")

        try:
            value = secret_attributes["value"]
        except KeyError:
            raise CorruptSecretError(path, "secret does not have value")

        encoding = secret_attributes.get("encoding", "identity")
        return _decode_secret(path, encoding, value), mtime

    def get_versioned_and_mtime(self, path: str) -> tuple[VersionedSecret, float]:
        """Return versioned secret and modification time.

        This returns the same data as :py:meth:`get_versioned` as well as a
        UNIX epoch timestamp indicating the last time the secrets data was
        updated.  This modification time can be used to know when to invalidate
        downstream caching.

        .. versionadded:: 1.5

        """
        secret_attributes, mtime = self.get_raw_and_mtime(path)

        if secret_attributes.get("type") != "versioned":
            raise CorruptSecretError(path, "secret does not have type=versioned")

        previous_value = secret_attributes.get("previous")
        next_value = secret_attributes.get("next")

        try:
            current_value = secret_attributes["current"]
        except KeyError:
            raise CorruptSecretError(path, "secret does not have 'current' value")

        encoding = secret_attributes.get("encoding", "identity")
        return (
            VersionedSecret(
                previous=_decode_secret(path, encoding, previous_value) if previous_value else None,
                current=_decode_secret(path, encoding, current_value),
                next=_decode_secret(path, encoding, next_value) if next_value else None,
            ),
            mtime,
        )

    def make_object_for_context(self, name: str, span: Span) -> "SecretsStore":
        """Return an object that can be added to the context object.

        This allows the secret store to be used with
        :py:meth:`~baseplate.Baseplate.add_to_context`::

           secrets = SecretsStore("/var/local/secrets.json")
           baseplate.add_to_context("secrets", secrets)

        """
        return _CachingSecretsStore(self._filewatcher, self.parser)


class _CachingSecretsStore(SecretsStore):
    """Lazily load and cache the parsed data until the server span ends."""

    def __init__(self, filewatcher: FileWatcher, parser: SecretParser):  # pylint: disable=super-init-not-called
        self._filewatcher = filewatcher
        self.parser = parser

    @cached_property
    def _data(self) -> tuple[Any, float]:
        return super()._get_data()

    def _get_data(self) -> tuple[dict, float]:
        return self._data


class VaultCSIEntry(NamedTuple):
    mtime: float
    data: Any


class VaultCSISecretsStore(SecretsStore):
    """Access to secret tokens using a vault CSI mount with automatic refresh.

    This store allows access to secrets stored in vault through the CSI interface.
    It performs caching and will automatically reload secrets from disk. Generally
    do not cache or store the values returned by this class's methods, rather get
    them from this class each time you need them. The secrets are served from memory
    when possible, so there's little performance impact in doing so, and you will be
    sure to always have the current version in the face of key rotation etc.
    """

    path: Path
    data_symlink: Path
    cache: dict[str, VaultCSIEntry]

    def __init__(
        self,
        path: str,
        parser: SecretParser,
    ):  # pylint: disable=super-init-not-called
        self.path = Path(path)
        self.parser = parser
        self.cache = {}
        self.data_symlink = self.path.joinpath("..data")
        if not self.path.is_dir():
            raise ValueError(f"Expected {self.path} to be a directory.")
        if not self.data_symlink.is_dir():
            raise ValueError(
                f"Expected {self.data_symlink} to be a directory. Verify {self.path} is the root of the Vault CSI mount."  # noqa: E501
            )

    def get_vault_url(self) -> str:
        """Deprecated and will be removed in 3.0.0"""
        raise NotImplementedError

    def get_vault_token(self) -> str:
        """Deprecated and will be removed in 3.0.0"""
        raise NotImplementedError

    def _get_mtime(self) -> float:
        """Modification time is store-wide for CSI secrets."""
        return os.path.getmtime(self.data_symlink)

    def _raw_secret(self, name: str) -> Any:
        try:
            with open(self.data_symlink.joinpath(name), encoding="UTF-8") as fp:
                return self.parser(json.load(fp))
        except FileNotFoundError as exc:
            raise SecretNotFoundError(name) from exc

    def get_raw_and_mtime(self, secret_path: str) -> tuple[dict[str, str], float]:
        mtime = self._get_mtime()
        if cache_entry := self.cache.get(secret_path):
            if cache_entry.mtime == mtime:
                return cache_entry.data, mtime
        secret_data = self._raw_secret(secret_path)
        self.cache[secret_path] = VaultCSIEntry(
            mtime=mtime,
            data=secret_data,
        )
        return secret_data, mtime

    def make_object_for_context(self, name: str, span: Span) -> SecretsStore:
        return self


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
    :param backoff: retry backoff time for secrets file watcher. Defaults to
        None, which is mapped to DEFAULT_FILEWATCHER_BACKOFF.
    :param provider: The secrets provider, acceptable values are 'vault' and
        'vault_csi'. Defaults to 'vault'

    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]

    cfg = config.parse_config(
        app_config,
        {
            config_prefix: {
                "path": config.Optional(config.String, default="/var/local/secrets.json"),
                "provider": config.Optional(config.String, default="vault"),
                "backoff": config.Optional(config.Timespan),
            }
        },
    )
    options = getattr(cfg, config_prefix)

    if options.backoff:
        backoff = options.backoff.total_seconds()
    else:
        backoff = None

    if options.provider == "vault_csi":
        return VaultCSISecretsStore(options.path, parser=parse_vault_csi)

    return SecretsStore(
        options.path, timeout=timeout, backoff=backoff, parser=parse_secrets_fetcher
    )
