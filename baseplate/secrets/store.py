"""Application integration with the secret fetcher daemon."""

import base64
import binascii
import collections
import json
import logging

from baseplate import config
from baseplate.context import ContextFactory
from baseplate.file_watcher import FileWatcher, WatchedFileNotAvailableError
from baseplate._utils import cached_property


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger(__name__)


class SecretNotFoundError(Exception):
    """Raised when the requested secret is not in the local vault."""

    def __init__(self, name):
        super(SecretNotFoundError, self).__init__("secret not found: {!r}".format(name))
        self.name = name


class CorruptSecretError(Exception):
    """Raised when the requested secret does not match the expected format."""

    def __init__(self, path, message):
        super(CorruptSecretError, self).__init__("{}: {}".format(path, message))
        self.path = path
        self.message = message


class SecretsNotAvailableError(Exception):
    """Raised when the secrets store was not accessible."""

    def __init__(self, inner):
        super(SecretsNotAvailableError, self).__init__("could not load secrets: {}".format(inner))
        self.inner = inner


_VersionedSecret = collections.namedtuple("VersionedSecret", "previous current next")


class VersionedSecret(_VersionedSecret):
    """A versioned secret.

    Versioned secrets allow for seamless rotation of keys. When using the
    secret to generate tokens (e.g. signing a message) always use the
    ``current`` value. When validating tokens, check against all the versions
    in ``all_versions``. This will allow keys to rotate smoothly even if not
    done instantly across all users of the secret.

    """

    @property
    def all_versions(self):
        """Return an iterator over the available versions of this secret."""
        yield self.current

        if self.previous is not None:
            yield self.previous

        if self.next is not None:
            yield self.next

    @classmethod
    def from_simple_secret(cls, value):
        """Make a fake versioned secret from a single value.

        This is a backwards compatibility shim for use with APIs that take
        versioned secrets. Try to use proper versioned secrets fetched from the
        secrets store instead.

        """
        return cls(previous=None, current=value, next=None)


_CredentialSecret = collections.namedtuple("CredentialSecret", "username password")


class CredentialSecret(_CredentialSecret):
    """A secret for storing username/password pairs.

    Credential secrets allow us to store usernames and passwords together in a
    single secret.  Note that they are not versioned since the general pattern
    for rotating credenitals like this would be to generate a new username/password
    pair.  This object has two properties:

    ``username``
        The username portion of the credentials as :py:class:`str`.
    ``password``
        The password portion of the credentials as :py:class:`str`.
    """


def _decode_secret(path, encoding, value):
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

    def __init__(self, path, timeout=None):
        self._filewatcher = FileWatcher(path, json.load, timeout=timeout)

    def _get_data(self):
        try:
            return self._filewatcher.get_data()
        except WatchedFileNotAvailableError as exc:
            raise SecretsNotAvailableError(exc)

    def get_raw(self, path):
        """Return a dictionary of key/value pairs for the given secret path.

        This is the raw representation of the secret in the underlying store.

        :rtype: :py:class:`dict`

        """
        data = self._get_data()

        try:
            return data["secrets"][path]
        except KeyError:
            raise SecretNotFoundError(path)

    def get_credentials(self, path):
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

        :rtype: :py:class:`CredentialSecret`

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

    def get_simple(self, path):
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

        :rtype: :py:class:`bytes`

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

    def get_versioned(self, path):
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

        :rtype: :py:class:`VersionedSecret`

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
            previous=previous_value and _decode_secret(path, encoding, previous_value),
            current=_decode_secret(path, encoding, current_value),
            next=next_value and _decode_secret(path, encoding, next_value),
        )

    def get_vault_url(self):
        """Return the URL for accessing Vault directly.

        :rtype: :py:class:`str`

        .. seealso:: The :py:mod:`baseplate.context.hvac` module provides
            integration with HVAC, a Vault client.

        """
        data = self._get_data()
        return data["vault"]["url"]

    def get_vault_token(self):
        """Return a Vault authentication token.

        The token will have policies attached based on the current EC2 server's
        Vault role. This is only necessary if talking directly to Vault.

        :rtype: :py:class:`str`

        .. seealso:: The :py:mod:`baseplate.context.hvac` module provides
            integration with HVAC, a Vault client.

        """
        data = self._get_data()
        return data["vault"]["token"]

    def make_object_for_context(self, name, span):
        """Return an object that can be added to the context object.

        This allows the secret store to be used with
        :py:meth:`~baseplate.core.Baseplate.add_to_context`::

           secrets = SecretsStore("/var/local/secrets.json")
           baseplate.add_to_context("secrets", secrets)

        """
        return _CachingSecretsStore(self._filewatcher)


class _CachingSecretsStore(SecretsStore):
    """Lazily load and cache the parsed data until the server span ends."""

    def __init__(self, filewatcher):  # pylint: disable=super-init-not-called
        self._filewatcher = filewatcher

    @cached_property
    def _data(self):
        return super(_CachingSecretsStore, self)._get_data()

    def _get_data(self):
        return self._data


def secrets_store_from_config(app_config, timeout=None, prefix="secrets."):
    """Configure and return a secrets store.

    The keys useful to :py:func:`secrets_store_from_config` should be prefixed, e.g.
    ``secrets.url``, etc.

    Supported keys:

    ``path``: the path to the secrets file generated by the secrets fetcher daemon.

    :param dict app_config: The application configuration which should have
        settings for the secrets store.
    :param float timeout: (Optional) How long, in seconds, to block instantiation waiting
        for the secrets data to become available (defaults to not blocking).
    :param str prefix: (Optional) specifies the prefix used to filter keys. Defaults
        to "secrets."
    :rtype: :py:class:`SecretsStore`

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
