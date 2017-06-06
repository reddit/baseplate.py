"""Application integration with the secret fetcher daemon."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import binascii
import collections
import json
import logging
import os

from .. import config
from ..context import ContextFactory


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger(__name__)


class SecretNotFoundError(Exception):
    """Raised when the requested secret is not in the local vault."""

    def __init__(self, name):
        super(SecretNotFoundError, self).__init__()
        self.name = name

    def __str__(self):  # pragma: nocover
        return "secret not found: {!r}".format(self.name)


class CorruptSecretError(Exception):
    """Raised when the requested secret does not match the expected format."""

    def __init__(self, path, message):
        super(CorruptSecretError, self).__init__()
        self.path = path
        self.message = message

    def __str__(self):  # pragma: nocover
        return "{}: {}".format(self.path, self.message)


class SecretsNotAvailableError(Exception):
    """Raised when the secrets store was not accessible."""

    def __init__(self, inner):
        super(SecretsNotAvailableError, self).__init__()
        self.inner = inner

    def __str__(self):  # pragma: nocover
        return "could not load secrets: {}".format(self.inner)


_VersionedSecret = collections.namedtuple(
    "VersionedSecret", "previous current next")


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
        return cls(
            previous=None,
            current=value,
            next=None,
        )


def _decode_secret(path, encoding, value):
    if encoding == "identity":
        # encode to bytes for consistency with the base64 path. utf-8 because
        # that undoes json encoding.
        return value.encode("utf-8")
    elif encoding == "base64":
        try:
            return base64.b64decode(value)
        except (TypeError, binascii.Error) as exc:
            raise CorruptSecretError(path, "unable to decode base64: %s" % exc)
    else:
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

    def __init__(self, path):
        self._path = path
        self._mtime = 0
        self._data = None

    def _load_if_needed(self):
        """Load the secrets from disk if modified since last read.

        It's important to reload if changed because this allows configuration
        changes of the fetcher daemon to be picked up automatically by running
        services without being restarted and also makes us less susceptible to
        race conditions when both are being restarted at the same time.

        """
        try:
            secrets_file_updated = self._mtime < os.path.getmtime(self._path)
        except OSError:
            secrets_file_updated = False

        if self._data is None or secrets_file_updated:
            logger.debug("Loading secrets from %s.", self._path)

            try:
                with open(self._path) as f:
                    data = json.load(f)
                    mtime = os.fstat(f.fileno()).st_mtime
            except IOError as exc:
                raise SecretsNotAvailableError(exc)

            self._data = data
            self._mtime = mtime

    def get_raw(self, path):
        """Return a dictionary of key/value pairs for the given secret path.

        This is the raw representation of the secret in the underlying store.

        :rtype: :py:class:`dict`

        """
        self._load_if_needed()

        try:
            return self._data["secrets"][path]
        except KeyError:
            raise SecretNotFoundError(path)

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
        self._load_if_needed()
        return self._data["vault"]["url"]

    def get_vault_token(self):
        """Return a Vault authentication token.

        The token will have policies attached based on the current EC2 server's
        Vault role. This is only necessary if talking directly to Vault.

        :rtype: :py:class:`str`

        .. seealso:: The :py:mod:`baseplate.context.hvac` module provides
            integration with HVAC, a Vault client.

        """
        self._load_if_needed()
        return self._data["vault"]["token"]

    def make_object_for_context(self, name, server_span):  # pragma: nocover
        """Return an object that can be added to the context object.

        This allows the secret store to be used with
        :py:meth:`~baseplate.core.Baseplate.add_to_context`::

           secrets = SecretsStore("/var/local/secrets.json")
           baseplate.add_to_context("secrets", secrets)

        """
        return self


def secrets_store_from_config(app_config):
    """Configure and return a secrets store.

    This expects one configuration option:

    ``secrets.path``
        The path to the secrets file generated by the secrets fetcher daemon.

    :param dict raw_config: The application configuration which should have
        settings for the secrets store.
    :rtype: :py:class:`SecretsStore`

    """
    cfg = config.parse_config(app_config, {
        "secrets": {
            "path": config.Optional(config.String, default="/var/local/secrets.json"),
        },
    })
    # pylint: disable=no-member
    return SecretsStore(cfg.secrets.path)
