"""Utilities for common cryptographic operations.

.. testsetup::

    import datetime
    from baseplate.crypto import make_signature, validate_signature
    from baseplate.secrets import SecretsStore

    secrets = SecretsStore("docs/secrets.json")

.. testcode::

    message = "Hello, world!"

    secret = secrets.get_versioned("some_signing_key")
    signature = make_signature(
        secret, message, max_age=datetime.timedelta(days=1))

    try:
        validate_signature(secret, message, signature)
    except SignatureError:
        print("Oh no, it was invalid!")
    else:
        print("Message was valid!")

.. testoutput::

    Message was valid!


"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import binascii
import collections
import hashlib
import hmac
import struct
import time

from baseplate._utils import warn_deprecated
from baseplate.secrets import VersionedSecret


if hasattr(hmac, "compare_digest"):
    # This was added in Python 2.7.7 and 3.3
    # pylint: disable=invalid-name,no-member
    constant_time_compare = hmac.compare_digest
else:
    def constant_time_compare(actual, expected):
        """Return whether or not two strings match.

        The time taken is dependent on the number of characters provided
        instead of the number of characters that match which makes this
        function resistant to timing attacks.

        """

        if type(actual) != type(expected):
            warn_deprecated("Future versions of constant_time_compare require that both "
                "parameters are of the same type.")

        actual_len = len(actual)
        expected_len = len(expected)
        result = actual_len ^ expected_len
        if expected_len > 0:
            for i in xrange(actual_len):
                result |= ord(actual[i]) ^ ord(expected[i % expected_len])
        return result == 0


class SignatureError(Exception):
    """Base class for all message signing related errors."""
    pass


class UnreadableSignatureError(SignatureError):
    """Raised when the signature is corrupt or wrongly formatted."""
    pass


class IncorrectSignatureError(SignatureError):
    """Raised when the signature is readable but does not match the message."""
    pass


class ExpiredSignatureError(SignatureError):
    """Raised when the signature is valid but has expired.

    The ``expiration`` attribute is the time (as seconds since the UNIX epoch)
    at which the signature expired.

    """
    def __init__(self, expiration):
        self.expiration = expiration
        super(ExpiredSignatureError, self).__init__()


# A signature is a base64 encoded binary blob, comprised of a header and
# digest.
#
# The first byte of the header is a version number indicating what format the
# signature is.
#
# In version 1, the only current version, the header then has two bytes of
# padding, to prevent base64 "=" padding, followed by the expiration time of
# the signature as seconds since the unix epoch. An HMAC-SHA256 digest follows.
_HEADER_FORMAT = struct.Struct("<BxxI")


_SignatureInfo = collections.namedtuple("_SignatureInfo",
    ["version", "expiration"])


class SignatureInfo(_SignatureInfo):
    """Information about a valid signature.

    :ivar int version: The version of the packed signature format.
    :ivar int expiration: The time, in seconds since the UNIX epoch, at which
        the signature will expire.

    """
    pass


def _compute_digest(secret_value, header, message):
    payload = header + message.encode("utf8")
    digest = hmac.new(secret_value, payload, hashlib.sha256).digest()  # pylint: disable=no-member
    return digest


def make_signature(secret, message, max_age):
    """Return a signature for the given message.

    To ensure that key rotation works automatically, always fetch the secret
    token from the secret store immediately before use and do not cache / save
    the token anywhere. The ``current`` version of the secret will be used to
    sign the token.

    :param baseplate.secrets.VersionedSecret secret: The secret signing key
        from the secret store.
    :param str message: The message to sign.
    :param datetime.timedelta max_age: The amount of time in the future
        the signature will be valid for.
    :return: An encoded signature.

    """
    version = 1
    expiration = int(time.time() + max_age.total_seconds())
    header = _HEADER_FORMAT.pack(version, expiration)
    digest = _compute_digest(secret.current, header, message)
    return base64.urlsafe_b64encode(header + digest)


def validate_signature(secret, message, signature):
    """Validate and assert a message's signature is correct.

    If the signature is valid, the function will return normally with a
    :py:class:`SignatureInfo` with some details about the signature.
    Otherwise, an exception will be raised.

    To ensure that key rotation works automatically, always fetch the secret
    token from the secret store immediately before use and do not cache / save
    the token anywhere. All active versions of the secret will be checked when
    validating the signature.

    :param baseplate.secrets.VersionedSecret secret: The secret signing key
        from the secret store.
    :param str message: The message payload to validate.
    :param str signature: The signature supplied with the message.
    :raises: :py:exc:`UnreadableSignatureError` The signature is corrupt.
    :raises: :py:exc:`IncorrectSignatureError` The digest is incorrect.
    :raises: :py:exc:`ExpiredSignatureError` The signature expired.
    :rtype: :py:class:`SignatureInfo`

    """
    try:
        signature_bytes = base64.urlsafe_b64decode(signature)
        header = signature_bytes[:_HEADER_FORMAT.size]
        signature_digest = signature_bytes[_HEADER_FORMAT.size:]
        version, expiration = _HEADER_FORMAT.unpack(header)
        if version != 1:
            raise ValueError
        if len(signature_digest) != hashlib.sha256().digest_size:  # pylint: disable=no-member
            raise ValueError
    except (struct.error, KeyError, binascii.Error, TypeError, ValueError):
        raise UnreadableSignatureError

    for secret_value in secret.all_versions:
        digest = _compute_digest(secret_value, header, message)
        if constant_time_compare(digest, signature_digest):
            break
    else:
        raise IncorrectSignatureError

    if time.time() > expiration:
        raise ExpiredSignatureError(expiration)

    return SignatureInfo(version, expiration)


class MessageSigner(object):
    """Helper which signs messages and validates signatures given a secret.

    This is for backwards compatibility. Use the secret store and proper
    versioned secrets for new code.

    """
    def __init__(self, secret_key):
        warn_deprecated("MessageSigner is deprecated in favor of the top-level "
                        "make_signature and validate_signature functions which "
                        "accept versioned secrets from the secret store.")
        self.secret = VersionedSecret.from_simple_secret(secret_key)

    def make_signature(self, message, max_age):
        return make_signature(self.secret, message, max_age)

    def validate_signature(self, message, signature):
        return validate_signature(self.secret, message, signature)
