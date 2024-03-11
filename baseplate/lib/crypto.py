"""Utilities for common cryptographic operations.

.. testsetup::

    import datetime
    from baseplate.lib.crypto import make_signature, validate_signature
    from baseplate.lib.secrets import SecretsStore

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
import base64
import binascii
import datetime
import hashlib
import hmac
import struct
import time

from typing import NamedTuple

from baseplate.lib.secrets import VersionedSecret


class SignatureError(Exception):
    """Base class for all message signing related errors."""


class UnreadableSignatureError(SignatureError):
    """Raised when the signature is corrupt or wrongly formatted."""


class IncorrectSignatureError(SignatureError):
    """Raised when the signature is readable but does not match the message."""


class ExpiredSignatureError(SignatureError):
    """Raised when the signature is valid but has expired.

    The ``expiration`` attribute is the time (as seconds since the UNIX epoch)
    at which the signature expired.

    """

    def __init__(self, expiration: int):
        self.expiration = expiration
        super().__init__()


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


class SignatureInfo(NamedTuple):
    """Information about a valid signature.

    :ivar version: The version of the packed signature format.
    :ivar expiration: The time, in seconds since the UNIX epoch, at which
        the signature will expire.

    """

    version: int
    expiration: int


def _compute_digest(secret_value: bytes, header: bytes, message: str) -> bytes:
    payload = header + message.encode("utf8")
    digest = hmac.new(secret_value, payload, hashlib.sha256).digest()  # pylint: disable=no-member
    return digest


def make_signature(secret: VersionedSecret, message: str, max_age: datetime.timedelta) -> bytes:
    """Return a signature for the given message.

    To ensure that key rotation works automatically, always fetch the secret
    token from the secret store immediately before use and do not cache / save
    the token anywhere. The ``current`` version of the secret will be used to
    sign the token.

    :param secret: The secret signing key from the secret store.
    :param message: The message to sign.
    :param max_age: The amount of time in the future the signature will be valid for.
    :return: An encoded signature.

    """
    version = 1
    expiration = int(time.time() + max_age.total_seconds())
    header = _HEADER_FORMAT.pack(version, expiration)
    digest = _compute_digest(secret.current, header, message)
    return base64.urlsafe_b64encode(header + digest)


def validate_signature(secret: VersionedSecret, message: str, signature: bytes) -> SignatureInfo:
    """Validate and assert a message's signature is correct.

    If the signature is valid, the function will return normally with a
    :py:class:`SignatureInfo` with some details about the signature.
    Otherwise, an exception will be raised.

    To ensure that key rotation works automatically, always fetch the secret
    token from the secret store immediately before use and do not cache / save
    the token anywhere. All active versions of the secret will be checked when
    validating the signature.

    :param secret: The secret signing key from the secret store.
    :param message: The message payload to validate.
    :param signature: The signature supplied with the message.
    :raises: :py:exc:`UnreadableSignatureError` The signature is corrupt.
    :raises: :py:exc:`IncorrectSignatureError` The digest is incorrect.
    :raises: :py:exc:`ExpiredSignatureError` The signature expired.

    """
    version: int
    expiration: int

    try:
        signature_bytes = base64.urlsafe_b64decode(signature)
        header = signature_bytes[: _HEADER_FORMAT.size]
        signature_digest = signature_bytes[_HEADER_FORMAT.size :]
        version, expiration = _HEADER_FORMAT.unpack(header)
        if version != 1:
            raise ValueError
        if len(signature_digest) != hashlib.sha256().digest_size:  # pylint: disable=no-member
            raise ValueError
    except (struct.error, KeyError, binascii.Error, TypeError, ValueError):
        raise UnreadableSignatureError

    if time.time() > expiration:
        raise ExpiredSignatureError(expiration)

    for secret_value in secret.all_versions:
        digest = _compute_digest(secret_value, header, message)
        if hmac.compare_digest(digest, signature_digest):
            break
    else:
        raise IncorrectSignatureError

    return SignatureInfo(version, expiration)
