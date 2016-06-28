"""Utilities for common cryptographic operations.


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


class MessageSigner(object):
    """Helper which signs messages and validates signatures given a secret.

    .. testsetup::

        import datetime
        from baseplate.crypto import MessageSigner

    .. testcode::

        message = "Hello, world!"

        signer = MessageSigner(b"supersecret")
        signature = signer.make_signature(
            message, max_age=datetime.timedelta(days=1))

        try:
            signer.validate_signature(message, signature)
        except SignatureError:
            print("Oh no, it was invalid!")
        else:
            print("Message was valid!")

    .. testoutput::

        Message was valid!

    """
    def __init__(self, secret_key):
        self.secret_key = secret_key

    def _compute_digest(self, header, message):
        payload = header + message.encode("utf8")
        digest = hmac.new(self.secret_key, payload, hashlib.sha256).digest()  # pylint: disable=no-member
        return digest

    def make_signature(self, message, max_age):
        """Return a signature for the given message.

        :param str message: The message to sign.
        :param datetime.timedelta max_age: The amount of time in the future
            the signature will be valid for.
        :return: An encoded signature.

        """
        version = 1
        expiration = int(time.time() + max_age.total_seconds())
        header = _HEADER_FORMAT.pack(version, expiration)
        digest = self._compute_digest(header, message)
        return base64.urlsafe_b64encode(header + digest)

    def validate_signature(self, message, signature):
        """Validate and assert a message's signature is correct.

        If the signature is valid, the function will return normally with a
        :py:class:`SignatureInfo` with some details about the signature.
        Otherwise, an exception will be raised.

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

        digest = self._compute_digest(header, message)
        if not constant_time_compare(digest, signature_digest):
            raise IncorrectSignatureError

        if time.time() > expiration:
            raise ExpiredSignatureError(expiration)

        return SignatureInfo(version, expiration)
