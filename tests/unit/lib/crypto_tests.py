import datetime
import unittest

from unittest import mock

from baseplate.lib import crypto
from baseplate.lib.secrets import VersionedSecret


class SignatureTests(unittest.TestCase):
    def setUp(self):
        self.message = "Hello, this is a message."
        self.secret = b"abcdefg"
        self.signer = crypto.MessageSigner(self.secret)

    def test_roundtrip(self):
        signature = self.signer.make_signature(self.message, max_age=datetime.timedelta(seconds=30))
        self.signer.validate_signature(self.message, signature)

    @mock.patch("time.time")
    def test_signature_info(self, time):
        time.return_value = 0
        signature = self.signer.make_signature(self.message, max_age=datetime.timedelta(seconds=30))

        info = self.signer.validate_signature(self.message, signature)

        self.assertEqual(info.version, 1)
        self.assertEqual(info.expiration, 30)

    @mock.patch("time.time")
    def test_expired(self, time):
        time.return_value = 0
        signature = self.signer.make_signature(self.message, max_age=datetime.timedelta(seconds=30))

        time.return_value = 90
        with self.assertRaises(crypto.ExpiredSignatureError):
            self.signer.validate_signature(self.message, signature)

    def test_unreadable(self):
        signature = self.signer.make_signature(self.message, max_age=datetime.timedelta(seconds=30))

        with self.assertRaises(crypto.UnreadableSignatureError):
            self.signer.validate_signature(self.message, signature[2:])

    def test_invalid(self):
        bad_signature = self.signer.make_signature(
            self.message + "bad", max_age=datetime.timedelta(seconds=30)
        )

        with self.assertRaises(crypto.IncorrectSignatureError):
            self.signer.validate_signature(self.message, bad_signature)

    def test_signature_urlsafe(self):
        signature = self.signer.make_signature(self.message, max_age=datetime.timedelta(seconds=30))
        self.assertTrue(b"=" not in signature)


class VersionedSecretTests(unittest.TestCase):
    @mock.patch("time.time")
    def test_versioned(self, time):
        time.return_value = 1000

        message = "hello!"
        max_age = datetime.timedelta(seconds=30)

        versioned = VersionedSecret(previous=b"one", current=b"two", next=b"three")

        previous = VersionedSecret.from_simple_secret(versioned.previous)
        current = VersionedSecret.from_simple_secret(versioned.current)
        next = VersionedSecret.from_simple_secret(versioned.next)

        self.assertEqual(
            crypto.make_signature(versioned, message, max_age),
            crypto.make_signature(current, message, max_age),
        )

        signature = crypto.make_signature(previous, message, max_age)
        info = crypto.validate_signature(versioned, message, signature)
        self.assertEqual(info.expiration, 1030)

        signature = crypto.make_signature(current, message, max_age)
        info = crypto.validate_signature(versioned, message, signature)
        self.assertEqual(info.expiration, 1030)

        signature = crypto.make_signature(next, message, max_age)
        info = crypto.validate_signature(versioned, message, signature)
        self.assertEqual(info.expiration, 1030)
