from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import unittest

from baseplate import crypto

from .. import mock


class ConstantTimeCompareTests(unittest.TestCase):
    def test_equal(self):
        self.assertTrue(crypto.constant_time_compare("abcdefg", "abcdefg"))

    def test_inequal(self):
        self.assertFalse(crypto.constant_time_compare("abcdefg", "hijklmnop"))

    def test_empty(self):
        self.assertFalse(crypto.constant_time_compare("abcdefg", ""))
        self.assertFalse(crypto.constant_time_compare("", "hijklmnop"))


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

    def test_signature_urlsafe(self):
        signature = self.signer.make_signature(self.message, max_age=datetime.timedelta(seconds=30))
        self.assertTrue(b"=" not in signature)
