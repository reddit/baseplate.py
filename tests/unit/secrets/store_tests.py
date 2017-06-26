from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.file_watcher import FileWatcher, WatchedFileNotAvailableError
from baseplate.secrets import store

from ... import mock


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.mock_filewatcher = mock.Mock(spec=FileWatcher)
        self.store = store.SecretsStore("/whatever")
        self.store._filewatcher = self.mock_filewatcher

    def test_file_not_found(self):
        self.mock_filewatcher.get_data.side_effect = WatchedFileNotAvailableError("path", None)

        with self.assertRaises(store.SecretsNotAvailableError):
            self.store.get_raw("test")

    def test_vault_info(self):
        self.mock_filewatcher.get_data.return_value = {
            "secrets": {},
            "vault": {
                "token": "test",
                "url": "http://vault.example.com:8200/",
            }
        }

        self.assertEqual(self.store.get_vault_token(), "test")
        self.assertEqual(self.store.get_vault_url(), "http://vault.example.com:8200/")

    def test_raw_secrets(self):
        self.mock_filewatcher.get_data.return_value = {
            "secrets": {
                "test": {
                    "something": "exists",
                },
            },
            "vault": {
                "token": "test",
                "url": "http://vault.example.com:8200/",
            }
        }

        self.assertEqual(self.store.get_raw("test"), {u"something": u"exists"})

        with self.assertRaises(store.SecretNotFoundError):
            self.store.get_raw("test_missing")

    def test_simple_secrets(self):
        self.mock_filewatcher.get_data.return_value = {
            "secrets": {
                "test": {
                    "type": "simple",
                    "value": "easy",
                },
                "test_base64": {
                    "type": "simple",
                    "value": "aHVudGVyMg==",
                    "encoding": "base64",
                },
                "test_unknown_encoding": {
                    "type": "simple",
                    "value": "sdlfkj",
                    "encoding": "mystery",
                },
                "test_not_simple": {
                    "something": "else",
                },
                "test_no_value": {
                    "type": "simple",
                },
                "test_bad_base64": {
                    "type": "simple",
                    "value": "aHVudGVyMg",
                    "encoding": "base64",
                }
            },
            "vault": {
                "token": "test",
                "url": "http://vault.example.com:8200/",
            }
        }

        self.assertEqual(self.store.get_simple("test"), b"easy")
        self.assertEqual(self.store.get_simple("test_base64"), b"hunter2")

        with self.assertRaises(store.CorruptSecretError):
            self.store.get_simple("test_unknown_encoding")

        with self.assertRaises(store.CorruptSecretError):
            self.store.get_simple("test_not_simple")

        with self.assertRaises(store.CorruptSecretError):
            self.store.get_simple("test_no_value")

        with self.assertRaises(store.CorruptSecretError):
            self.store.get_simple("test_bad_base64")

    def test_versioned_secrets(self):
        self.mock_filewatcher.get_data.return_value = {
            "secrets": {
                "test": {
                    "type": "versioned",
                    "current": "easy",
                },
                "test_base64": {
                    "type": "versioned",
                    "previous": "aHVudGVyMQ==",
                    "current": "aHVudGVyMg==",
                    "next": "aHVudGVyMw==",
                    "encoding": "base64",
                },
                "test_unknown_encoding": {
                    "type": "versioned",
                    "current": "sdlfkj",
                    "encoding": "mystery",
                },
                "test_not_versioned": {
                    "something": "else",
                },
                "test_no_value": {
                    "type": "versioned",
                },
                "test_bad_base64": {
                    "type": "simple",
                    "value": "aHVudGVyMg",
                    "encoding": "base64",
                },
            },
            "vault": {
                "token": "test",
                "url": "http://vault.example.com:8200/",
            },
        }

        simple = self.store.get_versioned("test")
        self.assertEqual(simple.current, b"easy")
        self.assertEqual(list(simple.all_versions), [b"easy"])

        encoded = self.store.get_versioned("test_base64")
        self.assertEqual(encoded.previous, b"hunter1")
        self.assertEqual(encoded.current, b"hunter2")
        self.assertEqual(encoded.next, b"hunter3")
        self.assertEqual(list(encoded.all_versions),
                         [b"hunter2", b"hunter1", b"hunter3"])

        with self.assertRaises(store.CorruptSecretError):
            self.store.get_versioned("test_unknown_encoding")

        with self.assertRaises(store.CorruptSecretError):
            self.store.get_versioned("test_not_versioned")

        with self.assertRaises(store.CorruptSecretError):
            self.store.get_versioned("test_no_value")

        with self.assertRaises(store.CorruptSecretError):
            self.store.get_versioned("test_bad_base64")


class StoreFromConfigTests(unittest.TestCase):
    def test_make_store(self):
        secrets = store.secrets_store_from_config({
            "secrets.path": "/tmp/test",
        })
        self.assertIsInstance(secrets, store.SecretsStore)
