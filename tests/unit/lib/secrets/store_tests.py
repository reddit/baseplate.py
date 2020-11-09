import unittest

from baseplate.lib.secrets import CorruptSecretError
from baseplate.lib.secrets import CredentialSecret
from baseplate.lib.secrets import SecretNotFoundError
from baseplate.lib.secrets import secrets_store_from_config
from baseplate.lib.secrets import SecretsNotAvailableError
from baseplate.lib.secrets import SecretsStore
from baseplate.testing.lib.file_watcher import FakeFileWatcher


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.fake_filewatcher = FakeFileWatcher()
        self.store = SecretsStore("/whatever")
        self.store._filewatcher = self.fake_filewatcher

    def test_file_not_found(self):
        with self.assertRaises(SecretsNotAvailableError):
            self.store.get_raw("test")

    def test_vault_info(self):
        self.fake_filewatcher.data = {
            "secrets": {},
            "vault": {"token": "test", "url": "http://vault.example.com:8200/"},
        }

        self.assertEqual(self.store.get_vault_token(), "test")
        self.assertEqual(self.store.get_vault_url(), "http://vault.example.com:8200/")

    def test_raw_secrets(self):
        self.fake_filewatcher.data = {
            "secrets": {"test": {"something": "exists"}},
            "vault": {"token": "test", "url": "http://vault.example.com:8200/"},
        }

        self.assertEqual(self.store.get_raw("test"), {"something": "exists"})

        with self.assertRaises(SecretNotFoundError):
            self.store.get_raw("test_missing")

    def test_simple_secrets(self):
        self.fake_filewatcher.data = {
            "secrets": {
                "test": {"type": "simple", "value": "easy"},
                "test_base64": {"type": "simple", "value": "aHVudGVyMg==", "encoding": "base64"},
                "test_unknown_encoding": {
                    "type": "simple",
                    "value": "sdlfkj",
                    "encoding": "mystery",
                },
                "test_not_simple": {"something": "else"},
                "test_no_value": {"type": "simple"},
                "test_bad_base64": {"type": "simple", "value": "aHVudGVyMg", "encoding": "base64"},
            },
            "vault": {"token": "test", "url": "http://vault.example.com:8200/"},
        }

        self.assertEqual(self.store.get_simple("test"), b"easy")
        self.assertEqual(self.store.get_simple("test_base64"), b"hunter2")

        with self.assertRaises(CorruptSecretError):
            self.store.get_simple("test_unknown_encoding")

        with self.assertRaises(CorruptSecretError):
            self.store.get_simple("test_not_simple")

        with self.assertRaises(CorruptSecretError):
            self.store.get_simple("test_no_value")

        with self.assertRaises(CorruptSecretError):
            self.store.get_simple("test_bad_base64")

    def test_versioned_secrets(self):
        self.fake_filewatcher.data = {
            "secrets": {
                "test": {"type": "versioned", "current": "easy"},
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
                "test_not_versioned": {"something": "else"},
                "test_no_value": {"type": "versioned"},
                "test_bad_base64": {"type": "simple", "value": "aHVudGVyMg", "encoding": "base64"},
            },
            "vault": {"token": "test", "url": "http://vault.example.com:8200/"},
        }

        simple = self.store.get_versioned("test")
        self.assertEqual(simple.current, b"easy")
        self.assertEqual(list(simple.all_versions), [b"easy"])

        encoded = self.store.get_versioned("test_base64")
        self.assertEqual(encoded.previous, b"hunter1")
        self.assertEqual(encoded.current, b"hunter2")
        self.assertEqual(encoded.next, b"hunter3")
        self.assertEqual(list(encoded.all_versions), [b"hunter2", b"hunter1", b"hunter3"])

        with self.assertRaises(CorruptSecretError):
            self.store.get_versioned("test_unknown_encoding")

        with self.assertRaises(CorruptSecretError):
            self.store.get_versioned("test_not_versioned")

        with self.assertRaises(CorruptSecretError):
            self.store.get_versioned("test_no_value")

        with self.assertRaises(CorruptSecretError):
            self.store.get_versioned("test_bad_base64")

    def test_credential_secrets(self):
        self.fake_filewatcher.data = {
            "secrets": {
                "test": {"type": "credential", "username": "user", "password": "password"},
                "test_identity": {
                    "type": "credential",
                    "username": "spez",
                    "password": "hunter2",
                    "encoding": "identity",
                },
                "test_base64": {
                    "type": "credential",
                    "username": "foo",
                    "password": "aHVudGVyMg==",
                    "encoding": "base64",
                },
                "test_unknown_encoding": {
                    "type": "credential",
                    "username": "fizz",
                    "password": "buzz",
                    "encoding": "something",
                },
                "test_not_credentials": {"type": "versioned", "current": "easy"},
                "test_no_values": {"type": "credential"},
                "test_no_username": {"type": "credential", "password": "password"},
                "test_no_password": {"type": "credential", "username": "user"},
                "test_bad_type": {"type": "credential", "username": "user", "password": 100},
            },
            "vault": {"token": "test", "url": "http://vault.example.com:8200/"},
        }

        self.assertEqual(self.store.get_credentials("test"), CredentialSecret("user", "password"))
        self.assertEqual(
            self.store.get_credentials("test_identity"), CredentialSecret("spez", "hunter2")
        )

        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("test_base64")

        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("test_unknown_encoding")

        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("test_not_credentials")

        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("test_no_values")

        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("test_no_username")

        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("test_no_password")


class StoreFromConfigTests(unittest.TestCase):
    def test_make_store(self):
        secrets = secrets_store_from_config({"secrets.path": "/tmp/test"})
        self.assertIsInstance(secrets, SecretsStore)

    def test_prefix(self):
        secrets = secrets_store_from_config(
            {"secrets.path": "/tmp/test", "test_secrets.path": "/tmp/secrets"},
            prefix="test_secrets.",
        )
        self.assertIsInstance(secrets, SecretsStore)
        self.assertEqual(secrets._filewatcher._path, "/tmp/secrets")
