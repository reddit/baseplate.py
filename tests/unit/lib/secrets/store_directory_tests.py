import unittest

from baseplate.lib.secrets import CorruptSecretError
from baseplate.lib.secrets import CredentialSecret
from baseplate.lib.secrets import DirectorySecretsStore
from baseplate.lib.secrets import parse_vault_csi
from baseplate.lib.secrets import SecretNotFoundError
from baseplate.lib.secrets import secrets_store_from_config
from baseplate.testing.lib.file_watcher import FakeFileWatcher


class StoreDirectoryTests(unittest.TestCase):
    def setUp(self):
        self.fake_filewatcher_1 = FakeFileWatcher()
        self.fake_filewatcher_2 = FakeFileWatcher()
        self.fake_filewatcher_3 = FakeFileWatcher()
        self.store = DirectorySecretsStore("/whatever", parse_vault_csi)
        self.store._filewatchers["secret1"] = self.fake_filewatcher_1
        self.store._filewatchers["secret2"] = self.fake_filewatcher_2
        self.store._filewatchers["secret3"] = self.fake_filewatcher_3

    def test_file_not_found(self):
        with self.assertRaises(SecretNotFoundError):
            self.store.get_raw("test")

    def test_vault_info(self):
        with self.assertRaises(NotImplementedError):
            self.store.get_vault_token()

        with self.assertRaises(NotImplementedError):
            self.store.get_vault_url()

    def test_raw_secrets(self):
        self.fake_filewatcher_1.data = {
            "data": {"something": "exists"},
        }

        self.assertEqual(self.store.get_raw("secret1"), {"something": "exists"})

        with self.assertRaises(SecretNotFoundError):
            self.store.get_raw("secret0")

    def test_simple_secrets(self):
        # simple test
        self.fake_filewatcher_1.data = {
            "data": {"type": "simple", "value": "easy"},
        }
        self.assertEqual(self.store.get_simple("secret1"), b"easy")

        # test base64
        self.fake_filewatcher_2.data = {
            "data": {"type": "simple", "value": "aHVudGVyMg==", "encoding": "base64"},
        }
        self.assertEqual(self.store.get_simple("secret2"), b"hunter2")

        # test unknown encoding
        self.fake_filewatcher_3.data = {
            "data": {"type": "simple", "value": "sdlfkj", "encoding": "mystery"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_simple("secret3")

        # test not simple
        self.fake_filewatcher_1.data = {
            "data": {"something": "else"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_simple("secret1")

        # test no value
        self.fake_filewatcher_2.data = {
            "data": {"type": "simple"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_simple("secret2")

        # test bad base64
        self.fake_filewatcher_3.data = {
            "data": {"type": "simple", "value": "aHVudGVyMg", "encoding": "base64"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_simple("secret3")

    def test_versioned_secrets(self):
        # simple test
        self.fake_filewatcher_1.data = {
            "data": {"type": "versioned", "current": "easy"},
        }
        simple = self.store.get_versioned("secret1")
        self.assertEqual(simple.current, b"easy")
        self.assertEqual(list(simple.all_versions), [b"easy"])

        # test base64
        self.fake_filewatcher_2.data = {
            "data": {
                "type": "versioned",
                "previous": "aHVudGVyMQ==",
                "current": "aHVudGVyMg==",
                "next": "aHVudGVyMw==",
                "encoding": "base64",
            },
        }
        encoded = self.store.get_versioned("secret2")
        self.assertEqual(encoded.previous, b"hunter1")
        self.assertEqual(encoded.current, b"hunter2")
        self.assertEqual(encoded.next, b"hunter3")
        self.assertEqual(list(encoded.all_versions), [b"hunter2", b"hunter1", b"hunter3"])

        # test unknown encoding
        self.fake_filewatcher_3.data = {
            "data": {"type": "versioned", "current": "sdlfkj", "encoding": "mystery"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_versioned("secret3")

        # test not versioned
        self.fake_filewatcher_1.data = {
            "data": {"something": "else"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_versioned("secret1")

        # test no value
        self.fake_filewatcher_2.data = {
            "data": {"type": "versioned"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_versioned("secret2")

        # test bad base64
        self.fake_filewatcher_3.data = {
            "data": {"type": "simple", "value": "aHVudGVyMg", "encoding": "base64"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_versioned("secret3")

    def test_credential_secrets(self):
        # simple test
        self.fake_filewatcher_1.data = {
            "data": {"type": "credential", "username": "user", "password": "password"},
        }
        self.assertEqual(
            self.store.get_credentials("secret1"), CredentialSecret("user", "password")
        )

        # test identiy
        self.fake_filewatcher_2.data = {
            "data": {
                "type": "credential",
                "username": "spez",
                "password": "hunter2",
                "encoding": "identity",
            },
        }
        self.assertEqual(self.store.get_credentials("secret2"), CredentialSecret("spez", "hunter2"))

        # test base64
        self.fake_filewatcher_2.data = {
            "data": {
                "type": "credential",
                "username": "foo",
                "password": "aHVudGVyMg==",
                "encoding": "base64",
            },
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("secret2")

        # test unknkown encoding
        self.fake_filewatcher_3.data = {
            "data": {
                "type": "credential",
                "username": "fizz",
                "password": "buzz",
                "encoding": "something",
            },
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("secret3")

        # test not credentials
        self.fake_filewatcher_1.data = {
            "data": {"type": "versioned", "current": "easy"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("secret1")

        # test no values
        self.fake_filewatcher_2.data = {
            "data": {"type": "credential"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("secret2")

        # test no username
        self.fake_filewatcher_3.data = {
            "data": {"type": "credential", "password": "password"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("secret3")

        # test no password
        self.fake_filewatcher_1.data = {
            "data": {"type": "credential", "username": "user"},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("secret1")

        # test bad type
        self.fake_filewatcher_2.data = {
            "data": {"type": "credential", "username": "user", "password": 100},
        }
        with self.assertRaises(CorruptSecretError):
            self.store.get_credentials("secret2")


class StoreFromConfigTests(unittest.TestCase):
    def test_make_store(self):
        secrets = secrets_store_from_config(
            {"secrets.path": "/tmp", "secrets.provider": "vault_csi"}
        )
        self.assertIsInstance(secrets, DirectorySecretsStore)
