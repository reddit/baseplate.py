from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import tempfile
import unittest

from baseplate.secrets import store

from ... import mock


class StoreTests(unittest.TestCase):
    def setUp(self):
        real_datetime = datetime.datetime
        datetime_patcher = mock.patch("datetime.datetime", autospec=True)
        self.addCleanup(datetime_patcher.stop)
        self.mock_datetime_cls = datetime_patcher.start()
        self.mock_datetime_cls.side_effect = lambda *a, **kw: real_datetime(*a, **kw)
        self.mock_datetime_cls.min = real_datetime.min
        self.mock_datetime_cls.strptime = real_datetime.strptime
        self.mock_datetime_cls.utcnow.return_value = datetime.datetime(2017, 5, 5, 9, 35)

        self.tempfile = tempfile.NamedTemporaryFile()

    def _fill_secrets_file(self, data):
        self.tempfile.seek(0)
        self.tempfile.write(data.encode("utf8"))
        self.tempfile.flush()

    def test_initial_fetch_loads_secrets(self):
        self._fill_secrets_file("""{
            "secrets": {
                "test": {"a": 1}
            },
            "vault_token": "test"
        }""")

        secrets = store.SecretsStore(self.tempfile.name)
        secret = secrets.get_raw("test")

        self.assertEqual(secret, {"a": 1})
        self.assertEqual(secrets.get_vault_token(), "test")

        with self.assertRaises(store.SecretNotFoundError):
            secrets.get_raw("does_not_exist")

    @mock.patch("os.fstat")
    @mock.patch("os.path.getmtime")
    def test_dont_reload_while_not_changed(self, getmtime, fstat):
        getmtime.return_value = 1
        fstat.return_value = mock.Mock(st_mtime=1)
        self._fill_secrets_file("""{
            "secrets": {
                "test": {"a": 1}
            },
            "vault_token": "test1"
        }""")

        secrets = store.SecretsStore(self.tempfile.name)
        self.assertEqual(secrets.get_raw("test"), {"a": 1})

        # this will not parse, so we know we didn't reload if we don't crash!
        self._fill_secrets_file("!")
        self.assertEqual(secrets.get_raw("test"), {"a": 1})

    # we patch the mtime stuff because it has whole-second granularity and it's
    # best not to make the tests take longer just to ensure we tick over.
    @mock.patch("os.fstat")
    @mock.patch("os.path.getmtime")
    def test_reload_when_mtime_changes(self, getmtime, fstat):
        getmtime.return_value = 1
        fstat.return_value = mock.Mock(st_mtime=1)
        self._fill_secrets_file("""{
            "secrets": {
                "test": {"a": 1}
            },
            "vault_token": "test1"
        }""")

        secrets = store.SecretsStore(self.tempfile.name)
        self.assertEqual(secrets.get_raw("test"), {"a": 1})

        getmtime.return_value = 2
        fstat.return_value = mock.Mock(st_mtime=2)
        self._fill_secrets_file("""{
            "secrets": {
                "test": {"a": 2}
            },
            "vault_token": "test1"
        }""")
        self.assertEqual(secrets.get_raw("test"), {"a": 2})

    def test_simple_secrets(self):
        self._fill_secrets_file("""{
            "secrets": {
                "test": {
                    "type": "simple",
                    "value": "easy"
                },
                "test_base64": {
                    "type": "simple",
                    "value": "aHVudGVyMg==",
                    "encoding": "base64"
                },
                "test_unknown_encoding": {
                    "type": "simple",
                    "value": "sdlfkj",
                    "encoding": "mystery"
                },
                "test_not_simple": {
                    "something": "else"
                },
                "test_no_value": {
                    "type": "simple"
                },
                "test_bad_base64": {
                    "type": "simple",
                    "value": "aHVudGVyMg",
                    "encoding": "base64"
                }
            },
            "vault_token": "test1"
        }""")

        secrets = store.SecretsStore(self.tempfile.name)

        self.assertEqual(secrets.get_simple("test"), b"easy")
        self.assertEqual(secrets.get_simple("test_base64"), b"hunter2")

        with self.assertRaises(store.CorruptSecretError):
            secrets.get_simple("test_unknown_encoding")

        with self.assertRaises(store.CorruptSecretError):
            secrets.get_simple("test_not_simple")

        with self.assertRaises(store.CorruptSecretError):
            secrets.get_simple("test_no_value")

        with self.assertRaises(store.CorruptSecretError):
            secrets.get_simple("test_bad_base64")

    def test_versioned_secrets(self):
        self._fill_secrets_file("""{
            "secrets": {
                "test": {
                    "type": "versioned",
                    "current": "easy"
                },
                "test_base64": {
                    "type": "versioned",
                    "previous": "aHVudGVyMQ==",
                    "current": "aHVudGVyMg==",
                    "next": "aHVudGVyMw==",
                    "encoding": "base64"
                },
                "test_unknown_encoding": {
                    "type": "versioned",
                    "current": "sdlfkj",
                    "encoding": "mystery"
                },
                "test_not_versioned": {
                    "something": "else"
                },
                "test_no_value": {
                    "type": "versioned"
                },
                "test_bad_base64": {
                    "type": "simple",
                    "value": "aHVudGVyMg",
                    "encoding": "base64"
                }
            },
            "vault_token": "test1"
        }""")

        secrets = store.SecretsStore(self.tempfile.name)

        simple = secrets.get_versioned("test")
        self.assertEqual(simple.current, b"easy")
        self.assertEqual(list(simple.all_versions), [b"easy"])

        encoded = secrets.get_versioned("test_base64")
        self.assertEqual(encoded.previous, b"hunter1")
        self.assertEqual(encoded.current, b"hunter2")
        self.assertEqual(encoded.next, b"hunter3")
        self.assertEqual(list(encoded.all_versions),
                         [b"hunter2", b"hunter1", b"hunter3"])

        with self.assertRaises(store.CorruptSecretError):
            secrets.get_versioned("test_unknown_encoding")

        with self.assertRaises(store.CorruptSecretError):
            secrets.get_versioned("test_not_versioned")

        with self.assertRaises(store.CorruptSecretError):
            secrets.get_versioned("test_no_value")

        with self.assertRaises(store.CorruptSecretError):
            secrets.get_versioned("test_bad_base64")


class StoreFromConfigTests(unittest.TestCase):
    def test_make_store(self):
        secrets = store.secrets_store_from_config({
            "secrets.path": "/tmp/test",
        })
        self.assertIsInstance(secrets, store.SecretsStore)
