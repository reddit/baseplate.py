from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    from sqlalchemy.engine.url import URL
except ImportError:
    raise unittest.SkipTest("sqlalchemy is not installed")

from baseplate.config import ConfigurationError
from baseplate.context.sqlalchemy import engine_from_config
from baseplate.file_watcher import FileWatcher
from baseplate.secrets.store import SecretsStore

from ... import mock

class EngineFromConfigTests(unittest.TestCase):
    def setUp(self):
        mock_filewatcher = mock.Mock(spec=FileWatcher)
        mock_filewatcher.get_data.return_value = {
            "secrets": {
                "secret/sql/account": {
                    "type": "credential",
                    "username": "reddit",
                    "password": "password",
                },
            },
            "vault": {
                "token": "test",
                "url": "http://vault.example.com:8200/",
            }
        }
        secrets = SecretsStore("/secrets")
        secrets._filewatcher = mock_filewatcher
        self.secrets = secrets

    def test_url(self):
        engine = engine_from_config({"database.url": "sqlite://"})
        self.assertEqual(engine.url, URL("sqlite"))

    @mock.patch('baseplate.context.sqlalchemy.create_engine')
    def test_credentials(self, create_engine_mock):
        engine = engine_from_config({
            "database.url": "postgresql://fizz:buzz@localhost:9000/db",
            "database.credentials_secret": "secret/sql/account",
        }, self.secrets)
        create_engine_mock.assert_called_once_with(URL(
            drivername="postgresql",
            username="reddit",
            password="password",
            host="localhost",
            port="9000",
            database="db",
        ))

    @mock.patch('baseplate.context.sqlalchemy.create_engine')
    def test_credentials_no_secrets(self, create_engine_mock):
        with self.assertRaises(TypeError):
            engine_from_config({
                "database.url": "postgresql://fizz:buzz@localhost:9000/db",
                "database.credentials_secret": "secret/sql/account",
            })
        self.assertEqual(create_engine_mock.call_count, 0)
