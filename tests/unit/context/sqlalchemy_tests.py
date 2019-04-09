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
                "secret/sql/password": {
                    "type": "simple",
                    "value": "password",
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

    def test_drivername_only(self):
        engine = engine_from_config({"database.drivername": "sqlite"}, self.secrets)
        self.assertEqual(engine.url, URL("sqlite"))

    def test_url(self):
        engine = engine_from_config({"database.url": "sqlite://"}, self.secrets)
        self.assertEqual(engine.url, URL("sqlite"))

    @mock.patch('baseplate.context.sqlalchemy.create_engine')
    def test_drivername_missing(self, create_engine_mock):
        with self.assertRaises(ConfigurationError):
            engine_from_config({"database.username": "reddit"}, self.secrets)
        self.assertEqual(create_engine_mock.call_count, 0)

    @mock.patch('baseplate.context.sqlalchemy.create_engine')
    def test_no_query(self, create_engine_mock):
        app_config = {
            "database.drivername": "postgresql",
            "database.username": "reddit",
            "database.password_secret": "secret/sql/password",
            "database.host": "database.local",
            "database.port": "8000",
            "database.database": "test",
        }
        engine_from_config(app_config, self.secrets)
        create_engine_mock.assert_called_once_with(URL(
            drivername="postgresql",
            username="reddit",
            password="password",
            host="database.local",
            port=8000,
            database="test",
        ))

    @mock.patch('baseplate.context.sqlalchemy.create_engine')
    def test_query(self, create_engine_mock):
        app_config = {
            "database.drivername": "postgresql",
            "database.username": "reddit",
            "database.password_secret": "secret/sql/password",
            "database.host": "database.local",
            "database.port": "8000",
            "database.database": "test",
            "database.query.foo": "bar",
            "database.query.hello": "world",
        }
        engine_from_config(app_config, self.secrets)
        create_engine_mock.assert_called_once_with(URL(
            drivername="postgresql",
            username="reddit",
            password="password",
            host="database.local",
            port=8000,
            database="test",
            query={"foo": "bar", "hello": "world"},
        ))

    @mock.patch('baseplate.context.sqlalchemy.create_engine')
    def test_kwarg_override(self, create_engine_mock):
        app_config = {
            "database.drivername": "postgresql",
            "database.username": "reddit",
            "database.password_secret": "secret/sql/password",
            "database.host": "database.local",
            "database.port": "8000",
            "database.database": "test",
            "database.query.foo": "bar",
            "database.query.hello": "world",
        }
        engine_from_config(
            app_config,
            self.secrets,
            drivername="mysql",
            username="foo",
            password="bar",
            host="mydb.local",
            port=9000,
            database="db",
            query={"fizz": "buzz"},
        )
        create_engine_mock.assert_called_once_with(URL(
            drivername="mysql",
            username="foo",
            password="bar",
            host="mydb.local",
            port=9000,
            database="db",
            query={"fizz": "buzz"},
        ))

    @mock.patch('baseplate.context.sqlalchemy.create_engine')
    def test_unsupported_kwarg_override(self, create_engine_mock):
        app_config = {
            "database.drivername": "postgresql",
            "database.username": "reddit",
            "database.password_secret": "secret/sql/password",
            "database.host": "database.local",
            "database.port": "8000",
            "database.database": "test",
            "database.query.foo": "bar",
            "database.query.hello": "world",
        }
        with self.assertRaises(TypeError):
            engine_from_config(
                app_config,
                self.secrets,
                drivername="mysql",
                username="foo",
                password="bar",
                host="mydb.local",
                port=9000,
                database="db",
                query={"fizz": "buzz"},
                unsupported="oops",
            )
        self.assertEqual(create_engine_mock.call_count, 0)
