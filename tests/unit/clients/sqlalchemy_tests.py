import unittest

from unittest import mock

try:
    from sqlalchemy.engine.url import URL
except ImportError:
    raise unittest.SkipTest("sqlalchemy is not installed")

from baseplate.clients.sqlalchemy import engine_from_config
from baseplate.testing.lib.secrets import FakeSecretsStore


class EngineFromConfigTests(unittest.TestCase):
    def setUp(self):
        secrets = FakeSecretsStore(
            {
                "secrets": {
                    "secret/sql/account": {
                        "type": "credential",
                        "username": "reddit",
                        "password": "password",
                    }
                }
            }
        )
        self.secrets = secrets

    def test_url(self):
        engine = engine_from_config({"database.url": "sqlite://"})
        self.assertEqual(engine.url, URL("sqlite"))

    @mock.patch("baseplate.clients.sqlalchemy.create_engine")
    def test_credentials(self, create_engine_mock):
        engine_from_config(
            {
                "database.url": "postgresql://fizz:buzz@localhost:9000/db",
                "database.credentials_secret": "secret/sql/account",
                "database.pool_recycle": "60",
                "database.pool_size": "10",
                "database.max_overflow": "5",
            },
            self.secrets,
        )
        create_engine_mock.assert_called_once_with(
            URL(
                drivername="postgresql",
                username="reddit",
                password="password",
                host="localhost",
                port="9000",
                database="db",
            ),
            pool_recycle=60,
            pool_size=10,
            max_overflow=5,
        )

    @mock.patch("baseplate.clients.sqlalchemy.create_engine")
    def test_credentials_no_secrets(self, create_engine_mock):
        with self.assertRaises(TypeError):
            engine_from_config(
                {
                    "database.url": "postgresql://fizz:buzz@localhost:9000/db",
                    "database.credentials_secret": "secret/sql/account",
                }
            )
        self.assertEqual(create_engine_mock.call_count, 0)
