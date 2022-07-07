import unittest

from unittest import mock

try:
    from sqlalchemy.engine.url import URL
except ImportError:
    raise unittest.SkipTest("sqlalchemy is not installed")

from baseplate.clients.sqlalchemy import engine_from_config
from baseplate.clients.sqlalchemy import SQLAlchemyEngineContextFactory
from baseplate.testing.lib.secrets import FakeSecretsStore

from prometheus_client import REGISTRY
from sqlalchemy.pool import QueuePool


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


class EngineContextFactoryTest(unittest.TestCase):
    @mock.patch("baseplate.clients.sqlalchemy.event")  # I'm just ignoring all the events
    def setUp(self, event):
        SQLAlchemyEngineContextFactory.max_connections_gauge.clear()
        SQLAlchemyEngineContextFactory.checked_out_connections_gauge.clear()
        SQLAlchemyEngineContextFactory.latency_seconds.clear()
        SQLAlchemyEngineContextFactory.requests_total.clear()

        engine = mock.MagicMock()
        # engine.execution_options.return_value = mock.MagicMock()
        self.factory = SQLAlchemyEngineContextFactory(engine, "factory_name")

    def test_report_runtime_metrics_prom_no_queue_pool(self):
        batch = mock.MagicMock()

        pool = mock.MagicMock()  # this will fail the isinstance check
        pool.size.return_value = 4
        pool.checkedout.return_value = 12
        self.factory.engine.pool = pool

        self.factory.report_runtime_metrics(batch)

        prom_labels = {"pool": "factory_name"}
        # this serves to prove that we never set these metrics / go down the code path after the isinstance check
        self.assertEqual(
            REGISTRY.get_sample_value(self.factory.max_connections_gauge._name, prom_labels), None
        )
        self.assertEqual(
            REGISTRY.get_sample_value(
                self.factory.checked_out_connections_gauge._name, prom_labels
            ),
            None,
        )

    def test_report_runtime_metrics_prom_with_queue_pool(self):
        batch = mock.MagicMock()

        pool = mock.MagicMock(spec=QueuePool)  # this will pass the isinstance check
        pool.size.return_value = 4
        pool.checkedout.return_value = 12
        pool.overflow.return_value = 16
        self.factory.engine.pool = pool

        self.factory.report_runtime_metrics(batch)

        prom_labels = {"pool": "factory_name"}
        self.assertEqual(
            REGISTRY.get_sample_value(self.factory.max_connections_gauge._name, prom_labels), 4
        )
        self.assertEqual(
            REGISTRY.get_sample_value(
                self.factory.checked_out_connections_gauge._name, prom_labels
            ),
            12,
        )

    def test_on_after_execute(self):
        conn = mock.MagicMock()
        conn.engine.url.host = "test_hostname"
        conn.engine.url.database = "test_database"
        self.factory.on_after_execute(
            conn=conn,
            cursor=None,
            statement="",
            parameters=None,
            context=None,
            executemany=False,
        )

        prom_labels = {
            "sql_address": "test_hostname",
            "sql_database": "test_database",
            "sql_success": "true",
        }

        self.assertEqual(
            REGISTRY.get_sample_value(f"{self.factory.requests_total._name}_total", prom_labels), 1
        )
        self.assertEqual(
            REGISTRY.get_sample_value(
                f"{self.factory.latency_seconds._name}_bucket", {**prom_labels, "le": "+Inf"}
            ),
            1,
        )

    def test_on_error(self):
        exception_context = mock.MagicMock()
        exception_context.connection.engine.url.host = "test_hostname"
        exception_context.connection.engine.url.database = "test_database"
        self.factory.on_error(exception_context)

        prom_labels = {
            "sql_address": "test_hostname",
            "sql_database": "test_database",
            "sql_success": "false",
        }

        self.assertEqual(
            REGISTRY.get_sample_value(f"{self.factory.requests_total._name}_total", prom_labels), 1
        )
        self.assertEqual(
            REGISTRY.get_sample_value(
                f"{self.factory.latency_seconds._name}_bucket", {**prom_labels, "le": "+Inf"}
            ),
            1,
        )
