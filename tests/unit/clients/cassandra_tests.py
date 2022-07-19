import unittest

from unittest import mock

from prometheus_client import REGISTRY

try:
    import cassandra

    del cassandra
except ImportError:
    raise unittest.SkipTest("cassandra-driver is not installed")

import baseplate
import logging
from baseplate.lib.config import ConfigurationError
from baseplate.clients.cassandra import (
    cluster_from_config,
    CassandraCallbackArgs,
    CassandraPrometheusLabels,
    CassandraSessionAdapter,
    REQUEST_TIME,
    REQUEST_ACTIVE,
    REQUEST_TOTAL,
    _on_execute_complete,
    _on_execute_failed,
)
from baseplate.lib.secrets import SecretsStore

logger = logging.getLogger(__name__)


class ClusterFromConfigTests(unittest.TestCase):
    def test_empty_config(self):
        with self.assertRaises(ConfigurationError):
            cluster_from_config({})

    def test_contact_points(self):
        cluster = cluster_from_config({"cassandra.contact_points": "127.0.1.1, 127.0.1.2"})

        self.assertEqual(cluster.contact_points, ["127.0.1.1", "127.0.1.2"])

    def test_port(self):
        cluster = cluster_from_config(
            {"cassandra.contact_points": "127.0.1.1", "cassandra.port": "9999"}
        )

        self.assertEqual(cluster.port, 9999)

    def test_kwarg_passthrough(self):
        cluster = cluster_from_config({"cassandra.contact_points": "127.0.0.1"}, protocol_version=3)

        self.assertEqual(cluster.protocol_version, 3)

    def test_alternate_prefix(self):
        cluster = cluster_from_config(
            {"noodle.contact_points": "127.0.1.1, 127.0.1.2"}, prefix="noodle."
        )

        self.assertEqual(cluster.contact_points, ["127.0.1.1", "127.0.1.2"])

    def test_credentials_but_no_secrets(self):
        with self.assertRaises(TypeError):
            cluster_from_config(
                {
                    "cassandra.contact_points": "127.0.0.1",
                    "cassandra.credentials_secret": "secret/foo/bar",
                }
            )

    def test_credentials(self):
        secrets = mock.Mock(autospec=SecretsStore)
        cluster = cluster_from_config(
            {
                "cassandra.contact_points": "127.0.0.1",
                "cassandra.credentials_secret": "secret/foo/bar",
            },
            secrets=secrets,
        )
        self.assertIsNotNone(cluster.auth_provider)


class CassandraSessionAdapterTests(unittest.TestCase):
    def setUp(self):
        # cleaning up prom registry
        REQUEST_TIME.clear()
        REQUEST_ACTIVE.clear()
        REQUEST_TOTAL.clear()

        self.session = mock.MagicMock()
        self.prepared_statements = {}
        self.mock_server_span = mock.MagicMock(spec=baseplate.ServerSpan)
        self.adapter = CassandraSessionAdapter(
            "test", self.mock_server_span, self.session, self.prepared_statements
        )

    def test_prepare(self):
        statement = "SELECT foo from bar;"
        self.adapter.prepare(statement, cache=True)
        self.adapter.prepare(statement, cache=True)
        # Assert that when preparing an identical statement twice, the CQL
        # session only gets one prepare.
        self.session.prepare.assert_called_once_with(statement)
        self.adapter.prepare(statement, cache=False)
        # Assert that preparing an identical statement with cache=False always
        # prepares.
        calls = [mock.call(statement), mock.call(statement)]
        self.session.prepare.assert_has_calls(calls)

    def test_execute_async_prom_metrics(self):
        self.session.keyspace = "keyspace"  # mocking keyspace name
        self.adapter.execute_async("SELECT foo from bar;")

        self.assertEqual(
            REGISTRY.get_sample_value(
                "cassandra_client_active_requests",
                {
                    "cassandra_client_name": "test",  # client name defaults to name when not provided
                    "cassandra_keyspace": "keyspace",
                    "cassandra_query_name": "",
                },
            ),
            1,
        )

    def test_execute_async_with_query_name_prom_metrics(self):
        self.session.keyspace = "keyspace"  # mocking keyspace name
        self.adapter.execute_async("SELECT foo from bar;", query_name="foo_bar")

        self.assertEqual(
            REGISTRY.get_sample_value(
                "cassandra_client_active_requests",
                {
                    "cassandra_client_name": "test",
                    "cassandra_keyspace": "keyspace",
                    "cassandra_query_name": "foo_bar",
                },
            ),
            1,
        )

    def test_execute_async_prom_metrics_client_name_specified(self):
        self.adapter = CassandraSessionAdapter(
            "test",
            self.mock_server_span,
            self.session,
            self.prepared_statements,
            prometheus_client_name="test_client_name",
        )
        self.session.keyspace = "keyspace"  # mocking keyspace name
        self.adapter.execute_async("SELECT foo from bar;")

        self.assertEqual(
            REGISTRY.get_sample_value(
                "cassandra_client_active_requests",
                {
                    "cassandra_client_name": "test_client_name",
                    "cassandra_keyspace": "keyspace",
                    "cassandra_query_name": "",
                },
            ),
            1,
        )

    def test_execute_async_prom_metrics_client_name_empty(self):
        self.adapter = CassandraSessionAdapter(
            "test",
            self.mock_server_span,
            self.session,
            self.prepared_statements,
            prometheus_client_name="",
        )
        self.session.keyspace = "keyspace"  # mocking keyspace name
        self.adapter.execute_async("SELECT foo from bar;")

        self.assertEqual(
            REGISTRY.get_sample_value(
                "cassandra_client_active_requests",
                {
                    "cassandra_client_name": "",
                    "cassandra_keyspace": "keyspace",
                    "cassandra_query_name": "",
                },
            ),
            1,
        )


class CassandraTests(unittest.TestCase):
    def setUp(self):
        REQUEST_TIME.clear()
        REQUEST_ACTIVE.clear()
        REQUEST_TOTAL.clear()

    def test_prom__on_execute_complete(self):
        result = mock.MagicMock()
        span = mock.MagicMock()
        event = mock.MagicMock()
        start_time = 1.0

        prom_labels_tuple = CassandraPrometheusLabels(
            cassandra_client_name="test_client_name",
            cassandra_keyspace="keyspace",
            cassandra_query_name="",
        )

        _on_execute_complete(
            result,
            CassandraCallbackArgs(
                span=span,
                start_time=start_time,
                prom_labels=prom_labels_tuple,
            ),
            event,
        )
        prom_labels = prom_labels_tuple._asdict()
        prom_labels_w_success = {**prom_labels, **{"cassandra_success": "true"}}

        self.assertEquals(
            REGISTRY.get_sample_value("cassandra_client_requests_total", prom_labels_w_success), 1
        )

        # we start from 0 here since this is a unit test, so -1 is the expected result
        self.assertEquals(
            REGISTRY.get_sample_value("cassandra_client_active_requests", prom_labels), -1
        )

        self.assertEquals(
            REGISTRY.get_sample_value(
                "cassandra_client_latency_seconds_bucket",
                {**prom_labels_w_success, **{"le": "+Inf"}},
            ),
            1,
        )

    def test_prom__on_execute_failed(self):
        result = mock.MagicMock()
        span = mock.MagicMock()
        event = mock.MagicMock()
        start_time = 1.0

        prom_labels_tuple = CassandraPrometheusLabels(
            cassandra_client_name="test_client_name",
            cassandra_keyspace="keyspace",
            cassandra_query_name="",
        )

        _on_execute_failed(
            result,
            CassandraCallbackArgs(
                span=span,
                start_time=start_time,
                prom_labels=prom_labels_tuple,
            ),
            event,
        )
        prom_labels = prom_labels_tuple._asdict()
        prom_labels_w_success = {**prom_labels, **{"cassandra_success": "false"}}

        self.assertEquals(
            REGISTRY.get_sample_value("cassandra_client_requests_total", prom_labels_w_success), 1
        )

        # we start from 0 here since this is a unit test, so -1 is the expected result
        self.assertEquals(
            REGISTRY.get_sample_value("cassandra_client_active_requests", prom_labels), -1
        )

        self.assertEquals(
            REGISTRY.get_sample_value(
                "cassandra_client_latency_seconds_bucket",
                {**prom_labels_w_success, **{"le": "+Inf"}},
            ),
            1,
        )
