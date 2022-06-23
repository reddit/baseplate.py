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
    CassandraSessionAdapter,
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
        collectors = list(REGISTRY._collector_to_names.keys())
        for collector in collectors:
            # some system collectors cannot be cleared
            if hasattr(collector, "clear"):
                collector.clear()

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
        self.session.cluster.contact_points = [
            "hostname1",
            "hostname2",
        ]  # mocking cluster contact points
        self.adapter.execute_async("SELECT foo from bar;")

        requests_active = REGISTRY._names_to_collectors[
            "cassandra_client_requests_active"
        ].collect()
        self.assertEquals(len(requests_active), 1)
        self.assertEquals(len(requests_active[0].samples), 1)
        requests_active_s = requests_active[0].samples[0]
        self.assertEquals(requests_active_s.name, "cassandra_client_requests_active")
        self.assertEquals(
            len(requests_active_s.labels.keys()), 3
        )  # contact_points, keyspace, query_name
        self.assertEquals(
            requests_active_s.labels["cassandra_contact_points"], "hostname1,hostname2"
        )
        self.assertEquals(requests_active_s.labels["cassandra_keyspace"], "keyspace")
        self.assertEquals(requests_active_s.labels["cassandra_query_name"], "")
        self.assertEquals(requests_active_s.value, 1)

    def test_execute_async_with_query_name_prom_metrics(self):
        self.session.keyspace = "keyspace"  # mocking keyspace name
        self.session.cluster.contact_points = [
            "hostname1",
            "hostname2",
        ]  # mocking cluster contact points
        self.adapter.execute_async("SELECT foo from bar;", query_name="foo_bar")

        requests_active = REGISTRY._names_to_collectors[
            "cassandra_client_requests_active"
        ].collect()
        self.assertEquals(len(requests_active), 1)
        self.assertEquals(len(requests_active[0].samples), 1)
        requests_active_s = requests_active[0].samples[0]
        self.assertEquals(requests_active_s.name, "cassandra_client_requests_active")
        self.assertEquals(
            len(requests_active_s.labels.keys()), 3
        )  # contact_points, keyspace, query_name
        self.assertEquals(
            requests_active_s.labels["cassandra_contact_points"], "hostname1,hostname2"
        )
        self.assertEquals(requests_active_s.labels["cassandra_keyspace"], "keyspace")
        self.assertEquals(requests_active_s.labels["cassandra_query_name"], "foo_bar")
        self.assertEquals(requests_active_s.value, 1)


class CassandraTests(unittest.TestCase):
    def setUp(self):
        collectors = list(REGISTRY._collector_to_names.keys())
        for collector in collectors:
            # some system collectors cannot be cleared
            if hasattr(collector, "clear"):
                collector.clear()

    def test_prom__on_execute_complete(self):
        result = mock.MagicMock()
        span = mock.MagicMock()
        event = mock.MagicMock()
        start_time = 1.0
        prom_labels = (
            "contact1,contact2",
            "cassandra_keyspace",
            "",
        )

        _on_execute_complete(
            result,
            (
                span,
                start_time,
                prom_labels,
            ),
            event,
        )

        requests_total = REGISTRY._names_to_collectors["cassandra_client_requests_total"].collect()
        self.assertEquals(len(requests_total), 1)
        self.assertEquals(len(requests_total[0].samples), 2)  # _total and _created
        requests_total_s = requests_total[0].samples[0]
        self.assertEquals(requests_total_s.name, "cassandra_client_requests_total")
        self.assertEquals(
            len(requests_total_s.labels.keys()), 4
        )  # contact_points, keyspace, query_name  and success
        self.assertEquals(requests_total_s.labels["cassandra_contact_points"], prom_labels[0])
        self.assertEquals(requests_total_s.labels["cassandra_keyspace"], prom_labels[1])
        self.assertEquals(requests_total_s.labels["cassandra_query_name"], prom_labels[2])
        self.assertEquals(
            requests_total_s.labels["cassandra_success"], "true"
        )  # this is the happy path
        self.assertEquals(requests_total_s.value, 1)

        requests_active = REGISTRY._names_to_collectors[
            "cassandra_client_requests_active"
        ].collect()
        self.assertEquals(len(requests_active), 1)
        self.assertEquals(len(requests_active[0].samples), 1)
        requests_active_s = requests_active[0].samples[0]
        self.assertEquals(requests_active_s.name, "cassandra_client_requests_active")
        self.assertEquals(
            len(requests_active_s.labels.keys()), 3
        )  # contact_points, keyspace, query_name
        self.assertEquals(requests_active_s.labels["cassandra_contact_points"], prom_labels[0])
        self.assertEquals(requests_active_s.labels["cassandra_keyspace"], prom_labels[1])
        self.assertEquals(requests_active_s.labels["cassandra_query_name"], prom_labels[2])
        self.assertEquals(
            requests_active_s.value, -1
        )  # we start from 0 here since this is a unit test, so -1 is the expected result

        requests_latency = REGISTRY._names_to_collectors[
            "cassandra_client_latency_seconds"
        ].collect()
        self.assertEquals(len(requests_latency), 1)
        self.assertEquals(
            len(requests_latency[0].samples), 18
        )  # 15 buckets (including +Inf), _total, _sum and _created
        requests_latency_s = requests_latency[0].samples[14]  # +Inf
        self.assertEquals(requests_latency_s.name, "cassandra_client_latency_seconds_bucket")
        self.assertEquals(
            len(requests_latency_s.labels.keys()), 5
        )  # contact_points, keyspace, query_name and success, +le
        self.assertEquals(requests_latency_s.labels["cassandra_contact_points"], prom_labels[0])
        self.assertEquals(requests_latency_s.labels["cassandra_keyspace"], prom_labels[1])
        self.assertEquals(requests_latency_s.labels["cassandra_query_name"], prom_labels[2])
        self.assertEquals(
            requests_latency_s.labels["cassandra_success"], "true"
        )  # this is the happy path
        self.assertEquals(requests_latency_s.labels["le"], "+Inf")
        self.assertEquals(requests_latency_s.value, 1)

    def test_prom__on_execute_failed(self):
        result = mock.MagicMock()
        span = mock.MagicMock()
        event = mock.MagicMock()
        start_time = 1.0
        prom_labels = (
            "contact1,contact2",
            "cassandra_keyspace",
            "",
        )

        _on_execute_failed(
            result,
            (
                span,
                start_time,
                prom_labels,
            ),
            event,
        )

        requests_total = REGISTRY._names_to_collectors["cassandra_client_requests_total"].collect()
        self.assertEquals(len(requests_total), 1)
        self.assertEquals(len(requests_total[0].samples), 2)  # _total and _created
        requests_total_s = requests_total[0].samples[0]
        self.assertEquals(requests_total_s.name, "cassandra_client_requests_total")
        self.assertEquals(
            len(requests_total_s.labels.keys()), 4
        )  # contact_points, keyspace, query_name and success
        self.assertEquals(requests_total_s.labels["cassandra_contact_points"], prom_labels[0])
        self.assertEquals(requests_total_s.labels["cassandra_keyspace"], prom_labels[1])
        self.assertEquals(requests_total_s.labels["cassandra_query_name"], prom_labels[2])
        self.assertEquals(
            requests_total_s.labels["cassandra_success"], "false"
        )  # this is the failure path
        self.assertEquals(requests_total_s.value, 1)

        requests_active = REGISTRY._names_to_collectors[
            "cassandra_client_requests_active"
        ].collect()
        self.assertEquals(len(requests_active), 1)
        self.assertEquals(len(requests_active[0].samples), 1)
        requests_active_s = requests_active[0].samples[0]
        self.assertEquals(requests_active_s.name, "cassandra_client_requests_active")
        self.assertEquals(
            len(requests_active_s.labels.keys()), 3
        )  # contact_points, keyspace, query_name
        self.assertEquals(requests_active_s.labels["cassandra_contact_points"], prom_labels[0])
        self.assertEquals(requests_active_s.labels["cassandra_keyspace"], prom_labels[1])
        self.assertEquals(requests_active_s.labels["cassandra_query_name"], prom_labels[2])
        self.assertEquals(
            requests_active_s.value, -1
        )  # we start from 0 here since this is a unit test, so -1 is the expected result

        requests_latency = REGISTRY._names_to_collectors[
            "cassandra_client_latency_seconds"
        ].collect()
        self.assertEquals(len(requests_latency), 1)
        self.assertEquals(
            len(requests_latency[0].samples), 18
        )  # 15 buckets (including +Inf), _total, _sum and _created
        requests_latency_s = requests_latency[0].samples[14]  # +Inf
        self.assertEquals(requests_latency_s.name, "cassandra_client_latency_seconds_bucket")
        self.assertEquals(
            len(requests_latency_s.labels.keys()), 5
        )  # contact_points, keyspace, query_name and success, +le
        self.assertEquals(requests_latency_s.labels["cassandra_contact_points"], prom_labels[0])
        self.assertEquals(requests_latency_s.labels["cassandra_keyspace"], prom_labels[1])
        self.assertEquals(requests_latency_s.labels["cassandra_query_name"], prom_labels[2])
        self.assertEquals(
            requests_latency_s.labels["cassandra_success"], "false"
        )  # this is the failure path
        self.assertEquals(requests_latency_s.labels["le"], "+Inf")
        self.assertEquals(requests_latency_s.value, 1)
