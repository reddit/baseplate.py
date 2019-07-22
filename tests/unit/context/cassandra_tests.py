import unittest

from unittest import mock

try:
    import cassandra

    del cassandra
except ImportError:
    raise unittest.SkipTest("cassandra-driver is not installed")

import baseplate
from baseplate.lib.config import ConfigurationError
from baseplate.clients.cassandra import cluster_from_config, CassandraSessionAdapter
from baseplate.lib.secrets import SecretsStore


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
