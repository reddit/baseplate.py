from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    import cassandra
except ImportError:
    raise unittest.SkipTest("cassandra-driver is not installed")
except:
    del cassandra

from baseplate.config import ConfigurationError
from baseplate.context.cassandra import CQLMetadataExtractor, cluster_from_config


class ClusterFromConfigTests(unittest.TestCase):
    def test_empty_config(self):
        with self.assertRaises(ConfigurationError):
            cluster_from_config({})

    def test_contact_points(self):
        cluster = cluster_from_config({
            "cassandra.contact_points": "127.0.1.1, 127.0.1.2",
        })

        self.assertEqual(cluster.contact_points, ["127.0.1.1", "127.0.1.2"])

    def test_port(self):
        cluster = cluster_from_config({
            "cassandra.contact_points": "127.0.1.1",
            "cassandra.port": "9999",
        })

        self.assertEqual(cluster.port, 9999)

    def test_kwarg_passthrough(self):
        cluster = cluster_from_config({
            "cassandra.contact_points": "127.0.0.1",
        }, protocol_version=3)

        self.assertEqual(cluster.protocol_version, 3)

    def test_alternate_prefix(self):
        cluster = cluster_from_config({
            "noodle.contact_points": "127.0.1.1, 127.0.1.2",
        }, prefix="noodle.")

        self.assertEqual(cluster.contact_points, ["127.0.1.1", "127.0.1.2"])


class CqlMetadataExtractorTests(unittest.TestCase):
    def test_select_statements(self):
        statements = (
            "SELECT name, occupation FROM users WHERE userid IN (200, 207)",
            "select JSON name, occupation FROM users WHERE userid = 199;",
            "SELECt name AS user_name FROM users;"
        )
        expected_metadata = {
            "type": "select",
            "table": "users"
        }
        for statement in statements:
            self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))

    def test_select_statements_with_keyspace(self):
        statements = (
            "SELECT name, occupation FROM system.users WHERE userid IN (200, 207)",
            "select JSON name, occupation FROM \"system\".users WHERE userid = 199;",
            "SELECt name AS user_name FROM system.users;"
        )
        expected_metadata = {
            "keyspace": "system",
            "type": "select",
            "table": "users"
        }
        for statement in statements:
            self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))

    def test_insert_statements(self):
        statements = (
            "INSERT INTO NerdMovies JSON '{\"movie\": \"Serenity\", \"year\": 2005}';",
            "INSERT INTO NerdMovies (movie, year) VALUES ('Serenity', 2005) USING TTL 86400;"
        )
        expected_metadata = {
            "type": "insert",
            "table": "NerdMovies"
        }
        for statement in statements:
            self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))

    def test_insert_statements_with_keyspace(self):
        statements = (
            "INSERT INTO \"system\".NerdMovies JSON '{\"movie\": \"Serenity\", \"year\": 2005}';",
            "INSERT INTO system.NerdMovies (movie, year) VALUES ('Serenity', 2005) USING TTL 86400;"
        )
        expected_metadata = {
            "keyspace": "system",
            "type": "insert",
            "table": "NerdMovies"
        }
        for statement in statements:
            self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))

    def test_update_statements(self):
        statements = (
            "UPDATE NerdMovies USING TTL 400 SET year = 2005 WHERE movie = 'Serenity';",
            "UPDATE NerdMovies SET total = total + 2 WHERE user = B70DE1D0-9908-4AE3-BE34-5573E5B09F14;"
        )
        expected_metadata = {
            "type": "update",
            "table": "NerdMovies"
        }
        for statement in statements:
            self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))

    def test_update_statements_with_keyspace(self):
        statements = (
            "UPDATE system.NerdMovies USING TTL 400 SET year = 2005 WHERE movie = 'Serenity';",
            "UPDATE \"system\".NerdMovies SET total = total + 2 WHERE user = 123;"
        )
        expected_metadata = {
            "keyspace": "system",
            "type": "update",
            "table": "NerdMovies"
        }
        for statement in statements:
            self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))

    def test_delete_statements(self):
        statements = (
            "DELETE FROM NerdMovies USING TIMESTAMP 1240003134 WHERE movie = 'Serenity';",
            "DELETE phone FROM NerdMovies WHERE userid IN (B70DE1D0-9908-4AE3-BE34-5573E5B09F14);"
        )
        expected_metadata = {
            "type": "delete",
            "table": "NerdMovies"
        }
        for statement in statements:
            self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))

    def test_delete_statements_with_keyspace(self):
        statements = (
            "DELETE FROM system.NerdMovies USING TIMESTAMP 1240003134 WHERE movie = 'Serenity';",
            "DELETE phone FROM \"system\".NerdMovies WHERE userid IN (123);"
        )
        expected_metadata = {
            "keyspace": "system",
            "type": "delete",
            "table": "NerdMovies"
        }
        for statement in statements:
            self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))

    def test_create_statements(self):
        statement = "CREATE TABLE t (k text PRIMARY KEY);"
        expected_metadata = {
            "type": "create"
        }
        self.assertDictEqual(expected_metadata, CQLMetadataExtractor.extract(statement))
