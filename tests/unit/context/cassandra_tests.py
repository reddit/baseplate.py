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
from baseplate.context.cassandra import cluster_from_config


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
