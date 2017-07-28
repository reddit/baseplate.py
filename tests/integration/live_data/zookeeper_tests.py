from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    from kazoo.exceptions import NoNodeError
except ImportError:
    raise unittest.SkipTest("kazoo is not installed")

from baseplate.live_data.zookeeper import zookeeper_client_from_config
from baseplate.secrets import SecretsStore

from .. import skip_if_server_unavailable
from ... import mock

skip_if_server_unavailable("zookeeper", 2181)


class ZooKeeperTests(unittest.TestCase):
    def test_create_client_no_secrets(self):
        secrets = mock.Mock(spec=SecretsStore)
        client = zookeeper_client_from_config(secrets, {
            "zookeeper.hosts": "localhost:2181",
        })

        client.start()

        with self.assertRaises(NoNodeError):
            client.get("/does_not_exist")

        client.stop()

    def test_create_client_with_credentials(self):
        secrets = mock.Mock(spec=SecretsStore)
        secrets.get_simple.return_value = b"myzkuser:hunter2"

        client = zookeeper_client_from_config(secrets, {
            "zookeeper.hosts": "localhost:2181",
            "zookeeper.credentials": "secret/zk-user",
        })

        client.start()
        with self.assertRaises(NoNodeError):
            client.get("/does_not_exist")
        client.stop()

        secrets.get_simple.assert_called_with("secret/zk-user")
        self.assertEqual(client.auth_data, [("digest", "myzkuser:hunter2")])
