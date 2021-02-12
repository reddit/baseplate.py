import unittest

from unittest import mock

try:
    from kazoo.exceptions import NoNodeError
except ImportError:
    raise unittest.SkipTest("kazoo is not installed")

from baseplate.lib.live_data.zookeeper import zookeeper_client_from_config
from baseplate.lib.secrets import SecretsStore

from .. import get_endpoint_or_skip_container

zookeeper_endpoint = get_endpoint_or_skip_container("zookeeper", 2181)


class ZooKeeperTests(unittest.TestCase):
    def test_create_client_no_secrets(self):
        secrets = mock.Mock(spec=SecretsStore)
        client = zookeeper_client_from_config(
            secrets, {"zookeeper.hosts": "%s:%d" % zookeeper_endpoint.address}
        )

        client.start()

        with self.assertRaises(NoNodeError):
            client.get("/does_not_exist")

        client.stop()

    def test_create_client_with_credentials(self):
        secrets = mock.Mock(spec=SecretsStore)
        secrets.get_simple.return_value = b"myzkuser:hunter2"

        client = zookeeper_client_from_config(
            secrets,
            {
                "zookeeper.hosts": "%s:%d" % zookeeper_endpoint.address,
                "zookeeper.credentials": "secret/zk-user",
            },
        )

        client.start()
        with self.assertRaises(NoNodeError):
            client.get("/does_not_exist")
        client.stop()

        secrets.get_simple.assert_called_with("secret/zk-user")
        self.assertEqual(list(client.auth_data), [("digest", "myzkuser:hunter2")])
