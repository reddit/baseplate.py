import time
import unittest

from unittest import mock

import gevent.socket

try:
    from kazoo.exceptions import NoNodeError
    from kazoo.handlers.gevent import SequentialGeventHandler
    from kazoo.handlers.threading import SequentialThreadingHandler
    from kazoo.retry import KazooRetry
except ImportError:
    raise unittest.SkipTest("kazoo is not installed")

from baseplate.lib.live_data.zookeeper import zookeeper_client_from_config
from baseplate.lib.secrets import SecretsStore

from .. import get_endpoint_or_skip_container

zookeeper_endpoint = get_endpoint_or_skip_container("zookeeper", 2181)


class ZooKeeperHandlerWithPatchingTests(unittest.TestCase):
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

    def test_create_client_uses_threading_handler_when_not_gevent_patched(self):
        secrets = mock.Mock(spec=SecretsStore)
        client = zookeeper_client_from_config(
            secrets, {"zookeeper.hosts": "%s:%d" % zookeeper_endpoint.address}
        )
        assert isinstance(client.handler, SequentialThreadingHandler)

    @mock.patch("baseplate.lib.live_data.zookeeper.gevent_is_patched", return_value=True)
    def test_create_client_uses_gevent_handler_when_gevent_patched(self, mock_gevent_is_patched):
        patched_default_values = tuple(
            gevent.sleep if value is time.sleep else value
            for value in KazooRetry.__init__.__defaults__
        )
        with mock.patch("kazoo.retry.KazooRetry.__init__.__defaults__", patched_default_values):
            secrets = mock.Mock(spec=SecretsStore)
            client = zookeeper_client_from_config(
                secrets, {"zookeeper.hosts": "%s:%d" % zookeeper_endpoint.address}
            )
            assert isinstance(client.handler, SequentialGeventHandler)
