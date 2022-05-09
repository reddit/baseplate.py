import importlib
import unittest

from unittest import mock

import gevent.socket

try:
    from kazoo.exceptions import NoNodeError
    from kazoo.handlers.gevent import SequentialGeventHandler
    from kazoo.handlers.threading import SequentialThreadingHandler
except ImportError:
    raise unittest.SkipTest("kazoo is not installed")

from baseplate.lib.live_data.zookeeper import zookeeper_client_from_config
from baseplate.lib.secrets import SecretsStore

from .. import get_endpoint_or_skip_container

zookeeper_endpoint = get_endpoint_or_skip_container("zookeeper", 2181)


class ZooKeeperHandlerWithPatchingTests(unittest.TestCase):
    def tearDown(self):
        # Cleanup gevent patching
        import socket

        importlib.reload(socket)
        gevent.monkey.saved.clear()

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

    @mock.patch(
        "kazoo.retry.KazooRetry.__init__.__defaults__",
        (1, 0.1, 2, 0.4, 60.0, True, gevent.sleep, None, None),
    )
    def test_create_client_uses_gevent_handler_when_gevent_patched(self):
        # We patch `socket` just to make sure that the gevent handler is chosen,
        # nothing is special about `socket` in particular. We don't just use
        # `gevent.patch_all()` because we can't unpatch builtin functions such
        # as time.sleep with `importlib.reload`.
        import socket

        importlib.reload(socket)
        gevent.monkey.patch_socket()

        secrets = mock.Mock(spec=SecretsStore)
        client = zookeeper_client_from_config(
            secrets, {"zookeeper.hosts": "%s:%d" % zookeeper_endpoint.address}
        )
        assert isinstance(client.handler, SequentialGeventHandler)
