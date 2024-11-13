import unittest

from io import StringIO
from unittest import mock

from baseplate.lib import service_discovery
from baseplate.lib.file_watcher import FileWatcher
from baseplate.lib.file_watcher import WatchedFileNotAvailableError


TEST_INVENTORY_ONE = """\
[
    {
        "haproxy_server_options": null,
        "host": "10.0.1.2",
        "id": 205,
        "labels": null,
        "name": "i-258fc8b6",
        "port": 9090,
        "weight": null
    }
]
"""

TEST_INVENTORY_TWO = """\
[
    {
        "haproxy_server_options": null,
        "host": "10.0.1.2",
        "id": 205,
        "labels": null,
        "name": "i-258fc8b6",
        "port": 9090,
        "weight": 1
    },
    {
        "haproxy_server_options": null,
        "host": "10.0.1.3",
        "id": 215,
        "labels": null,
        "name": "i-650b6a6b",
        "port": 9090,
        "weight": 2
    },
    {
        "haproxy_server_options": null,
        "host": "10.0.1.4",
        "id": 216,
        "labels": null,
        "name": "i-deadbeef",
        "port": 9090,
        "weight": 1
    }
]
"""


class ServiceInventoryTests(unittest.TestCase):
    def setUp(self):
        self.mock_filewatcher = mock.Mock(spec=FileWatcher)
        self.inventory = service_discovery.ServiceInventory("/whatever")
        self.inventory._filewatcher = self.mock_filewatcher

    def _set_inventory_contents(self, text):
        parsed = service_discovery._parse(StringIO(text))
        self.mock_filewatcher.get_data.return_value = parsed

    def test_load_backends(self):
        self._set_inventory_contents(TEST_INVENTORY_ONE)

        backends = self.inventory.get_backends()
        self.assertEqual(len(backends), 1)
        self.assertEqual(backends[0].id, 205)
        self.assertEqual(backends[0].name, "i-258fc8b6")
        self.assertEqual(backends[0].endpoint.address.host, "10.0.1.2")
        self.assertEqual(backends[0].endpoint.address.port, 9090)
        self.assertEqual(backends[0].weight, 1)

        self._set_inventory_contents(TEST_INVENTORY_TWO)

        backends = self.inventory.get_backends()
        self.assertEqual(len(backends), 3)

    def test_single_get(self):
        self._set_inventory_contents(TEST_INVENTORY_ONE)
        backend = self.inventory.get_backend()
        self.assertEqual(backend.id, 205)

    def test_no_backends_available(self):
        self.mock_filewatcher.get_data.side_effect = WatchedFileNotAvailableError("", None)
        with self.assertRaises(service_discovery.NoBackendsAvailableError):
            self.inventory.get_backend()
        self.assertEqual(self.inventory.get_backends(), [])
        self.mock_filewatcher.get_data.side_effect = None

        self._set_inventory_contents("[]")
        with self.assertRaises(service_discovery.NoBackendsAvailableError):
            self.inventory.get_backend()
