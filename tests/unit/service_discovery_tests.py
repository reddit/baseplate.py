from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import tempfile
import time
import unittest

from baseplate import service_discovery


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
        self.inventory_filename = tempfile.mktemp()

    def tearDown(self):
        try:
            os.unlink(self.inventory_filename)
        except OSError:
            pass

    def test_load_backends(self):
        with open(self.inventory_filename, "w") as f:
            f.write(TEST_INVENTORY_ONE)

        inventory = service_discovery.ServiceInventory(self.inventory_filename)

        backends = inventory.get_backends()
        self.assertEqual(len(backends), 1)
        backends = inventory.get_backends()
        self.assertEqual(len(backends), 1)
        self.assertEqual(backends[0].id, 205)
        self.assertEqual(backends[0].name, "i-258fc8b6")
        self.assertEqual(backends[0].endpoint.address.host, "10.0.1.2")
        self.assertEqual(backends[0].endpoint.address.port, 9090)
        self.assertEqual(backends[0].weight, 1)

        with open(self.inventory_filename, "w") as f:
            f.write(TEST_INVENTORY_TWO)

        # force update the mtime into the future so we don't have to
        # wait around for the filesystem's minimum resolution
        os.utime(self.inventory_filename, (time.time(), time.time()))

        backends = inventory.get_backends()
        self.assertEqual(len(backends), 3)
        backends = inventory.get_backends()
        self.assertEqual(len(backends), 3)

    def test_file_starts_out_missing_then_appears(self):
        inventory = service_discovery.ServiceInventory(self.inventory_filename)

        backends = inventory.get_backends()
        self.assertEqual(backends, [])

        with open(self.inventory_filename, "w") as f:
            f.write(TEST_INVENTORY_ONE)

        backends = inventory.get_backends()
        self.assertEqual(len(backends), 1)

    def test_file_exists_then_goes_missing(self):
        with open(self.inventory_filename, "w") as f:
            f.write(TEST_INVENTORY_ONE)

        inventory = service_discovery.ServiceInventory(self.inventory_filename)
        backends = inventory.get_backends()
        self.assertEqual(len(backends), 1)

        os.unlink(self.inventory_filename)

        backends = inventory.get_backends()
        self.assertEqual(len(backends), 1)

    def test_single_get(self):
        with open(self.inventory_filename, "w") as f:
            f.write(TEST_INVENTORY_ONE)

        inventory = service_discovery.ServiceInventory(self.inventory_filename)
        backend = inventory.get_backend()
        self.assertEqual(backend.id, 205)

    def test_no_backends_available(self):
        inventory = service_discovery.ServiceInventory(self.inventory_filename)
        with self.assertRaises(service_discovery.NoBackendsAvailableError):
            inventory.get_backend()
