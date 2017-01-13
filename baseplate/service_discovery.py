"""Integration with Synapse's ``file_output`` service discovery method.

.. note:: Production Baseplate services have Synapse hooked up to a
    local HAProxy instance which will automatically route connections to
    services for you if you connect to the correct address/port on
    localhost. That is the preferred method of connecting to services.

    The contents of this module are useful for inspecting the service
    inventory directly for cases where a blind TCP connection is
    insufficient (e.g. to give service addresses to a client, or for
    topology-aware clients like Cassandra).

A basic example of usage::

    inventory = ServiceInventory("/var/lib/synapse/example.json")
    backend = inventory.get_backend()
    print(backend.endpoint.address)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import json
import logging
import os

from .config import Endpoint
from .random import WeightedLottery


Backend_ = collections.namedtuple("Backend", "id name endpoint weight")


class Backend(Backend_):
    """A description of a service backend.

    This is a tuple of several values:

    ``id``
        A unique integer ID identifying the backend.

    ``name``
        The name of the backend.

    ``endpoint``
        An :py:class:`~baseplate.config.EndpointConfiguration` object
        describing the network address of the backend.

    ``weight``
        An integer weight indicating how much to prefer this backend
        when choosing whom to connect to.

    """
    pass


def _backend_from_json(d):
    endpoint = Endpoint("%s:%d" % (d["host"], d["port"]))
    weight = d["weight"] if d["weight"] is not None else 1
    return Backend(d["id"], d["name"], endpoint, weight)


class NoBackendsAvailableError(Exception):
    """Raised when no backends are available for this service."""
    pass


class ServiceInventory(object):
    """The inventory enumerates available backends for a single service.

    :param str filename: The absolute path to the Synapse-generated
        inventory file in JSON format.

    """
    def __init__(self, filename):
        self.filename = filename
        self.mtime = None
        self.backends = []
        self.lottery = None

    def _load_backends(self):
        logging.debug("Loading backends from %s", self.filename)

        try:
            with open(self.filename) as f:
                self.backends = [_backend_from_json(d) for d in json.load(f)]
                self.lottery = WeightedLottery(
                    self.backends, weight_key=lambda b: b.weight)
                self.mtime = os.fstat(f.fileno()).st_mtime
        except IOError as exc:
            logging.debug("Failed to read service inventory: %s", exc)

    def get_backends(self):
        """Return a list of all available backends in the inventory.

        If the inventory file becomes unavailable, the previously seen
        inventory is returned.

        :rtype: list of :py:class:`Backend` objects

        """
        if self.mtime is None:
            # we've not loaded the file yet (or we did but that file
            # got deleted). load the inventory anew.
            self._load_backends()
        else:
            try:
                current_mtime = os.path.getmtime(self.filename)
            except OSError:
                # we had loaded the inventory, but now the file is
                # gone. blank out our mtime so we try to fetch every
                # time, but keep the old list around so we don't go
                # dark.
                self.mtime = None
            else:
                if self.mtime < current_mtime:
                    # our copy of the data is stale. reload.
                    self._load_backends()
        return self.backends

    def get_backend(self):
        """Return a randomly chosen backend from the available backends.

        If weights are specified in the inventory, they will be
        respected when making the random selection.

        :rtype: :py:class:`Backend`
        :raises: :py:exc:`NoBackendsAvailableError` if the inventory
            has no available endpoints.

        """

        # refresh as necessary
        backends = self.get_backends()
        if not backends:
            raise NoBackendsAvailableError

        return self.lottery.pick()
