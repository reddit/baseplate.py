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

from .config import Endpoint
from .file_watcher import FileWatcher, WatchedFileNotAvailableError
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


_Inventory = collections.namedtuple("_Inventory", "backends lottery")


def _parse(watched_file):
    backends = []
    for d in json.load(watched_file):
        endpoint = Endpoint("%s:%d" % (d["host"], d["port"]))
        weight = d["weight"] if d["weight"] is not None else 1
        backend = Backend(d["id"], d["name"], endpoint, weight)
        backends.append(backend)

    lottery = None
    if backends:
        lottery = WeightedLottery(backends, weight_key=lambda b: b.weight)

    return _Inventory(backends, lottery)


class NoBackendsAvailableError(Exception):
    """Raised when no backends are available for this service."""
    pass


class ServiceInventory(object):
    """The inventory enumerates available backends for a single service.

    :param str filename: The absolute path to the Synapse-generated
        inventory file in JSON format.

    """
    def __init__(self, filename):
        self._filewatcher = FileWatcher(filename, _parse)

    def get_backends(self):
        """Return a list of all available backends in the inventory.

        If the inventory file becomes unavailable, the previously seen
        inventory is returned.

        :rtype: list of :py:class:`Backend` objects

        """

        try:
            # pylint: disable=maybe-no-member
            return self._filewatcher.get_data().backends
        except WatchedFileNotAvailableError:
            return []

    def get_backend(self):
        """Return a randomly chosen backend from the available backends.

        If weights are specified in the inventory, they will be
        respected when making the random selection.

        :rtype: :py:class:`Backend`
        :raises: :py:exc:`NoBackendsAvailableError` if the inventory
            has no available endpoints.

        """

        try:
            inventory = self._filewatcher.get_data()
        except WatchedFileNotAvailableError:
            inventory = None

        # pylint: disable=maybe-no-member
        if not inventory or not inventory.backends:
            raise NoBackendsAvailableError

        return inventory.lottery.pick()
