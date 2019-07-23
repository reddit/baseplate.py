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
import json

from typing import IO
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Sequence

from baseplate.lib.config import Endpoint
from baseplate.lib.config import EndpointConfiguration
from baseplate.lib.file_watcher import FileWatcher
from baseplate.lib.file_watcher import WatchedFileNotAvailableError
from baseplate.lib.random import WeightedLottery


class Backend(NamedTuple):
    """A description of a service backend.

    This is a tuple of several values:

    ``id``
        A unique integer ID identifying the backend.

    ``name``
        The name of the backend.

    ``endpoint``
        An :py:class:`~baseplate.lib.config.EndpointConfiguration` object
        describing the network address of the backend.

    ``weight``
        An integer weight indicating how much to prefer this backend
        when choosing whom to connect to.

    """

    id: int
    name: str
    endpoint: EndpointConfiguration
    weight: int


class _Inventory(NamedTuple):
    backends: List[Backend]
    lottery: Optional[WeightedLottery[Backend]]


def _parse(watched_file: IO) -> _Inventory:
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


class ServiceInventory:
    """The inventory enumerates available backends for a single service.

    :param filename: The absolute path to the Synapse-generated inventory file
        in JSON format.

    """

    def __init__(self, filename: str):
        self._filewatcher = FileWatcher(filename, _parse)

    def get_backends(self) -> Sequence[Backend]:
        """Return a list of all available backends in the inventory.

        If the inventory file becomes unavailable, the previously seen
        inventory is returned.

        """
        try:
            # pylint: disable=maybe-no-member
            return self._filewatcher.get_data().backends
        except WatchedFileNotAvailableError:
            return []

    def get_backend(self) -> Backend:
        """Return a randomly chosen backend from the available backends.

        If weights are specified in the inventory, they will be
        respected when making the random selection.

        :raises: :py:exc:`NoBackendsAvailableError` if the inventory
            has no available endpoints.

        """
        inventory: Optional[_Inventory]

        try:
            inventory = self._filewatcher.get_data()
        except WatchedFileNotAvailableError:
            inventory = None

        # pylint: disable=maybe-no-member
        if not inventory or not inventory.lottery:
            raise NoBackendsAvailableError

        return inventory.lottery.pick()
