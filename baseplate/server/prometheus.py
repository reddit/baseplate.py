"""Prometheus exporter server.

This is automatically run by `baseplate-serve` whenever the `prometheus-client`
package is installed.

This exporter is designed for multi-process usage, like under Einhorn.  The
PROMETHEUS_MULTIPROC_DIR environment variable must be set to the path of an
existing writeable directory where individual application workers will write
their metrics. Each application worker will be serving the exporter as well and
can aggregate and serve metrics for all workers.

"""
import logging
import os

from baseplate.lib.config import Endpoint
from baseplate.lib.config import EndpointConfiguration
from baseplate.server.admin import AdminServer
from baseplate.server.net import bind_socket

logger = logging.getLogger(__name__)
PROMETHEUS_EXPORTER_ADDRESS = Endpoint("0.0.0.0:6060")


def start_prometheus_exporter(address: EndpointConfiguration = PROMETHEUS_EXPORTER_ADDRESS) -> None:
    server = AdminServer(address=address, serve_metrics=True)
    server.start()


def start_prometheus_exporter_for_sidecar() -> None:
    port = os.environ.get("BASEPLATE_SIDECAR_ADMIN_PORT")
    if port is None:
        logger.error(
            "BASEPLATE_SIDECAR_ADMIN_PORT must be set for sidecar to expose Prometheus metrics."
        )
    else:
        endpoint = Endpoint("0.0.0.0:" + port)
        start_prometheus_exporter(endpoint)
