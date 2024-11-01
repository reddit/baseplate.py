"""Prometheus exporter server.

This is automatically run by `baseplate-serve` whenever the `prometheus-client`
package is installed.

This exporter is designed for multi-process usage, like under Einhorn.  The
PROMETHEUS_MULTIPROC_DIR environment variable must be set to the path of an
existing writeable directory where individual application workers will write
their metrics. Each application worker will be serving the exporter as well and
can aggregate and serve metrics for all workers.

"""

import atexit
import logging
import os
import sys
from collections.abc import Iterable
from typing import TYPE_CHECKING

from gevent.pywsgi import LoggingLogAdapter, WSGIServer
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
    multiprocess,
    values,
)
from prometheus_client.values import MultiProcessValue

from baseplate.lib.config import Endpoint, EndpointConfiguration
from baseplate.server.net import bind_socket

if TYPE_CHECKING:
    from _typeshed.wsgi import (
        StartResponse,  # pylint: disable=import-error,no-name-in-module
        WSGIEnvironment,  # pylint: disable=import-error,no-name-in-module
    )


logger = logging.getLogger(__name__)
PROMETHEUS_EXPORTER_ADDRESS = Endpoint("0.0.0.0:6060")
METRICS_ENDPOINT = "/metrics"


def worker_id() -> str:
    worker = os.environ.get("MULTIPROCESS_WORKER_ID")
    if worker is None:
        worker = str(os.getpid())
    return worker


def export_metrics(environ: "WSGIEnvironment", start_response: "StartResponse") -> Iterable[bytes]:
    if environ["PATH_INFO"] != METRICS_ENDPOINT:
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        return [b"Not Found"]

    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    data = generate_latest(registry)
    response_headers = [("Content-type", CONTENT_TYPE_LATEST), ("Content-Length", str(len(data)))]
    start_response("200 OK", response_headers)
    return [data]


def start_prometheus_exporter(address: EndpointConfiguration = PROMETHEUS_EXPORTER_ADDRESS) -> None:
    if "PROMETHEUS_MULTIPROC_DIR" not in os.environ:
        logger.error(
            "prometheus-client is installed but PROMETHEUS_MULTIPROC_DIR is not set to a writeable directory."  # noqa: E501
        )
        sys.exit(1)

    values.ValueClass = MultiProcessValue(worker_id)
    atexit.register(multiprocess.mark_process_dead, worker_id())

    server_socket = bind_socket(address)
    server = WSGIServer(
        server_socket,
        application=export_metrics,
        log=LoggingLogAdapter(logger, level=logging.DEBUG),
        error_log=LoggingLogAdapter(logger, level=logging.ERROR),
    )
    logger.info(
        "Prometheus metrics exported on server listening on %s%s",
        address,
        METRICS_ENDPOINT,
    )
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
