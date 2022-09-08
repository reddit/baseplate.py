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

from typing import Iterable
from typing import TYPE_CHECKING

from gevent.pywsgi import LoggingLogAdapter
from gevent.pywsgi import WSGIServer
from prometheus_client import CollectorRegistry
from prometheus_client import CONTENT_TYPE_LATEST
from prometheus_client import generate_latest
from prometheus_client import multiprocess

from baseplate.lib.config import Endpoint
from baseplate.server.net import bind_socket


if TYPE_CHECKING:
    from _typeshed.wsgi import StartResponse  # pylint: disable=import-error,no-name-in-module
    from _typeshed.wsgi import WSGIEnvironment  # pylint: disable=import-error,no-name-in-module


logger = logging.getLogger(__name__)
PROMETHEUS_EXPORTER_ADDRESS = Endpoint("0.0.0.0:6060")
METRICS_ENDPOINT = "/metrics"


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


def start_prometheus_exporter() -> None:
    if "PROMETHEUS_MULTIPROC_DIR" not in os.environ:
        logger.error(
            "prometheus-client is installed but PROMETHEUS_MULTIPROC_DIR is not set to a writeable directory."
        )
        sys.exit(1)

    atexit.register(multiprocess.mark_process_dead, os.getpid())

    server_socket = bind_socket(PROMETHEUS_EXPORTER_ADDRESS)
    server = WSGIServer(
        server_socket,
        application=export_metrics,
        log=LoggingLogAdapter(logger, level=logging.DEBUG),
        error_log=LoggingLogAdapter(logger, level=logging.ERROR),
    )
    logger.info(
        "Prometheus metrics exported on server listening on %s%s",
        PROMETHEUS_EXPORTER_ADDRESS,
        METRICS_ENDPOINT,
    )
    server.start()
