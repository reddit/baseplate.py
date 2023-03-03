import atexit
import importlib
import json
import logging
import os
import sys
import urllib

from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import TYPE_CHECKING

from gevent.pywsgi import LoggingLogAdapter
from gevent.pywsgi import WSGIServer
from prometheus_client import CollectorRegistry
from prometheus_client import CONTENT_TYPE_LATEST
from prometheus_client import generate_latest
from prometheus_client import multiprocess
from prometheus_client import values
from prometheus_client.values import MultiProcessValue

from baseplate.lib import config
from baseplate.lib.config import Endpoint
from baseplate.lib.config import EndpointConfiguration
from baseplate.server.net import bind_socket

logger = logging.getLogger(__name__)
ADMIN_ADDRESS = Endpoint("0.0.0.0:6060")
METRICS_ENDPOINT = "/metrics"
HEALTH_ENDPOINT = "/health"

if TYPE_CHECKING:
    from _typeshed.wsgi import StartResponse  # pylint: disable=import-error,no-name-in-module
    from _typeshed.wsgi import WSGIEnvironment  # pylint: disable=import-error,no-name-in-module


def is_health_enabled(raw_config: Dict[str, str]) -> bool:
    cfg = config.parse_config(
        raw_config,
        {
            "health": {
                "enabled": config.Optional(config.Boolean, default=False),
            }
        },
    )

    if cfg.health.enabled is not None:
        return cfg.health.enabled

    return False


def get_health_callback(raw_config: Dict[str, str]) -> bool:
    cfg = config.parse_config(
        raw_config,
        {
            "health": {
                "callback": config.Optional(config.String),
            }
        },
    )

    if cfg.health.callback:
        logger.info(f"selected callback is: {cfg.health.callback}")
        return load_function(cfg.health.callback)

    return lambda: False


def load_function(url: str, default_name: Optional[str] = None) -> Callable:
    """Load a factory function from a config file."""
    module_name, sep, func_name = url.partition(":")
    module = importlib.import_module(module_name)
    function = getattr(module, func_name)
    return function


def worker_id() -> str:
    worker = os.environ.get("MULTIPROCESS_WORKER_ID")
    if worker is None:
        worker = str(os.getpid())
    return worker


class AdminServer:
    def __init__(
        self,
        address: EndpointConfiguration = ADMIN_ADDRESS,
        serve_health: bool = False,
        serve_metrics: bool = False,
        healthcheck: Callable = lambda: True,
    ):
        self.address = address
        self.serve_health = serve_health
        self.serve_metrics = serve_metrics
        self.healthcheck = healthcheck

        if self.serve_metrics:
            self.check_can_serve()

        values.ValueClass = MultiProcessValue(worker_id)
        atexit.register(multiprocess.mark_process_dead, worker_id())

        server_socket = bind_socket(address)
        self.server = WSGIServer(
            server_socket,
            application=self.admin_pages,
            log=LoggingLogAdapter(logger, level=logging.DEBUG),
            error_log=LoggingLogAdapter(logger, level=logging.ERROR),
        )

    def check_can_serve(self):
        if "PROMETHEUS_MULTIPROC_DIR" not in os.environ:
            logger.error(
                "prometheus-client is installed but PROMETHEUS_MULTIPROC_DIR is not set to a writeable directory."
            )
            sys.exit(1)

    def admin_pages(
        self, environ: "WSGIEnvironment", start_response: "StartResponse"
    ) -> Iterable[bytes]:
        if self.serve_metrics and environ["PATH_INFO"] == METRICS_ENDPOINT:
            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
            data = generate_latest(registry)
            response_headers = [
                ("Content-type", CONTENT_TYPE_LATEST),
                ("Content-Length", str(len(data))),
            ]
            start_response("200 OK", response_headers)
            return [data]

        elif self.serve_health and environ["PATH_INFO"] == HEALTH_ENDPOINT:
            queries = urllib.parse.parse_qs(environ["QUERY_STRING"])
            check = queries.get("check", "liveness")

            status = "ok"
            response = "200 OK"
            if check == "liveness":
                pass
            elif check == "startup" or check == "readiness":
                if self.healthcheck():
                    status = "ok"
                    response = "200 OK"
                else:
                    status = "check_failed"
                    response = "503 Service Unavailable"
            else:
                status = "error"
                response = "400 Bad Request"

            logger.info(f"{check}, {status}, {response}")
            response_headers = [("content-type", "application/json")]
            data = json.dumps({"service": "service_name", "status": status, "check_type": check})
            start_response(response, response_headers)
            return [data.encode("utf-8")]

        start_response("404 Not Found", [("Content-Type", "text/plain")])
        return [b"Not Found"]

    def start(self) -> None:
        if self.serve_metrics:
            logger.info(
                "Prometheus metrics exported on server listening on %s%s",
                self.address,
                METRICS_ENDPOINT,
            )
        if self.serve_health:
            logger.info(
                "Baseplate health check server listening on %s%s",
                self.address,
                HEALTH_ENDPOINT,
            )

        self.server.start()

    def stop(self) -> None:
        self.server.stop(self._stop_timeout)
