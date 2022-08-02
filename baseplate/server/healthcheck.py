"""Check health of a baseplate service on localhost."""
import argparse
import re
import socket
import sys
import typing
import urllib.parse

import requests

from baseplate.lib._requests import add_unix_socket_support
from baseplate.lib.config import Endpoint
from baseplate.lib.config import EndpointConfiguration
from baseplate.lib.config import InternetAddress
from baseplate.lib.thrift_pool import ThriftConnectionPool
from baseplate.thrift import BaseplateServiceV2
from baseplate.thrift.ttypes import IsHealthyProbe
from baseplate.thrift.ttypes import IsHealthyRequest


TIMEOUT = 30  # seconds
REDIS_TIMEOUT = 2  # seconds
PING_BUFFER = 128  # byte size
CLUSTER_INFO_BUFFER = 1024  # byte size


def check_thrift_service(endpoint: EndpointConfiguration, probe: int) -> None:
    pool = ThriftConnectionPool(endpoint, size=1, timeout=TIMEOUT)
    with pool.connection() as protocol:
        client = BaseplateServiceV2.Client(protocol)
        assert client.is_healthy(
            request=IsHealthyRequest(probe=probe),
        ), f"service indicated unhealthiness in probe {probe}"


def check_http_service(endpoint: EndpointConfiguration, probe: int) -> None:
    if endpoint.family == socket.AF_INET:
        address: InternetAddress = typing.cast(InternetAddress, endpoint.address)
        url = f"http://{address.host}:{address.port}/health?type={probe}"
    elif endpoint.family == socket.AF_UNIX:
        quoted_path = urllib.parse.quote(typing.cast(str, endpoint.address), safe="")
        url = f"http+unix://{quoted_path}/health?type={probe}"
    else:
        raise ValueError(f"unrecognized socket family {endpoint.family!r}")

    session = requests.Session()
    add_unix_socket_support(session)
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    response.json()


def check_redis_service(endpoint: EndpointConfiguration, probe: int) -> None:
    # pylint: disable=unused-argument
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(REDIS_TIMEOUT)
            address: InternetAddress = typing.cast(InternetAddress, endpoint.address)
            sock.connect((address.host, address.port))
            sock.sendall(b"PING\n")
            data = sock.recv(PING_BUFFER).decode("UTF-8")
            if not re.match(r"\+PONG", data):
                raise ValueError("Did not receive a PONG to the PING")
    except socket.timeout:
        raise ValueError("Cannot connect to the endpoint")


CHECKERS = {
    "thrift": check_thrift_service,
    "wsgi": check_http_service,
    "redis": check_redis_service,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)

    parser.add_argument(
        "type",
        choices=CHECKERS.keys(),
        default="thrift",
        help="The protocol of the service to check.",
    )
    parser.add_argument(
        "endpoint",
        type=Endpoint,
        default=Endpoint("localhost:9090"),
        help="The endpoint to find the service on.",
    )
    parser.add_argument(
        "--probe",
        choices=[probe.lower() for probe in IsHealthyProbe._NAMES_TO_VALUES],
        default="readiness",
        help="The probe to check.",
    )

    return parser.parse_args()


def run_healthchecks() -> None:
    args = parse_args()

    checker = CHECKERS[args.type]
    checker(args.endpoint, IsHealthyProbe._NAMES_TO_VALUES[args.probe.upper()])
    print("OK!")


if __name__ == "__main__":
    run_healthchecks()
