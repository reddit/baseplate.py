"""Check health of a baseplate service on localhost."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import socket
import sys
import urllib

import requests

from baseplate.config import Endpoint
from baseplate.requests import add_unix_socket_support
from baseplate.thrift import BaseplateService
from baseplate.thrift_pool import ThriftConnectionPool


TIMEOUT = 30  # seconds


def check_thrift_service(endpoint):
    pool = ThriftConnectionPool(endpoint, size=1, timeout=TIMEOUT)
    with pool.connection() as protocol:
        client = BaseplateService.Client(protocol)
        assert client.is_healthy(), "service indicated unhealthiness"


def check_http_service(endpoint):
    if endpoint.family == socket.AF_INET:
        url = "http://{host}:{port}/health".format(
            host=endpoint.address.host, port=endpoint.address.port)
    elif endpoint.family == socket.AF_UNIX:
        quoted_path = urllib.quote(endpoint.address, safe="")
        url = "http+unix://{path}/health".format(path=quoted_path)
    else:
        raise ValueError("unrecognized socket family %r" % endpoint.family)

    session = requests.Session()
    add_unix_socket_support(session)
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    response.json()


CHECKERS = {
    "thrift": check_thrift_service,
    "wsgi": check_http_service,
}


def parse_args():
    parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__,)

    parser.add_argument("type", choices=CHECKERS.keys(), default="thrift",
        help="The protocol of the service to check.")
    parser.add_argument("endpoint", type=Endpoint, default=Endpoint("localhost:9090"),
        help="The endpoint to find the service on.")

    return parser.parse_args()


def run_healthchecks():
    args = parse_args()

    checker = CHECKERS[args.type]
    checker(args.endpoint)
    print("OK!")


if __name__ == "__main__":
    run_healthchecks()
