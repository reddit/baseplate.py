"""Check health of a baseplate service on localhost."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import sys

import requests

from thrift.protocol import THeaderProtocol
from thrift.transport import TSocket

from baseplate.thrift import BaseplateService


TIMEOUT = 30  # seconds


def check_thrift_service(port):
    transport = TSocket.TSocket("localhost", port)
    transport.setTimeout(TIMEOUT * 1000)
    transport.open()
    protocol = THeaderProtocol.THeaderProtocol(transport)
    client = BaseplateService.Client(protocol)
    assert client.is_healthy(), "service indicated unhealthiness"


def check_http_service(port):
    url = "http://localhost:{port}/health".format(port=port)
    response = requests.get(url, timeout=TIMEOUT)
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
    parser.add_argument("port", type=int, default=9090,
        help="The port to find the service on.")

    return parser.parse_args()


def run_healthchecks():
    args = parse_args()

    checker = CHECKERS[args.type]
    checker(args.port)
    print("OK!")


if __name__ == "__main__":
    run_healthchecks()
