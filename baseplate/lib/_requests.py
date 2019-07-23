"""Internal helpers for the requests HTTP client library.

This stuff is not stable yet, so it's only for baseplate-internal use.

"""
import socket
import urllib.parse

from typing import Mapping
from typing import Optional

import requests.adapters
import urllib3.connectionpool

# the adapter code below is inspired by similar code in
# https://github.com/msabramo/requests-unixsocket/blob/master/requests_unixsocket/adapters.py
# https://github.com/docker/docker-py/blob/master/docker/unixconn/unixconn.py


class _UNIXConnection(urllib3.connectionpool.HTTPConnection):
    # pylint: disable=super-init-not-called
    def __init__(self, url: str):
        urllib3.connectionpool.HTTPConnection.__init__(self, "localhost")
        self.url = urllib.parse.urlparse(url)

    def connect(self) -> None:
        socket_path = urllib.parse.unquote(self.url.netloc)

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(1)
        self.sock.connect(socket_path)


class _UNIXConnectionPool(urllib3.connectionpool.HTTPConnectionPool):
    def __init__(self, url: str):
        super().__init__(host="localhost")
        self.url = url

    def _new_conn(self) -> _UNIXConnection:
        return _UNIXConnection(self.url)


class _UNIXAdapter(requests.adapters.HTTPAdapter):
    def get_connection(
        self, url: str, proxies: Optional[Mapping[str, str]] = None
    ) -> _UNIXConnectionPool:
        assert not proxies, "proxies are not supported"
        return _UNIXConnectionPool(url)


def add_unix_socket_support(session: requests.Session) -> None:
    """Add support to a Requests session for HTTP over UNIX domain sockets."""
    session.mount("http+unix://", _UNIXAdapter())
