"""Internal helpers for the requests HTTP client library.

This stuff is not stable yet, so it's only for baseplate-internal use.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import socket
import urlparse
import urllib

import requests.adapters
import urllib3.connectionpool

# the adapter code below is inspired by similar code in
# https://github.com/msabramo/requests-unixsocket/blob/master/requests_unixsocket/adapters.py
# https://github.com/docker/docker-py/blob/master/docker/unixconn/unixconn.py


class _UNIXConnection(urllib3.connectionpool.HTTPConnection):
    # pylint: disable=super-init-not-called
    def __init__(self, url):
        urllib3.connectionpool.HTTPConnection.__init__(self, "localhost")
        self.url = urlparse.urlparse(url)

    def connect(self):
        socket_path = urllib.unquote(self.url.netloc)

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(1)
        self.sock.connect(socket_path)


class _UNIXConnectionPool(urllib3.connectionpool.HTTPConnectionPool):
    def __init__(self, url):
        super(_UNIXConnectionPool, self).__init__(host="localhost")
        self.url = url

    def _new_conn(self):
        return _UNIXConnection(self.url)


class _UNIXAdapter(requests.adapters.HTTPAdapter):
    def get_connection(self, url, proxies=None):
        assert not proxies, "proxies are not supported"
        return _UNIXConnectionPool(url)


def add_unix_socket_support(session):
    """Add support to a Requests session for HTTP over UNIX domain sockets."""
    session.mount("http+unix://", _UNIXAdapter())
