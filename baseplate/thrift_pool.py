"""A Thrift client connection pool.

.. note:: See :py:class:`baseplate.context.thrift.ThriftContextFactory` for
    a convenient way to integrate the pool with your application.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import contextlib
import logging
import socket
import time

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import THeaderProtocol

from ._compat import queue


logger = logging.getLogger(__name__)


def _make_protocol(endpoint):
    if endpoint.family == socket.AF_INET:
        trans = TSocket.TSocket(*endpoint.address)
    elif endpoint.family == socket.AF_UNIX:
        trans = TSocket.TSocket(unix_socket=endpoint.address)
    else:
        raise Exception("unsupported endpoint family %r" % endpoint.family)
    return THeaderProtocol.THeaderProtocol(trans)


class ThriftPoolError(Thrift.TException):
    """The base class for all thrift connection pool errors."""
    pass


class TimeoutError(ThriftPoolError):
    """Raised when the pool times out during an operation.

    This can be raised if:

    * the pool spends too long waiting for an available connection
    * a connection attempt takes too long
    * an RPC takes too long

    """
    def __init__(self):
        super(TimeoutError, self).__init__("timed out")


class MaxRetriesError(ThriftPoolError):
    """Raised when the maximum number of connection attempts is exceeded."""
    def __init__(self):
        super(MaxRetriesError, self).__init__(
            "giving up after multiple attempts to connect")


class ThriftConnectionPool(object):
    """A pool that maintains a queue of open Thrift connections.

    :param baseplate.config.EndpointConfiguration endpoint: The remote address
        of the Thrift service.
    :param int size: The maximum number of connections that can be open
        before new attempts to open block.
    :param int max_age: The maximum number of seconds a connection should be
        kept alive. Connections older than this will be reaped.
    :param int timeout: The maximum number of seconds a connection attempt or
        RPC call can take before a TimeoutError is raised.
    :param int max_retries: The maximum number of times the pool will attempt
        to open a connection.

    """
    def __init__(self, endpoint, size=10, max_age=120, timeout=1, max_retries=3):
        self.endpoint = endpoint
        self.max_age = max_age
        self.max_retries = max_retries
        self.timeout = timeout

        self.pool = queue.LifoQueue()
        for i in range(size):
            self.pool.put(None)

    def _acquire(self):
        try:
            prot = self.pool.get(block=True, timeout=self.timeout)
        except queue.Empty:
            raise TimeoutError

        for i in range(self.max_retries):
            if prot:
                if time.time() - prot.baseplate_birthdate < self.max_age:
                    return prot
                else:
                    prot.trans.close()
                    prot = None

            prot = _make_protocol(self.endpoint)
            prot.trans.getTransport().setTimeout(self.timeout * 1000.)

            try:
                prot.trans.open()
            except TTransport.TTransportException as exc:
                logger.info("Failed to connect to %r: %s",
                    self.endpoint, exc)
                prot = None
                continue

            prot.baseplate_birthdate = time.time()

            return prot
        else:
            raise MaxRetriesError

    def _release(self, prot):
        if prot.trans.isOpen():
            self.pool.put(prot)
        else:
            self.pool.put(None)

    @contextlib.contextmanager
    def connection(self):
        """Acquire a connection from the pool.

        This method is to be used with a context manager. It returns a
        connection from the pool, or blocks up to :attr:`timeout` seconds
        waiting for one if the pool is full and all connections are in use.

        When the context is exited, the connection is returned to the pool.
        However, if it was exited via an unexpected Thrift exception, the
        connection is closed instead because the state of the connection is
        unknown.

        """
        prot = self._acquire()
        try:
            yield prot
        except Thrift.TException:
            prot.trans.close()
            raise
        except socket.timeout:
            prot.trans.close()
            raise TimeoutError
        except socket.error as exc:
            prot.trans.close()
            raise ThriftPoolError(str(exc))
        finally:
            self._release(prot)
