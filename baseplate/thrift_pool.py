"""A Thrift client connection pool.

.. note:: See :py:class:`baseplate.context.thrift.ThriftContextFactory` for
    a convenient way to integrate the pool with your application.

The pool lazily creates connections and maintains them in a pool. Individual
connections have a maximum lifetime, after which they will be recycled.

A basic example of usage::

    pool = ThriftConnectionPool(endpoint)
    with pool.connection() as protocol:
        client = ExampleService.Client(protocol)
        client.do_example_thing()

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import contextlib
import logging
import socket
import time

from thrift.protocol import THeaderProtocol
from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException
from thrift.transport import TSocket
from thrift.transport.TTransport import TTransportException

from ._compat import queue
from .retry import RetryPolicy


logger = logging.getLogger(__name__)


def _make_protocol(endpoint):
    if endpoint.family == socket.AF_INET:
        trans = TSocket.TSocket(*endpoint.address)
    elif endpoint.family == socket.AF_UNIX:
        trans = TSocket.TSocket(unix_socket=endpoint.address)
    else:
        raise Exception("unsupported endpoint family %r" % endpoint.family)
    return THeaderProtocol.THeaderProtocol(trans)


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

    All exceptions raised by this class derive from
    :py:exc:`~thrift.transport.TTransport.TTransportException`.

    """
    # pylint: disable=too-many-arguments
    def __init__(self, endpoint, size=10, max_age=120, timeout=1, max_retries=3):
        self.endpoint = endpoint
        self.max_age = max_age
        self.retry_policy = RetryPolicy.new(attempts=max_retries)
        self.timeout = timeout

        self.pool = queue.LifoQueue()
        for _ in range(size):
            self.pool.put(None)

    def _acquire(self):
        try:
            prot = self.pool.get(block=True, timeout=self.timeout)
        except queue.Empty:
            raise TTransportException(
                type=TTransportException.NOT_OPEN,
                message="timed out waiting for a connection slot",
            )

        for _ in self.retry_policy:
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
            except TTransportException as exc:
                logger.info("Failed to connect to %r: %s",
                    self.endpoint, exc)
                prot = None
                continue

            prot.baseplate_birthdate = time.time()

            return prot

        raise TTransportException(
            type=TTransportException.NOT_OPEN,
            message="giving up after multiple attempts to connect",
        )

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
        except (TApplicationException, TProtocolException, TTransportException):
            # these exceptions usually indicate something low-level went wrong,
            # so it's safest to just close this connection because we don't
            # know what state it's in. the only other TException-derived errors
            # should be application level errors which should be safe for the
            # connection.
            prot.trans.close()
            raise
        except socket.timeout:
            # thrift doesn't re-wrap socket timeout errors appropriately so
            # we'll do it here for a saner exception hierarchy
            prot.trans.close()
            raise TTransportException(
                type=TTransportException.TIMED_OUT,
                message="timed out interacting with socket",
            )
        except socket.error as exc:
            prot.trans.close()
            raise TTransportException(
                type=TTransportException.UNKNOWN,
                message=str(exc),
            )
        finally:
            self._release(prot)
