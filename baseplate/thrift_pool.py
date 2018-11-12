"""A Thrift client connection pool.

.. note:: See :py:class:`baseplate.context.thrift.ThriftContextFactory` for
    a convenient way to integrate the pool with your application.

The pool lazily creates connections and maintains them in a pool. Individual
connections have a maximum lifetime, after which they will be recycled.

A basic example of usage::

    pool = thrift_pool_from_config(app_config, "example_service.")
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
from thrift.Thrift import TApplicationException, TException
from thrift.transport import TSocket
from thrift.transport.TTransport import TTransportException

from baseplate._compat import queue
from baseplate import config
from baseplate.retry import RetryPolicy


logger = logging.getLogger(__name__)


def _make_transport(endpoint):
    if endpoint.family == socket.AF_INET:
        trans = TSocket.TSocket(*endpoint.address)
    elif endpoint.family == socket.AF_UNIX:
        trans = TSocket.TSocket(unix_socket=endpoint.address)
    else:
        raise Exception("unsupported endpoint family %r" % endpoint.family)

    return trans


def thrift_pool_from_config(app_config, prefix, **kwargs):
    """Make a ThriftConnectionPool from a configuration dictionary.

    The keys useful to :py:func:`thrift_pool_from_config` should be prefixed,
    e.g.  ``example_service.endpoint`` etc. The ``prefix`` argument specifies
    the prefix used to filter keys.  Each key is mapped to a corresponding
    keyword argument on the :py:class:`ThriftConnectionPool` constructor.  Any
    keyword arguments given to this function will be also be passed through to
    the constructor. Keyword arguments take precedence over the configuration
    file.

    Supported keys:

    * ``endpoint`` (required): A ``host:port`` pair, e.g. ``localhost:2014``,
        where the Thrift server can be found.
    * ``size``: The size of the connection pool.
    * ``max_age``: The oldest a connection can be before it's recycled and
        replaced with a new one. Written as a time span e.g. ``1 minute``.
    * ``timeout``: The maximum amount of time a connection attempt or RPC call
        can take before a TimeoutError is raised.
    * ``max_retries``: The maximum number of times the pool will attempt to
        open a connection.

    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]

    cfg = config.parse_config(app_config, {
        config_prefix: {
            "endpoint": config.Endpoint,
            "size": config.Optional(config.Integer, default=10),
            "max_age": config.Optional(config.Timespan, default=config.Timespan("1 minute")),
            "timeout": config.Optional(config.Timespan, default=config.Timespan("1 second")),
            "max_retries": config.Optional(config.Integer, default=3),
        },
    })
    options = getattr(cfg, config_prefix)

    if options.size is not None:
        kwargs.setdefault("size", options.size)
    if options.max_age is not None:
        kwargs.setdefault("max_age", options.max_age.total_seconds())
    if options.timeout is not None:
        kwargs.setdefault("timeout", options.timeout.total_seconds())
    if options.max_retries is not None:
        kwargs.setdefault("max_retries", options.max_retries)

    return ThriftConnectionPool(endpoint=options.endpoint, **kwargs)


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
    :param protocol_factory: The factory to use for creating protocols from
        transports. This is useful for talking to services that don't support
        THeaderProtocol.

    All exceptions raised by this class derive from
    :py:exc:`~thrift.transport.TTransport.TTransportException`.

    """
    # pylint: disable=too-many-arguments
    def __init__(self, endpoint, size=10, max_age=120, timeout=1, max_retries=3,
                 protocol_factory=THeaderProtocol.THeaderProtocolFactory()):
        self.endpoint = endpoint
        self.max_age = max_age
        self.retry_policy = RetryPolicy.new(attempts=max_retries)
        self.timeout = timeout
        self.protocol_factory = protocol_factory

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

            trans = _make_transport(self.endpoint)
            trans.setTimeout(self.timeout * 1000.)
            prot = self.protocol_factory.getProtocol(trans)

            try:
                prot.trans.open()
            except TTransportException as exc:
                logger.info("Failed to connect to %r: %s",
                    self.endpoint, exc)
                prot = None
                continue

            prot.baseplate_birthdate = time.time()

            return prot

        self.pool.put(None)

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
            try:
                yield prot
            except socket.timeout:
                # thrift doesn't re-wrap socket timeout errors appropriately so
                # we'll do it here for a saner exception hierarchy
                raise TTransportException(
                    type=TTransportException.TIMED_OUT,
                    message="timed out interacting with socket",
                )
            except socket.error as exc:
                raise TTransportException(
                    type=TTransportException.UNKNOWN,
                    message=str(exc),
                )
        except (TApplicationException, TProtocolException, TTransportException):
            # these exceptions usually indicate something low-level went wrong,
            # so it's safest to just close this connection because we don't
            # know what state it's in.
            prot.trans.close()
            raise
        except TException as exc:
            # the only other TException-derived errors are application level
            # (expected) errors which should be safe for the connection.
            # don't close the transport here!
            raise
        except:
            # anything else coming out of thrift usually means parsing failed
            # or something nastier. we'll just play it safe and close the
            # connection.
            prot.trans.close()
            raise
        finally:
            self._release(prot)
