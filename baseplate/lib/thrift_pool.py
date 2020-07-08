"""A Thrift client connection pool.

.. note:: See :py:class:`baseplate.clients.thrift.ThriftContextFactory` for
    a convenient way to integrate the pool with your application.

The pool lazily creates connections and maintains them in a pool. Individual
connections have a maximum lifetime, after which they will be recycled.

A basic example of usage::

    pool = thrift_pool_from_config(app_config, "example_service.")
    with pool.connection() as protocol:
        client = ExampleService.Client(protocol)
        client.do_example_thing()

"""
import contextlib
import logging
import queue
import socket
import time

from typing import Any
from typing import Generator
from typing import Optional
from typing import TYPE_CHECKING

from thrift.protocol import THeaderProtocol
from thrift.protocol.TProtocol import TProtocolBase
from thrift.protocol.TProtocol import TProtocolException
from thrift.protocol.TProtocol import TProtocolFactory
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TTransportException

from baseplate.lib import config
from baseplate.lib import warn_deprecated
from baseplate.lib.retry import RetryPolicy


logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    ProtocolPool = queue.Queue[TProtocolBase]  # pylint: disable=unsubscriptable-object
else:
    ProtocolPool = queue.Queue


def _make_transport(endpoint: config.EndpointConfiguration) -> TSocket:
    if endpoint.family == socket.AF_INET:
        trans = TSocket(*endpoint.address)
    elif endpoint.family == socket.AF_UNIX:
        trans = TSocket(unix_socket=endpoint.address)
    else:
        raise Exception(f"unsupported endpoint family {endpoint.family!r}")

    return trans


def thrift_pool_from_config(
    app_config: config.RawConfig, prefix: str, **kwargs: Any
) -> "ThriftConnectionPool":
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
        replaced with a new one. Written as a
        :py:func:`~baseplate.lib.config.Timespan` e.g. ``1 minute``.
    * ``timeout``: The maximum amount of time a connection attempt or RPC call
        can take before a TimeoutError is raised.
        (:py:func:`~baseplate.lib.config.Timespan`)
    * ``max_connection_attempts``: The maximum number of times the pool will attempt to
        open a connection.

    .. versionchanged:: 1.2
        ``max_retries`` was renamed ``max_connection_attempts``.

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {
            "endpoint": config.Endpoint,
            "size": config.Optional(config.Integer, default=10),
            "max_age": config.Optional(config.Timespan, default=config.Timespan("1 minute")),
            "timeout": config.Optional(config.Timespan, default=config.Timespan("1 second")),
            "max_connection_attempts": config.Optional(config.Integer),
            "max_retries": config.Optional(config.Integer),
        }
    )
    options = parser.parse(prefix[:-1], app_config)

    if options.size is not None:
        kwargs.setdefault("size", options.size)
    if options.max_age is not None:
        kwargs.setdefault("max_age", options.max_age.total_seconds())
    if options.timeout is not None:
        kwargs.setdefault("timeout", options.timeout.total_seconds())
    if options.max_connection_attempts is not None:
        kwargs.setdefault("max_connection_attempts", options.max_connection_attempts)
    if options.max_retries is not None:
        kwargs.setdefault("max_retries", options.max_retries)

    return ThriftConnectionPool(endpoint=options.endpoint, **kwargs)


class ThriftConnectionPool:
    """A pool that maintains a queue of open Thrift connections.

    :param endpoint: The remote address
        of the Thrift service.
    :param size: The maximum number of connections that can be open
        before new attempts to open block.
    :param max_age: The maximum number of seconds a connection should be
        kept alive. Connections older than this will be reaped.
    :param timeout: The maximum number of seconds a connection attempt or
        RPC call can take before a TimeoutError is raised.
    :param max_connection_attempts: The maximum number of times the pool will attempt
        to open a connection.
    :param protocol_factory: The factory to use for creating protocols from
        transports. This is useful for talking to services that don't support
        THeaderProtocol.

    All exceptions raised by this class derive from
    :py:exc:`~thrift.transport.TTransport.TTransportException`.

    .. versionchanged:: 1.2
        ``max_retries`` was renamed ``max_connection_attempts``.

    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        endpoint: config.EndpointConfiguration,
        size: int = 10,
        max_age: int = 120,
        timeout: int = 1,
        max_connection_attempts: Optional[int] = None,
        max_retries: Optional[int] = None,
        protocol_factory: TProtocolFactory = THeaderProtocol.THeaderProtocolFactory(),
    ):
        if max_connection_attempts and max_retries:
            raise Exception("do not mix max_retries and max_connection_attempts")

        if max_retries:
            warn_deprecated(
                "ThriftConnectionPool's max_retries is now named max_connection_attempts"
            )
            max_connection_attempts = max_retries
        elif not max_connection_attempts:
            max_connection_attempts = 3

        self.endpoint = endpoint
        self.max_age = max_age
        self.retry_policy = RetryPolicy.new(attempts=max_connection_attempts)
        self.timeout = timeout
        self.protocol_factory = protocol_factory

        self.size = size
        self.pool: ProtocolPool = queue.LifoQueue()
        for _ in range(size):
            self.pool.put(None)

    def _get_from_pool(self) -> Optional[TProtocolBase]:
        try:
            return self.pool.get(block=True, timeout=self.timeout)
        except queue.Empty:
            raise TTransportException(
                type=TTransportException.NOT_OPEN, message="timed out waiting for a connection slot"
            )

    def _create_connection(self) -> TProtocolBase:
        for _ in self.retry_policy:
            trans = _make_transport(self.endpoint)
            trans.setTimeout(self.timeout * 1000.0)
            prot = self.protocol_factory.getProtocol(trans)

            try:
                prot.trans.open()
            except TTransportException as exc:
                logger.info("Failed to connect to %r: %s", self.endpoint, exc)
                continue

            prot.baseplate_birthdate = time.time()

            return prot

        raise TTransportException(
            type=TTransportException.NOT_OPEN,
            message="giving up after multiple attempts to connect",
        )

    def _is_stale(self, prot: TProtocolBase) -> bool:
        if not prot.trans.isOpen() or time.time() - prot.baseplate_birthdate > self.max_age:
            prot.trans.close()
            return True
        return False

    def _release(self, prot: Optional[TProtocolBase]) -> None:
        if prot and prot.trans.isOpen():
            self.pool.put(prot)
        else:
            self.pool.put(None)

    @contextlib.contextmanager
    def connection(self) -> Generator[TProtocolBase, None, None]:
        """Acquire a connection from the pool.

        This method is to be used with a context manager. It returns a
        connection from the pool, or blocks up to :attr:`timeout` seconds
        waiting for one if the pool is full and all connections are in use.

        When the context is exited, the connection is returned to the pool.
        However, if it was exited via an unexpected Thrift exception, the
        connection is closed instead because the state of the connection is
        unknown.

        """
        prot = self._get_from_pool()

        try:
            if not prot or self._is_stale(prot):
                prot = self._create_connection()

            try:
                yield prot
            except socket.timeout:
                # thrift doesn't re-wrap socket timeout errors appropriately so
                # we'll do it here for a saner exception hierarchy
                raise TTransportException(
                    type=TTransportException.TIMED_OUT, message="timed out interacting with socket"
                )
            except socket.error as exc:
                raise TTransportException(type=TTransportException.UNKNOWN, message=str(exc))
        except (TApplicationException, TProtocolException, TTransportException):
            # these exceptions usually indicate something low-level went wrong,
            # so it's safest to just close this connection because we don't
            # know what state it's in.
            if prot:
                prot.trans.close()
            raise
        except TException:
            # the only other TException-derived errors are application level
            # (expected) errors which should be safe for the connection.
            # don't close the transport here!
            raise
        except:  # noqa: E722
            # anything else coming out of thrift usually means parsing failed
            # or something nastier. we'll just play it safe and close the
            # connection.
            if prot:
                prot.trans.close()
            raise
        finally:
            self._release(prot)

    @property
    def checkedout(self) -> int:
        return self.size - self.pool.qsize()
