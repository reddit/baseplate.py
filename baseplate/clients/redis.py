import random
import time

from collections import defaultdict
from math import ceil
from typing import Any
from typing import Dict
from typing import Optional

import redis

# redis.client.StrictPipeline was renamed to redis.client.Pipeline in version 3.0
try:
    from redis.client import StrictPipeline as Pipeline  # type: ignore
except ImportError:
    from redis.client import Pipeline

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib import message_queue
from baseplate.lib import metrics


# Not complete, but covers the major ones
# https://redis.io/commands
# TODO(Fran): Currently this is just a list of known read commands, technically
#   we can add any command we consider idempotent to the list (such as SETNX) but
#   I feel like "we only retry read commands" is easier to reason about initially.
RETRIABLE_COMMANDS = frozenset(
    [
        "BITCOUNT",
        "BITPOS",
        "EXISTS",
        "GEODIST",
        "GEOHASH",
        "GEOPOS",
        "GEORADIUS",
        "GEORADIUSBYMEMBER",
        "GET",
        "GETBIT",
        "GETRANGE",
        "HEXISTS",
        "HGET",
        "HGETALL",
        "HKEYS",
        "HLEN",
        "HMGET",
        "HSTRLEN",
        "HVALS",
        "KEYS",
        "LINDEX",
        "LLEN",
        "LRANGE",
        "MGET",
        "PTTL",
        "RANDOMKEY",
        "SCARD",
        "SDIFF",
        "SINTER",
        "SISMEMBER",
        "SMEMBERS",
        "SRANDMEMBER",
        "STRLEN",
        "SUNION",
        "TTL",
        "ZCARD",
        "ZCOUNT",
        "ZRANGE",
        "ZSCORE",
    ]
)


def pool_from_config(
    app_config: config.RawConfig, prefix: str = "redis.", **kwargs: Any
) -> redis.ConnectionPool:
    """Make a ConnectionPool from a configuration dictionary.

    The keys useful to :py:func:`pool_from_config` should be prefixed, e.g.
    ``redis.url``, ``redis.max_connections``, etc. The ``prefix`` argument
    specifies the prefix used to filter keys.  Each key is mapped to a
    corresponding keyword argument on the :py:class:`redis.ConnectionPool`
    constructor.

    Supported keys:

    * ``url`` (required): a URL like ``redis://localhost/0``.
    * ``max_connections``: an integer maximum number of connections in the pool
    * ``socket_connect_timeout``: how long to wait for sockets to connect. e.g.
        ``200 milliseconds`` (:py:func:`~baseplate.lib.config.Timespan`)
    * ``socket_timeout``: how long to wait for socket operations, e.g.
        ``200 milliseconds`` (:py:func:`~baseplate.lib.config.Timespan`)

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {
            "url": config.String,
            "max_connections": config.Optional(config.Integer, default=None),
            "socket_connect_timeout": config.Optional(config.Timespan, default=None),
            "socket_timeout": config.Optional(config.Timespan, default=None),
            "enable_retries": config.Optional(config.Boolean, default=True),
        }
    )
    options = parser.parse(prefix[:-1], app_config)

    if options.max_connections is not None:
        kwargs.setdefault("max_connections", options.max_connections)
    if options.socket_connect_timeout is not None:
        kwargs.setdefault(
            "socket_connect_timeout", options.socket_connect_timeout.total_seconds()
        )
    if options.socket_timeout is not None:
        kwargs.setdefault("socket_timeout", options.socket_timeout.total_seconds())

    pool = redis.BlockingConnectionPool.from_url(options.url, **kwargs)
    setattr(pool, "redis_enable_retries", options.enable_retries)

    return pool


class RedisClient(config.Parser):
    """Configure a Redis client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`pool_from_config` for available configuration settings.

    """

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs

    def parse(
        self, key_path: str, raw_config: config.RawConfig
    ) -> "RedisContextFactory":
        connection_pool = pool_from_config(raw_config, f"{key_path}.", **self.kwargs)
        return RedisContextFactory(connection_pool)


class RedisContextFactory(ContextFactory):
    """Redis client context factory.

    This factory will attach a
    :py:class:`~baseplate.clients.redis.MonitoredRedisConnection` to an
    attribute on the :py:class:`~baseplate.RequestContext`. When Redis commands
    are executed via this connection object, they will use connections from the
    provided :py:class:`redis.ConnectionPool` and automatically record
    diagnostic information.

    :param connection_pool: A connection pool.

    """

    def __init__(self, connection_pool: redis.ConnectionPool):
        self.connection_pool = connection_pool

    def report_runtime_metrics(self, batch: metrics.Client) -> None:
        if not isinstance(self.connection_pool, redis.BlockingConnectionPool):
            return

        size = self.connection_pool.max_connections
        open_connections = len(self.connection_pool._connections)  # type: ignore
        available = self.connection_pool.pool.qsize()
        in_use = size - available

        batch.gauge("pool.size").replace(size)
        batch.gauge("pool.in_use").replace(in_use)
        batch.gauge("pool.open_and_available").replace(open_connections - in_use)

        retry_stats = RetryStats.get_retry_stats(self.connection_pool)
        for event_name, value in retry_stats.items():
            batch.counter(event_name).increment(value)

    def make_object_for_context(
        self, name: str, span: Span
    ) -> "MonitoredRedisConnection":
        kwargs = {}
        if hasattr(self.connection_pool, "redis_enable_retries"):
            kwargs["enable_retries"] = getattr(
                self.connection_pool, "redis_enable_retries"
            )
        return MonitoredRedisConnection(name, span, self.connection_pool, **kwargs)


class PipelineWithRetry(Pipeline):
    """Subclass of Pipeline that will automatically retry pipelines with only read commands."""

    def __init__(  # type: ignore
        self, connection_pool, response_callbacks, transaction, shard_hint, retriable
    ) -> None:
        super().__init__(
            connection_pool=connection_pool,
            response_callbacks=response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        self.is_retriable_pipeline = retriable

    def _execute_pipeline(self, connection, commands, raise_on_error):  # type: ignore
        retries = 0
        MAX_RETRIES = 10
        MAX_RETRY_WAIT_MS = 50

        cmd_list = [args[0] for args, _ in commands]
        is_read_only = all(cmd in RETRIABLE_COMMANDS for cmd in cmd_list)

        while retries < MAX_RETRIES:
            try:
                return super()._execute_pipeline(connection, commands, raise_on_error)
            except Exception:
                if self.is_retriable_pipeline and is_read_only:
                    retries += 1
                    if retries == MAX_RETRIES:
                        raise
                else:
                    raise
                RetryStats.increase_retry_stats(self.connection_pool, "pipeline")
                time.sleep(random.randint(0, MAX_RETRY_WAIT_MS) / 1000)


class RedisWithRetry(redis.StrictRedis):
    """
    Redis client subclass with ability to retry commands.

    The behavior of this class is identical to that of Redis, with the only
    difference that this class will attempt to retry a failed command if the
    failure belongs to a subset of known "safe to retry" errors
    """

    enable_retries = True

    def _is_retriable_error(self, e: Exception) -> bool:
        """
        Return True if exception e is one that we want to retry for.

        There are certain types of errors that we want to retry on the
        client side, but unfortunately it's not as simple as filtering
        for one particular exception type. A given exception type
        (such as ConnectionError) can mean anything from a connection
        closed to a Redis instance unavailable because it's loading its
        dataset in memory, so we need to be able to check the instance
        type *and* the exception message before we decide if we should
        retry it.

        If we want to always retry one particular exception type, setting
        its list of messages to an empty list will do it.
        """
        if not self.enable_retries:
            return False

        retriable_errors = {
            # "Redis is loading the dataset in memory" is technically a BusyLoadingError,
            # but that's a subclass of ConnectionError so this grouping is technically correct.
            # The reason we don't explicitly catch BusyLoadingError here is that when using
            # hiredis this exception is somehow only catchable as ConnectionError, but
            # if hiredis is not present you can choose to catch it as either.
            # Doing it as ConnectionError will just work for both, so we do that.
            redis.exceptions.ConnectionError: [
                "Connection closed by server",
                "Connection reset by peer",
                "Connection refused",
                "Redis is loading the dataset in memory",
            ],
            # upstream failures can happen when using Envoy as a proxy.
            # If we retry the proxy will eventually get us a healthy upstream host
            redis.exceptions.ResponseError: ["upstream failure", "no upstream host"],
        }

        for error_type, message_list in retriable_errors.items():
            if isinstance(e, error_type):
                if not message_list or any(err in str(e) for err in message_list):
                    return True

        return False

    def execute_command(self, *args: Any, **options: Any) -> Any:
        """Execute a command and return a parsed response."""
        retries = 0
        MAX_RETRIES = 25
        MAX_RETRY_WAIT_MS = 25
        while retries < MAX_RETRIES:
            try:
                return super().execute_command(*args, **options)
            except Exception as e:
                if self._is_retriable_error(e):
                    retries += 1
                    if retries == MAX_RETRIES:
                        raise
                else:
                    raise

                RetryStats.increase_retry_stats(self.connection_pool, args[0])
                time.sleep(random.randint(0, MAX_RETRY_WAIT_MS) / 1000)


# pylint: disable=too-many-public-methods
class MonitoredRedisConnection(RedisWithRetry):
    """Redis connection that collects diagnostic information.

    This connection acts like :py:class:`redis.StrictRedis` except that all
    operations are automatically wrapped with diagnostic collection.

    The interface is the same as that class except for the
    :py:meth:`~baseplate.clients.redis.MonitoredRedisConnection.pipeline`
    method.

    We take an enable_retries argument in case we need to disable the optional
    retry behavior.

    """

    def __init__(
        self,
        context_name: str,
        server_span: Span,
        connection_pool: redis.ConnectionPool,
        enable_retries: bool = True,
    ):
        self.context_name = context_name
        self.server_span = server_span

        super().__init__(connection_pool=connection_pool)

        self.enable_retries = enable_retries

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        command = args[0]
        trace_name = f"{self.context_name}.{command}"

        with self.server_span.make_child(trace_name):
            return super().execute_command(command, *args[1:], **kwargs)

    # pylint: disable=arguments-differ
    def pipeline(  # type: ignore
        self,
        name: str,
        transaction: bool = False,
        shard_hint: Optional[str] = None,
        retriable: Optional[bool] = None,
    ) -> "MonitoredRedisPipeline":
        """Create a pipeline.

        This returns an object on which you can call the standard Redis
        commands. Execution will be deferred until ``execute`` is called. This
        is useful for saving round trips.

        :param name: The name to attach to diagnostics for this pipeline.
        :param transaction: Whether or not the commands in the pipeline
            are wrapped with a transaction and executed atomically.
            Disabled by default.

        :param retriable: When an exception happens executing a pipeline,
            whether to retry. A pipeline will only be retried if it only
            contains read commands.
        """
        # By default we will retry pipelines if we already want to retry single commands
        if retriable is None:
            retriable = self.enable_retries

        return MonitoredRedisPipeline(
            f"{self.context_name}.pipeline_{name}",
            self.server_span,
            self.connection_pool,
            self.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
            retriable=retriable,
        )

    # these commands are not yet implemented, but probably not unimplementable
    def transaction(self, *args: Any, **kwargs: Any) -> Any:
        """Not currently implemented."""
        raise NotImplementedError


class MonitoredRedisPipeline(PipelineWithRetry):
    def __init__(
        self,
        trace_name: str,
        server_span: Span,
        connection_pool: redis.ConnectionPool,
        response_callbacks: Dict,
        **kwargs: Any,
    ):
        self.trace_name = trace_name
        self.server_span = server_span
        super().__init__(connection_pool, response_callbacks, **kwargs)

    # pylint: disable=arguments-differ
    def execute(self, **kwargs: Any) -> Any:
        with self.server_span.make_child(self.trace_name):
            return super().execute(**kwargs)


class MessageQueue:
    """A Redis-backed variant of :py:class:`~baseplate.lib.message_queue.MessageQueue`.

    :param name: can be any string.

    :param client: should be a :py:class:`redis.ConnectionPool` or
           :py:class:`redis.BlockingConnectionPool` from which a client
           connection can be created from (preferably generated from the
           :py:func:`pool_from_config` helper).

    """

    def __init__(self, name: str, client: redis.ConnectionPool):
        self.queue = name
        if isinstance(client, (redis.BlockingConnectionPool, redis.ConnectionPool)):
            self.client = RedisWithRetry(connection_pool=client)
        else:
            self.client = client

    def get(self, timeout: Optional[float] = None) -> bytes:
        """Read a message from the queue.

        :param timeout: If the queue is empty, the call will block up to
            ``timeout`` seconds or forever if ``None``, if a float is given,
            it will be rounded up to be an integer
        :raises: :py:exc:`~baseplate.lib.message_queue.TimedOutError` The queue
            was empty for the allowed duration of the call.

        """
        if isinstance(timeout, float):
            timeout = int(ceil(timeout))

        if timeout == 0:
            message = self.client.lpop(self.queue)
        else:
            message = self.client.blpop(self.queue, timeout=timeout or 0)

            if message:
                message = message[1]

        if not message:
            raise message_queue.TimedOutError

        return message

    def put(  # pylint: disable=unused-argument
        self, message: bytes, timeout: Optional[float] = None
    ) -> None:
        """Add a message to the queue.

        :param message: will be typecast to a string upon storage and will come
               out of the queue as a string regardless of what type they are
               when passed into this method.
        """
        self.client.rpush(self.queue, message)

    def unlink(self) -> None:
        """Not implemented for Redis variant."""

    def close(self) -> None:
        """Close queue when finished.

        Will delete the queue from the Redis server (Note, can still enqueue
        and dequeue as the actions will recreate the queue)
        """
        self.client.delete(self.queue)


class RetryStats:
    """
    Helper class to track some retry stats inside a Redis connection_pool object.

    We have to smuggle the stats inside a connection_pool object because it's the
    only part of the Redis client that persists across different calls.
    """

    @staticmethod
    def reset_retry_stats(connection_pool: redis.ConnectionPool) -> None:
        setattr(connection_pool, "retry_stats", defaultdict(lambda: 0))

    @classmethod
    def increase_retry_stats(
        cls, connection_pool: redis.ConnectionPool, event_name: str, delta: int = 1
    ) -> None:
        if not hasattr(connection_pool, "retry_stats"):
            cls.reset_retry_stats(connection_pool)

        stats = getattr(connection_pool, "retry_stats")
        stats[event_name] += 1

    @classmethod
    def get_retry_stats(cls, connection_pool: redis.ConnectionPool) -> Dict[Any, Any]:
        if not hasattr(connection_pool, "retry_stats"):
            cls.reset_retry_stats(connection_pool)

        return getattr(connection_pool, "retry_stats")
