from math import ceil
from time import perf_counter
from typing import Any
from typing import Dict
from typing import Optional

import redis

# redis.client.StrictPipeline was renamed to redis.client.Pipeline in version 3.0
try:
    from redis.client import StrictPipeline as Pipeline  # type: ignore
except ImportError:
    from redis.client import Pipeline

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib import message_queue
from baseplate.lib import metrics

from baseplate.lib.prometheus_metrics import default_latency_buckets

PROM_PREFIX = "redis_client"
PROM_LABELS_PREFIX = "redis"

PROM_SHARED_LABELS = [
    f"{PROM_LABELS_PREFIX}_command",
    f"{PROM_LABELS_PREFIX}_database",
    f"{PROM_LABELS_PREFIX}_client_name",
    f"{PROM_LABELS_PREFIX}_type",
]
LATENCY_SECONDS = Histogram(
    f"{PROM_PREFIX}_latency_seconds",
    "Latency histogram for calls made by clients",
    [*PROM_SHARED_LABELS, f"{PROM_LABELS_PREFIX}_success"],
    buckets=default_latency_buckets,
)

REQUESTS_TOTAL = Counter(
    f"{PROM_PREFIX}_requests_total",
    "Total number of requests made by client",
    [*PROM_SHARED_LABELS, f"{PROM_LABELS_PREFIX}_success"],
)

ACTIVE_REQUESTS = Gauge(
    f"{PROM_PREFIX}_active_requests",
    "Number of active requests for a given client",
    PROM_SHARED_LABELS,
)

PROM_POOL_PREFIX = f"{PROM_PREFIX}_pool"
PROM_LABELS = ["redis_pool"]

MAX_CONNECTIONS = Gauge(
    f"{PROM_POOL_PREFIX}_max_size",
    "Maximum number of connections allowed in this redis client connection pool",
    PROM_LABELS,
)
IDLE_CONNECTIONS = Gauge(
    f"{PROM_POOL_PREFIX}_idle_connections",
    "Number of idle connections in this redis client connection pool",
    PROM_LABELS,
)
OPEN_CONNECTIONS = Gauge(
    f"{PROM_POOL_PREFIX}_active_connections",
    "Number of open connections in this redis client connection pool",
    PROM_LABELS,
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
        }
    )
    options = parser.parse(prefix[:-1], app_config)

    if options.max_connections is not None:
        kwargs.setdefault("max_connections", options.max_connections)
    if options.socket_connect_timeout is not None:
        kwargs.setdefault("socket_connect_timeout", options.socket_connect_timeout.total_seconds())
    if options.socket_timeout is not None:
        kwargs.setdefault("socket_timeout", options.socket_timeout.total_seconds())

    return redis.BlockingConnectionPool.from_url(options.url, **kwargs)


class RedisClient(config.Parser):
    """Configure a Redis client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`pool_from_config` for available configuration settings.

    """

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "RedisContextFactory":
        connection_pool = pool_from_config(raw_config, f"{key_path}.", **self.kwargs)
        return RedisContextFactory(connection_pool, key_path)


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

    def __init__(self, connection_pool: redis.ConnectionPool, name: str = "redis"):
        self.connection_pool = connection_pool
        self.name = name

    def report_runtime_metrics(self, batch: metrics.Client) -> None:
        if not isinstance(self.connection_pool, redis.BlockingConnectionPool):
            return

        size = self.connection_pool.max_connections
        open_connections_num = len(self.connection_pool._connections)  # type: ignore
        available = self.connection_pool.pool.qsize()
        in_use = size - available

        MAX_CONNECTIONS.labels(self.name).set(size)
        IDLE_CONNECTIONS.labels(self.name).set(available)
        OPEN_CONNECTIONS.labels(self.name).set(open_connections_num)

        batch.gauge("pool.size").replace(size)
        batch.gauge("pool.in_use").replace(in_use)
        batch.gauge("pool.open_and_available").replace(open_connections_num - in_use)

    def make_object_for_context(self, name: str, span: Span) -> "MonitoredRedisConnection":
        return MonitoredRedisConnection(name, span, self.connection_pool)


# pylint: disable=too-many-public-methods
class MonitoredRedisConnection(redis.StrictRedis):
    """Redis connection that collects diagnostic information.

    This connection acts like :py:class:`redis.StrictRedis` except that all
    operations are automatically wrapped with diagnostic collection.

    The interface is the same as that class except for the
    :py:meth:`~baseplate.clients.redis.MonitoredRedisConnection.pipeline`
    method.

    """

    def __init__(self, context_name: str, server_span: Span, connection_pool: redis.ConnectionPool):
        self.context_name = context_name
        self.server_span = server_span

        super().__init__(connection_pool=connection_pool)

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        command = args[0]
        trace_name = f"{self.context_name}.{command}"

        labels = {
            f"{PROM_LABELS_PREFIX}_command": command,
            f"{PROM_LABELS_PREFIX}_client_name": self.connection_pool.connection_kwargs.get(
                "client_name", ""
            ),
            f"{PROM_LABELS_PREFIX}_database": self.connection_pool.connection_kwargs.get("db", ""),
            f"{PROM_LABELS_PREFIX}_type": "standalone",
        }
        with self.server_span.make_child(trace_name), ACTIVE_REQUESTS.labels(
            **labels
        ).track_inprogress():
            start_time = perf_counter()
            success = "true"

            try:
                res = super().execute_command(command, *args[1:], **kwargs)
                if isinstance(res, redis.RedisError):
                    success = "false"
                return res
            except:  # noqa: E722
                success = "false"
                raise
            finally:
                result_labels = {**labels, f"{PROM_LABELS_PREFIX}_success": success}
                REQUESTS_TOTAL.labels(**result_labels).inc()
                LATENCY_SECONDS.labels(**result_labels).observe(perf_counter() - start_time)

    # pylint: disable=arguments-differ
    def pipeline(  # type: ignore
        self, name: str, transaction: bool = True, shard_hint: Optional[str] = None
    ) -> "MonitoredRedisPipeline":
        """Create a pipeline.

        This returns an object on which you can call the standard Redis
        commands. Execution will be deferred until ``execute`` is called. This
        is useful for saving round trips.

        :param name: The name to attach to diagnostics for this pipeline.
        :param transaction: Whether or not the commands in the pipeline
            are wrapped with a transaction and executed atomically.

        """
        return MonitoredRedisPipeline(
            f"{self.context_name}.pipeline_{name}",
            self.server_span,
            self.connection_pool,
            self.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )

    # these commands are not yet implemented, but probably not unimplementable
    def transaction(self, *args: Any, **kwargs: Any) -> Any:
        """Not currently implemented."""
        raise NotImplementedError


class MonitoredRedisPipeline(Pipeline):
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
            success = "true"
            start_time = perf_counter()
            labels = {
                f"{PROM_LABELS_PREFIX}_command": "pipeline",
                f"{PROM_LABELS_PREFIX}_client_name": self.connection_pool.connection_kwargs.get(
                    "client_name", ""
                ),
                f"{PROM_LABELS_PREFIX}_database": self.connection_pool.connection_kwargs.get(
                    "db", ""
                ),
                f"{PROM_LABELS_PREFIX}_type": "standalone",
            }

            ACTIVE_REQUESTS.labels(**labels).inc()

            try:
                return super().execute(**kwargs)
            except:  # noqa: E722
                success = "false"
                raise
            finally:
                ACTIVE_REQUESTS.labels(**labels).dec()
                result_labels = {
                    **labels,
                    f"{PROM_LABELS_PREFIX}_success": success,
                }
                REQUESTS_TOTAL.labels(**result_labels).inc()
                LATENCY_SECONDS.labels(**result_labels).observe(perf_counter() - start_time)


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
            self.client = redis.Redis(connection_pool=client)
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
