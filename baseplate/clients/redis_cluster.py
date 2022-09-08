import logging
import random

from datetime import timedelta
from time import perf_counter
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import rediscluster

from redis import RedisError
from rediscluster.pipeline import ClusterPipeline

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.clients.redis import ACTIVE_REQUESTS
from baseplate.clients.redis import LATENCY_SECONDS
from baseplate.clients.redis import MAX_CONNECTIONS
from baseplate.clients.redis import OPEN_CONNECTIONS
from baseplate.clients.redis import PROM_LABELS_PREFIX
from baseplate.clients.redis import REQUESTS_TOTAL
from baseplate.lib import config
from baseplate.lib import metrics

logger = logging.getLogger(__name__)
randomizer = random.SystemRandom()


# Read commands that take a single key as their first parameter
SINGLE_KEY_READ_COMMANDS = frozenset(
    [
        "BITCOUNT",
        "BITPOS",
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
        "LINDEX",
        "LLEN",
        "LRANGE",
        "PTTL",
        "SCARD",
        "SISMEMBER",
        "SMEMBERS",
        "SRANDMEMBER",
        "STRLEN",
        "TTL",
        "ZCARD",
        "ZCOUNT",
        "ZRANGE",
        "ZSCORE",
    ]
)

# Read commands that take a list of keys as parameters
MULTI_KEY_READ_COMMANDS = frozenset(["EXISTS", "MGET", "SDIFF", "SINTER", "SUNION"])

# Write commands that take a single key as their first parameter.
SINGLE_KEY_WRITE_COMMANDS = frozenset(
    [
        "EXPIRE",
        "EXPIREAT",
        "HINCRBY",
        "HINCRBYFLOAT",
        "HDEL",
        "HMSET",
        "HSET",
        "HSETNX",
        "LPUSH",
        "LREM",
        "LPOP",
        "LSET",
        "LTRIM",
        "RPOP",
        "RPUSH",
        "SADD",
        "SET",
        "SETNX",
        "SPOP",
        "SREM",
        "ZADD",
        "ZINCRBY",
        "ZPOPMAX",
        "ZPOPMIN",
        "ZREM",
        "ZREMRANGEBYLEX",
        "ZREMRANGEBYRANK",
        "ZREMRANGEBYSCORE",
    ]
)

# Write commands that take a list of keys as argument
MULTI_KEY_WRITE_COMMANDS = frozenset(["DEL"])

# These are a special case of multi-key write commands that take arguments in the form
#  of key value [key value ...]
MULTI_KEY_BATCH_WRITE_COMMANDS = frozenset(["MSET", "MSETNX"])


class HotKeyTracker:
    """
    HotKeyTracker can be used to help identify hot keys within Redis.

    Helper class that can be used to track our key usage and identify hot keys within
    Redis. Whenever we send a read command to Redis we have a (very low but configurable)
    chance to increase a counter associated with that key in Redis. Over time this should
    allow us to find keys that are disproportionaly represented by querying the sorted
    set "baseplate-hot-key-tracker-reads" in Redis. A same sorted set by the name of
    "baseplate-hot-key-tracker-writes" will be used to track write frequency.

    Both read and writes tracking have different configurable percentages, which means
    we can enable tracking for reads without enabling it for writes or have different
    percentages for them, which is useful when the number of reads is much higher than
    the number of writes to a cluster.

    This feature can be turned off by setting the tracking percentage to zero, and should
    probably only be enabled if we're actively debugging an issue or looking for a regression.

    The "baseplate-hot-key-tracker-reads" will have a TTL of 24 hours to ensure that
    older key counts don't interfere with new debugging sessions. This means that the
    sorted set and its contents will disappear in 24 hours after this feature is disabled
    and we stopped writing to it.
    """

    def __init__(
        self,
        redis_client: rediscluster.RedisCluster,
        track_reads_sample_rate: float,
        track_writes_sample_rate: float,
    ):
        self.redis_client = redis_client
        self.track_reads_sample_rate = track_reads_sample_rate
        self.track_writes_sample_rate = track_writes_sample_rate

        self.reads_sorted_set_name = "baseplate-hot-key-tracker-reads"
        self.writes_sorted_set_name = "baseplate-hot-key-tracker-writes"

    def should_track_key_reads(self) -> bool:
        return randomizer.random() < self.track_reads_sample_rate

    def should_track_key_writes(self) -> bool:
        return randomizer.random() < self.track_writes_sample_rate

    def increment_keys_read_counter(self, key_list: List[str], ignore_errors: bool = True) -> None:
        self._increment_hot_key_counter(key_list, self.reads_sorted_set_name, ignore_errors)

    def increment_keys_written_counter(
        self, key_list: List[str], ignore_errors: bool = True
    ) -> None:
        self._increment_hot_key_counter(key_list, self.writes_sorted_set_name, ignore_errors)

    def _increment_hot_key_counter(
        self, key_list: List[str], set_name: str, ignore_errors: bool = True
    ) -> None:
        if len(key_list) == 0:
            return

        try:
            with self.redis_client.pipeline(set_name) as pipe:
                for key in key_list:
                    pipe.zincrby(set_name, 1, key)
                # Reset the TTL for the sorted set
                pipe.expire(set_name, timedelta(hours=24))
                pipe.execute()
        except Exception as e:
            # We don't want to disrupt this request even if key tracking fails, so just
            # log it.
            logger.exception(e)
            if not ignore_errors:
                raise

    def maybe_track_key_usage(self, args: List[str]) -> None:
        """Probabilistically track usage of the keys in this command.

        If we have enabled key usage tracing *and* this command is withing the
        percentage of commands we want to track, then write it to a sorted set
        so we can keep track of the most accessed keys.
        """
        if len(args) == 0:
            return

        command = args[0]

        if self.should_track_key_reads():
            if command in SINGLE_KEY_READ_COMMANDS:
                self.increment_keys_read_counter([args[1]])
            elif command in MULTI_KEY_READ_COMMANDS:
                self.increment_keys_read_counter(args[1:])

        if self.should_track_key_writes():
            if command in SINGLE_KEY_WRITE_COMMANDS:
                self.increment_keys_written_counter([args[1]])
            elif command in MULTI_KEY_WRITE_COMMANDS:
                self.increment_keys_written_counter(args[1:])
            elif command in MULTI_KEY_BATCH_WRITE_COMMANDS:
                # These commands follow key value [key value...] format
                self.increment_keys_written_counter(args[1::2])


# We want to be able to combine blocking behaviour with the ability to read from replicas
# Unfortunately this is not provide as-is so we combine two connection pool classes to provide
# the desired behaviour.
class ClusterWithReadReplicasBlockingConnectionPool(rediscluster.ClusterBlockingConnectionPool):
    # pylint: disable=arguments-differ
    def get_node_by_slot(self, slot: int, read_command: bool = False) -> Dict[str, Any]:
        """Get a node from the slot.

        If the command is a read command we'll try to return a random node.
        If there are no replicas or this isn't a read command we'll return the primary.
        """
        if read_command:
            return random.choice(self.nodes.slots[slot])

        # This isn't a read command, so return the primary (first node)
        return self.nodes.slots[slot][0]


def cluster_pool_from_config(
    app_config: config.RawConfig, prefix: str = "rediscluster.", **kwargs: Any
) -> rediscluster.ClusterConnectionPool:
    """Make a ClusterConnectionPool from a configuration dictionary.

    The keys useful to :py:func:`cluster_pool_from_config` should be prefixed, e.g.
    ``rediscluster.url``, ``rediscluster.max_connections``, etc. The ``prefix`` argument
    specifies the prefix used to filter keys.  Each key is mapped to a
    corresponding keyword argument on the :py:class:`rediscluster.ClusterConnectionPool`
    constructor.

    Supported keys:

    * ``url`` (required): a URL like ``redis://localhost/0``.
    * ``max_connections``: an integer maximum number of connections in the pool
    * ``max_connections_per_node``: Boolean, whether max_connections should be applied
        globally (False) or per node (True).
    * ``skip_full_coverage_check``: Skips the check of cluster-require-full-coverage
      config, useful for clusters without the CONFIG command (like aws)
    * ``nodemanager_follow_cluster``: Tell the node manager to reuse the last set of
      nodes it was operating on when intializing.
    * ``read_from_replicas``: (Boolean) Whether the client should send all read queries to
        replicas instead of just the primary
    * ``timeout``: . e.g. ``200 milliseconds`` (:py:func:`~baseplate.lib.config.Timespan`).
        How long to wait for a connection to become available.  Additionally, will set
        ``socket_connect_timeout`` and ``socket_timeout`` if they're not set explicitly.
    * ``socket_connect_timeout``: e.g. ``200 milliseconds`` (:py:func:`~baseplate.lib.config.Timespan`)
        How long to wait for sockets to connect.
    * ``socket_timeout``: e.g. ``200 milliseconds`` (:py:func:`~baseplate.lib.config.Timespan`)
        How long to wait for socket operations.
    * ``track_key_reads_sample_rate``: If greater than zero, which percentage of requests will
        be inspected to keep track of hot key usage within Redis when reading.
        Every command inspected will result in a write to a sorted set
        (baseplate-hot-key-tracker-reads) for tracking.
    * ``track_key_writes_sample_rate``: If greater than zero, which percentage of requests will
        be inspected to keep track of hot key usage within Redis when writing.
        Every command inspected will result in a write to a sorted set
        (baseplate-hot-key-tracker-reads) for tracking.

    """
    assert prefix.endswith(".")

    parser = config.SpecParser(
        {
            "url": config.String,
            "max_connections": config.Optional(config.Integer, default=50),
            "max_connections_per_node": config.Optional(config.Boolean, default=False),
            "socket_connect_timeout": config.Optional(config.Timespan, default=None),
            "socket_timeout": config.Optional(config.Timespan, default=None),
            "timeout": config.Optional(config.Timespan, default=None),
            "read_from_replicas": config.Optional(config.Boolean, default=True),
            "skip_full_coverage_check": config.Optional(config.Boolean, default=True),
            "nodemanager_follow_cluster": config.Optional(config.Boolean, default=None),
            "decode_responses": config.Optional(config.Boolean, default=True),
            "track_key_reads_sample_rate": config.Optional(config.Float, default=0),
            "track_key_writes_sample_rate": config.Optional(config.Float, default=0),
        }
    )

    options = parser.parse(prefix[:-1], app_config)

    # We're explicitly setting a default here because of https://github.com/Grokzen/redis-py-cluster/issues/435
    kwargs.setdefault("max_connections", options.max_connections)

    kwargs.setdefault("decode_responses", options.decode_responses)

    if options.nodemanager_follow_cluster is not None:
        kwargs.setdefault("nodemanager_follow_cluster", options.nodemanager_follow_cluster)
    if options.skip_full_coverage_check is not None:
        kwargs.setdefault("skip_full_coverage_check", options.skip_full_coverage_check)
    if options.socket_connect_timeout is not None:
        kwargs.setdefault("socket_connect_timeout", options.socket_connect_timeout.total_seconds())
    if options.socket_timeout is not None:
        kwargs.setdefault("socket_timeout", options.socket_timeout.total_seconds())
    if options.timeout is not None:
        kwargs.setdefault("timeout", options.timeout.total_seconds())
        # set socket_connection_timeout and socket_timeout if not set already
        kwargs.setdefault("socket_connect_timeout", options.timeout.total_seconds())
        kwargs.setdefault("socket_timeout", options.timeout.total_seconds())

    if options.read_from_replicas:
        connection_pool = ClusterWithReadReplicasBlockingConnectionPool.from_url(
            options.url, **kwargs
        )
    else:
        connection_pool = rediscluster.ClusterBlockingConnectionPool.from_url(options.url, **kwargs)

    connection_pool.track_key_reads_sample_rate = options.track_key_reads_sample_rate
    connection_pool.track_key_writes_sample_rate = options.track_key_writes_sample_rate

    connection_pool.read_from_replicas = options.read_from_replicas
    connection_pool.skip_full_coverage_check = options.skip_full_coverage_check

    return connection_pool


class ClusterRedisClient(config.Parser):
    """Configure a clustered Redis client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.
    See :py:func:`cluster_pool_from_config` for available configuration settings.
    """

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "ClusterRedisContextFactory":
        connection_pool = cluster_pool_from_config(raw_config, f"{key_path}.", **self.kwargs)
        return ClusterRedisContextFactory(connection_pool, key_path)


class ClusterRedisContextFactory(ContextFactory):
    """Cluster Redis client context factory.

    This factory will attach a
    :py:class:`~baseplate.clients.redis.MonitoredRedisClusterConnection` to an
    attribute on the :py:class:`~baseplate.RequestContext`. When Redis commands
    are executed via this connection object, they will use connections from the
    provided :py:class:`rediscluster.ClusterConnectionPool` and automatically record
    diagnostic information.
    :param connection_pool: A connection pool.
    """

    def __init__(self, connection_pool: rediscluster.ClusterConnectionPool, name: str = "redis"):
        self.connection_pool = connection_pool
        self.name = name

    def report_runtime_metrics(self, batch: metrics.Client) -> None:
        if not isinstance(self.connection_pool, rediscluster.ClusterBlockingConnectionPool):
            return

        size = self.connection_pool.max_connections
        open_connections_num = len(self.connection_pool._connections)
        MAX_CONNECTIONS.labels(self.name).set(size)
        OPEN_CONNECTIONS.labels(self.name).set(open_connections_num)

        batch.gauge("pool.size").replace(size)
        batch.gauge("pool.open_connections").replace(open_connections_num)

    def make_object_for_context(self, name: str, span: Span) -> "MonitoredRedisClusterConnection":
        return MonitoredRedisClusterConnection(
            name,
            span,
            self.connection_pool,
            getattr(self.connection_pool, "track_key_reads_sample_rate", 0),
            getattr(self.connection_pool, "track_key_writes_sample_rate", 0),
        )


class MonitoredRedisClusterConnection(rediscluster.RedisCluster):
    """Cluster Redis connection that collects diagnostic information.

    This connection acts like :py:class:`rediscluster.Redis` except that all
    operations are automatically wrapped with diagnostic collection.
    The interface is the same as that class except for the
    :py:meth:`~baseplate.clients.redis.MonitoredRedisClusterConnection.pipeline`
    method.
    """

    def __init__(
        self,
        context_name: str,
        server_span: Span,
        connection_pool: rediscluster.ClusterConnectionPool,
        track_key_reads_sample_rate: float = 0,
        track_key_writes_sample_rate: float = 0,
    ):
        self.context_name = context_name
        self.server_span = server_span
        self.track_key_reads_sample_rate = track_key_reads_sample_rate
        self.track_key_writes_sample_rate = track_key_writes_sample_rate
        self.hot_key_tracker = HotKeyTracker(
            self, self.track_key_reads_sample_rate, self.track_key_writes_sample_rate
        )

        super().__init__(
            connection_pool=connection_pool,
            read_from_replicas=connection_pool.read_from_replicas,
            skip_full_coverage_check=connection_pool.skip_full_coverage_check,
        )

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        command = args[0]
        trace_name = f"{self.context_name}.{command}"

        with self.server_span.make_child(trace_name):
            start_time = perf_counter()
            success = "true"
            labels = {
                f"{PROM_LABELS_PREFIX}_command": command,
                f"{PROM_LABELS_PREFIX}_client_name": self.connection_pool.connection_kwargs.get(
                    "client_name", ""
                ),
                f"{PROM_LABELS_PREFIX}_database": self.connection_pool.connection_kwargs.get(
                    "db", ""
                ),
                f"{PROM_LABELS_PREFIX}_type": "cluster",
            }

            try:
                with ACTIVE_REQUESTS.labels(**labels).track_inprogress():
                    res = super().execute_command(command, *args[1:], **kwargs)
                if isinstance(res, RedisError):
                    success = "false"
            except:  # noqa: E722
                success = "false"
                raise
            finally:
                result_labels = {**labels, f"{PROM_LABELS_PREFIX}_success": success}
                REQUESTS_TOTAL.labels(**result_labels).inc()
                LATENCY_SECONDS.labels(**result_labels).observe(perf_counter() - start_time)

        self.hot_key_tracker.maybe_track_key_usage(list(args))

        return res

    # pylint: disable=arguments-differ
    def pipeline(self, name: str) -> "MonitoredClusterRedisPipeline":
        """Create a pipeline.

        This returns an object on which you can call the standard Redis
        commands. Execution will be deferred until ``execute`` is called. This
        is useful for saving round trips even in a clustered environment .
        :param name: The name to attach to diagnostics for this pipeline.
        """
        return MonitoredClusterRedisPipeline(
            f"{self.context_name}.pipeline_{name}",
            self.server_span,
            self.connection_pool,
            self.response_callbacks,
            read_from_replicas=self.read_from_replicas,
            hot_key_tracker=self.hot_key_tracker,
        )

    # No transaction support in redis-py-cluster
    def transaction(self, *args: Any, **kwargs: Any) -> Any:
        """Not currently implemented."""
        raise NotImplementedError


# pylint: disable=abstract-method
class MonitoredClusterRedisPipeline(ClusterPipeline):
    def __init__(
        self,
        trace_name: str,
        server_span: Span,
        connection_pool: rediscluster.ClusterConnectionPool,
        response_callbacks: Dict,
        hot_key_tracker: Optional[HotKeyTracker],
        **kwargs: Any,
    ):
        self.trace_name = trace_name
        self.server_span = server_span
        self.hot_key_tracker = hot_key_tracker
        super().__init__(connection_pool, response_callbacks, **kwargs)

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        res = super().execute_command(*args, **kwargs)

        if self.hot_key_tracker is not None:
            self.hot_key_tracker.maybe_track_key_usage(list(args))

        return res

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
                f"{PROM_LABELS_PREFIX}_type": "cluster",
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
