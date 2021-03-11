import random

from typing import Any
from typing import Dict

import rediscluster

from rediscluster.pipeline import ClusterPipeline

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib import metrics


# We want to be able to combine blocking behaviour with the ability to read from replicas
# Unfortunately this is not provide as-is so we combine two connection pool classes to provide
# the desired behaviour.
class ClusterWithReadReplicasBlockingConnectionPool(rediscluster.ClusterBlockingConnectionPool):
    # pylint: disable=arguments-differ
    def get_node_by_slot(self, slot: int, read_command: bool = False) -> Dict[str, Any]:
        """
        Get a node from the slot.
        If the command is a read command we'll try to return a random node.
        If there are no replicas or this isn't a read command we'll return the primary.
        """
        if read_command:
            return random.choice(self.nodes.slots[slot])

        # This isn't a read command, so return the primary
        return self.nodes.slots[slot]


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
    * ``skip_full_coverage_check``: Skips the check of cluster-require-full-coverage
      config, useful for clusters without the CONFIG command (like aws)
    * ``nodemanager_follow_cluster``: Tell the node manager to reuse the last set of
      nodes it was operating on when intializing.
    * ``read_from_replicas``: (Boolean) Whether the client should send all read queries to
        replicas instead of the primary
    * ``timeout``: how long to wait for sockets to connect. e.g.
        ``200 milliseconds`` (:py:func:`~baseplate.lib.config.Timespan`)
    """

    assert prefix.endswith(".")

    parser = config.SpecParser(
        {
            "url": config.String,
            "max_connections": config.Optional(config.Integer, default=50),
            "timeout": config.Optional(config.Timespan, default=100),
            "read_from_replicas": config.Optional(config.Boolean, default=False),
            "skip_full_coverage_check": config.Optional(config.Boolean, default=True),
            "nodemanager_follow_cluster": config.Optional(config.Boolean, default=None),
            "decode_responses": config.Optional(config.Boolean, default=True),
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
    if options.timeout is not None:
        kwargs.setdefault("timeout", options.timeout.total_seconds())

    if options.read_from_replicas:
        connection_pool = ClusterWithReadReplicasBlockingConnectionPool.from_url(
            options.url, **kwargs
        )
    else:
        connection_pool = rediscluster.ClusterBlockingConnectionPool.from_url(options.url, **kwargs)

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
        return ClusterRedisContextFactory(connection_pool)


class ClusterRedisContextFactory(ContextFactory):
    """Cluster Redis client context factory.

    This factory will attach a
    :py:class:`~baseplate.clients.redis.MonitoredClusterRedisConnection` to an
    attribute on the :py:class:`~baseplate.RequestContext`. When Redis commands
    are executed via this connection object, they will use connections from the
    provided :py:class:`rediscluster.ClusterConnectionPool` and automatically record
    diagnostic information.

    :param connection_pool: A connection pool.
    """

    def __init__(self, connection_pool: rediscluster.ClusterConnectionPool):
        self.connection_pool = connection_pool

    def report_runtime_metrics(self, batch: metrics.Client) -> None:
        if not isinstance(self.connection_pool, rediscluster.ClusterBlockingConnectionPool):
            return

        size = self.connection_pool.max_connections
        open_connections = len(self.connection_pool._connections)
        available = self.connection_pool.pool.qsize()
        in_use = size - available

        batch.gauge("pool.size").replace(size)
        batch.gauge("pool.in_use").replace(in_use)
        batch.gauge("pool.open_and_available").replace(open_connections - in_use)

    def make_object_for_context(self, name: str, span: Span) -> "MonitoredClusterRedisConnection":
        return MonitoredClusterRedisConnection(name, span, self.connection_pool)


class MonitoredClusterRedisConnection(rediscluster.RedisCluster):
    """Cluster Redis connection that collects diagnostic information.

    This connection acts like :py:class:`rediscluster.Redis` except that all
    operations are automatically wrapped with diagnostic collection.

    The interface is the same as that class except for the
    :py:meth:`~baseplate.clients.redis.MonitoredClusterRedisConnection.pipeline`
    method.

    """

    def __init__(
        self,
        context_name: str,
        server_span: Span,
        connection_pool: rediscluster.ClusterConnectionPool,
    ):
        self.context_name = context_name
        self.server_span = server_span

        super().__init__(
            connection_pool=connection_pool,
            read_from_replicas=connection_pool.read_from_replicas,
            skip_full_coverage_check=connection_pool.skip_full_coverage_check,
        )

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        command = args[0]
        trace_name = f"{self.context_name}.{command}"

        with self.server_span.make_child(trace_name):
            return super().execute_command(command, *args[1:], **kwargs)

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
        **kwargs: Any,
    ):
        self.trace_name = trace_name
        self.server_span = server_span
        super().__init__(connection_pool, response_callbacks, **kwargs)

    # pylint: disable=arguments-differ
    def execute(self, **kwargs: Any) -> Any:
        with self.server_span.make_child(self.trace_name):
            return super().execute(**kwargs)
