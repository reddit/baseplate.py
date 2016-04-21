from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import redis
import redis.client

from . import ContextFactory


class RedisContextFactory(ContextFactory):
    """Redis client context factory.

    This factory will attach a
    :py:class:`~baseplate.context.redis.MonitoredRedisConnection` to an
    attribute on the :term:`context object`. When redis commands are executed
    via this connection object, they will use connections from the provided
    :py:class:`redis.ConnectionPool` and automatically record diagnostic
    information.

    :param redis.ConnectionPool connection_pool: A connection pool.

    :returns: :py:class:`~baseplate.context.redis.MonitoredRedisConnection`

    """
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool

    def make_object_for_context(self, name, root_span):
        return MonitoredRedisConnection(name, root_span, self.connection_pool)


class MonitoredRedisConnection(redis.StrictRedis):
    """Redis connection that collects diagnostic information.

    This connection acts like :py:class:`redis.StrictRedis` except that all
    operations are automatically wrapped with diagnostic collection.

    The interface is the same as that class except for the
    :py:meth:`~baseplate.context.redis.MonitoredRedisConnection.pipeline`
    method.

    .. note:: Locks and pubsub are currently unsupported.

    """

    def __init__(self, context_name, root_span, connection_pool):
        self.context_name = context_name
        self.root_span = root_span

        super(MonitoredRedisConnection, self).__init__(
            connection_pool=connection_pool)

    def execute_command(self, command, *args, **kwargs):
        trace_name = "{}.{}".format(self.context_name, command)

        with self.root_span.make_child(trace_name):
            return super(MonitoredRedisConnection, self).execute_command(
                command, *args, **kwargs)

    def pipeline(self, name, transaction=True, shard_hint=None):
        """Create a pipeline.

        This returns an object on which you can call the standard redis
        commands. Execution will be deferred until ``execute`` is called. This
        is useful for saving round trips.

        :param str name: The name to attach to diagnostics for this pipeline.
        :param bool transaction: Whether or not the commands in the pipeline
            are wrapped with a transaction and executed atomically.

        """
        return MonitoredRedisPipeline(
            "{}.pipeline_{}".format(self.context_name, name),
            self.root_span,
            self.connection_pool,
            self.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )

    # these commands are not yet implemented, but probably not unimplementable
    def transaction(self, *args, **kwargs):
        raise NotImplementedError

    def lock(self, *args, **kwargs):
        raise NotImplementedError

    def pubsub(self, *args, **kwargs):
        raise NotImplementedError


class MonitoredRedisPipeline(redis.client.StrictPipeline):
    def __init__(self, trace_name, root_span, connection_pool,
                 response_callbacks, **kwargs):
        self.trace_name = trace_name
        self.root_span = root_span
        super(MonitoredRedisPipeline, self).__init__(
            connection_pool, response_callbacks, **kwargs)

    def execute(self, **kwargs):
        with self.root_span.make_child(self.trace_name):
            return super(MonitoredRedisPipeline, self).execute(**kwargs)
