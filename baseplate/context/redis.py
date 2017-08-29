from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import redis
import redis.client
from math import ceil

from . import ContextFactory
from .. import config, message_queue


def pool_from_config(app_config, prefix="redis.", **kwargs):
    """Make a ConnectionPool from a configuration dictionary.

    The keys useful to :py:func:`pool_from_config` should be prefixed, e.g.
    ``redis.url``, ``redis.max_connections``, etc. The ``prefix`` argument
    specifies the prefix used to filter keys.  Each key is mapped to a
    corresponding keyword argument on the :py:class:`redis.ConnectionPool`
    constructor.

    Supported keys:

    * ``url`` (required): a URL like ``redis://localhost/0``.
    * ``max_connections``: an integer maximum number of connections in the pool
    * ``socket_connect_timeout``: a timespan of how long to wait for sockets
        to connect. e.g. ``200 milliseconds``.
    * ``socket_timeout``: a timespan of how long to wait for socket operations,
        e.g. ``200 milliseconds``.

    """

    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "url": config.String,
            "max_connections": config.Optional(config.Integer, default=None),
            "socket_connect_timeout": config.Optional(config.Timespan, default=None),
            "socket_timeout": config.Optional(config.Timespan, default=None),
        },
    })

    options = getattr(cfg, config_prefix)

    if options.max_connections is not None:
        kwargs.setdefault("max_connections", options.max_connections)
    if options.socket_connect_timeout is not None:
        kwargs.setdefault("socket_connect_timeout", options.socket_connect_timeout.total_seconds())
    if options.socket_timeout is not None:
        kwargs.setdefault("socket_timeout", options.socket_timeout.total_seconds())

    return redis.BlockingConnectionPool.from_url(options.url, **kwargs)


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

    def make_object_for_context(self, name, server_span):
        return MonitoredRedisConnection(name, server_span, self.connection_pool)


# pylint: disable=too-many-public-methods
class MonitoredRedisConnection(redis.StrictRedis):
    """Redis connection that collects diagnostic information.

    This connection acts like :py:class:`redis.StrictRedis` except that all
    operations are automatically wrapped with diagnostic collection.

    The interface is the same as that class except for the
    :py:meth:`~baseplate.context.redis.MonitoredRedisConnection.pipeline`
    method.

    .. note:: Locks and PubSub are currently unsupported.

    """

    def __init__(self, context_name, server_span, connection_pool):
        self.context_name = context_name
        self.server_span = server_span

        super(MonitoredRedisConnection, self).__init__(
            connection_pool=connection_pool)

    def execute_command(self, command, *args, **kwargs):
        trace_name = "{}.{}".format(self.context_name, command)

        with self.server_span.make_child(trace_name):
            return super(MonitoredRedisConnection, self).execute_command(
                command, *args, **kwargs)

    # pylint: disable=arguments-differ
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
            self.server_span,
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
    def __init__(self, trace_name, server_span, connection_pool,
                 response_callbacks, **kwargs):
        self.trace_name = trace_name
        self.server_span = server_span
        super(MonitoredRedisPipeline, self).__init__(
            connection_pool, response_callbacks, **kwargs)

    def execute(self, **kwargs):
        with self.server_span.make_child(self.trace_name):
            return super(MonitoredRedisPipeline, self).execute(**kwargs)


class MessageQueue(object):
    """A Redis-backed variant of :py:class:`~baseplate.message_queue.MessageQueue`.

    :param name: can be any string.

    :param client: should be a :py:class:`redis.ConnectionPool` or
           :py:class:`redis.BlockingConnectionPool` from which a client
           connection can be created from (preferably generated from the
           :py:func:`pool_from_config` helper).

    """
    def __init__(self, name, client):
        self.queue = name
        if isinstance(client, redis.BlockingConnectionPool) or \
                isinstance(client, redis.ConnectionPool):
            self.client = redis.Redis(connection_pool=client)
        else:
            self.client = client

    def get(self, timeout=None):
        """Read a message from the queue.

        :param int timeout: If the queue is empty, the call will block up to
            ``timeout`` seconds or forever if ``None``, if a float is given,
            it will be rounded up to be an integer
        :raises: :py:exc:`TimedOutError` The queue was empty for the allowed
            duration of the call.

        """
        if isinstance(timeout, float):
            timeout = int(ceil(timeout))

        if timeout == 0:
            message = self.client.lpop(self.queue)
        else:
            message = self.client.blpop(self.queue, timeout=timeout)

            if message:
                message = message[1]

        if not message:
            raise message_queue.TimedOutError

        return message

    def put(self, message, timeout=None):
        """Add a message to the queue.

        :param message: will be typecast to a string upon storage and will come
               out of the queue as a string regardless of what type they are
               when passed into this method.
        """
        return self.client.rpush(self.queue, message)

    def unlink(self):
        """Not implemented for Redis variant
        """
        pass

    def close(self):
        """Close queue when finished

        Will delete the queue from the Redis server (Note, can still enqueue
        and dequeue as the actions will recreate the queue)
        """
        self.client.delete(self.queue)
