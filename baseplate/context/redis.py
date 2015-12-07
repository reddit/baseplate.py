from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import redis

from . import ContextFactory


class RedisContextFactory(ContextFactory):
    """Redis client context factory.

    This factory will attach a proxy object which acts like a
    :py:class:`redis.StrictRedis` object to an attribute on the :term:`context
    object`. When redis commands are executed via this proxy object, they will
    use connections from the provided :py:class:`redis.ConnectionPool` and
    automatically record diagnostic information.

    .. note::

        Pipelines, transactions, locks, and pubsub are currently unsupported.

    :param redis.ConnectionPool connection_pool: A connection pool.

    """
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool

    def make_object_for_context(self, name, root_span):
        return RedisConnectionAdapter(name, root_span, self.connection_pool)


class RedisConnectionAdapter(redis.StrictRedis):
    def __init__(self, context_name, root_span, connection_pool):
        self.context_name = context_name
        self.root_span = root_span

        super(RedisConnectionAdapter, self).__init__(
            connection_pool=connection_pool)

    def execute_command(self, command, *args, **kwargs):
        trace_name = "{}.{}".format(self.context_name, command)

        with self.root_span.make_child(trace_name):
            return super(RedisConnectionAdapter, self).execute_command(
                command, *args, **kwargs)

    # these commands are not yet implemented because i haven't taken the time
    # to look at if they go through `execute_command` like the basic commands
    # (e.g. "get" and "set") do. if these commands end up needed, it should
    # just be some elbow grease to get them going.
    def pipeline(self, *args, **kwargs):
        raise NotImplementedError

    def transaction(self, *args, **kwargs):
        raise NotImplementedError

    def lock(self, *args, **kwargs):
        raise NotImplementedError

    def pubsub(self, *args, **kwargs):
        raise NotImplementedError
