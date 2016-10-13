from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from cassandra.cluster import Cluster, _NOT_SET

from . import ContextFactory
from .. import config


def cluster_from_config(app_config, prefix="cassandra.", **kwargs):
    """Make a Cluster from a configuration dictionary.

    The keys useful to :py:func:`cluster_from_config` should be prefixed, e.g.
    ``cassandra.contact_points`` etc. The ``prefix`` argument specifies the
    prefix used to filter keys.  Each key is mapped to a corresponding keyword
    argument on the :py:class:`~cassandra.cluster.Cluster` constructor.  Any
    kwargs given to this function will be passed through to the
    :py:class:`~cassandra.cluster.Cluster` constructor.  If you pass a kwarg
    that is duplicated by ``app_config`` then the value in ``app_config``
    will be passed rather than the one in kwargs.

    Supported keys:

    * ``contact_points`` (required): comma delimited list of contact points to
      try connecting for cluster discovery
    * ``port``: The server-side port to open connections to.

    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "contact_points": config.TupleOf(config.String),
            "port": config.Optional(config.Integer, default=None),
        },
    })

    options = getattr(cfg, config_prefix)

    if options.port:
        kwargs.setdefault("port", options.port)

    return Cluster(options.contact_points, **kwargs)


class CassandraContextFactory(ContextFactory):
    """Cassandra session context factory.

    This factory will attach a proxy object which acts like a
    :py:class:`cassandra.cluster.Session` to an attribute on the :term:`context
    object`. The ``execute``, ``execute_async`` or ``prepare`` methods will
    automatically record diagnostic information.

    :param cassandra.cluster.Session session: A configured session object.

    """
    def __init__(self, session):
        self.session = session

    def make_object_for_context(self, name, server_span):
        return CassandraSessionAdapter(name, server_span, self.session)


def _on_execute_complete(_, span):
    # TODO: tag with anything from the result set?
    # TODO: tag with any returned warnings
    span.finish()


def _on_execute_failed(exc, span):
    span.tag("error", str(exc))
    span.finish()


class CassandraSessionAdapter(object):
    def __init__(self, context_name, server_span, session):
        self.context_name = context_name
        self.server_span = server_span
        self.session = session

    def execute(self, query, parameters=None, timeout=_NOT_SET):
        return self.execute_async(query, parameters, timeout).result()

    def execute_async(self, query, parameters=None, timeout=_NOT_SET):
        trace_name = "{}.{}".format(self.context_name, "execute")
        span = self.server_span.make_child(trace_name)
        span.start()
        # TODO: include custom payload
        span.tag("statement", query)
        future = self.session.execute_async(query, parameters, timeout)
        future.add_callback(_on_execute_complete, span)
        future.add_errback(_on_execute_failed, span)
        return future

    def prepare(self, query):
        trace_name = "{}.{}".format(self.context_name, "prepare")
        with self.server_span.make_child(trace_name) as span:
            span.tag("statement", query)
            return self.session.prepare(query)
