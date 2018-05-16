from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from cassandra.cluster import Cluster, _NOT_SET
from cassandra.query import SimpleStatement, PreparedStatement, BoundStatement

from . import ContextFactory
from .. import config
from .._compat import string_types


def cluster_from_config(app_config, prefix="cassandra.", **kwargs):
    """Make a Cluster from a configuration dictionary.

    The keys useful to :py:func:`cluster_from_config` should be prefixed, e.g.
    ``cassandra.contact_points`` etc. The ``prefix`` argument specifies the
    prefix used to filter keys.  Each key is mapped to a corresponding keyword
    argument on the :py:class:`~cassandra.cluster.Cluster` constructor.  Any
    keyword arguments given to this function will be passed through to the
    :py:class:`~cassandra.cluster.Cluster` constructor. Keyword arguments take
    precedence over the configuration file.

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
        self.prepared_statements = {}

    def make_object_for_context(self, name, server_span):
        return CassandraSessionAdapter(name, server_span, self.session,
                                       self.prepared_statements)


class CQLMapperContextFactory(CassandraContextFactory):
    """CQLMapper ORM connection context factory

    This factory will attach a new CQLMapper
    :py:class:`cqlmapper.connection.Connection` to an attribute on the
    :term:`context object`. This Connection object will use the same proxy
    object that CassandraContextFactory attaches to a context to run queries
    so the `execute` command will automatically record diagnostic information.

    :param cassandra.cluster.Session session: A configured session object.

    """

    def make_object_for_context(self, name, server_span):
        # Import inline so you can still use the regular Cassandra integration
        # without installing cqlmapper
        import cqlmapper.connection
        session_adapter = super(
            CQLMapperContextFactory,
            self,
        ).make_object_for_context(name, server_span)
        return cqlmapper.connection.Connection(session_adapter)


def _on_execute_complete(_, span):
    # TODO: tag with anything from the result set?
    # TODO: tag with any returned warnings
    span.finish()


def _on_execute_failed(exc, span):
    exc_info = (type(exc), exc, None)
    span.finish(exc_info=exc_info)


class CassandraSessionAdapter(object):
    def __init__(self, context_name, server_span, session, prepared_statements):
        self.context_name = context_name
        self.server_span = server_span
        self.session = session
        self.prepared_statements = prepared_statements

    @property
    def cluster(self):
        return self.session.cluster

    @property
    def encoder(self):
        return self.session.encoder

    @property
    def keyspace(self):
        return self.session.keyspace

    @property
    def row_factory(self):
        return self.session.row_factory

    @row_factory.setter
    def row_factory(self, new_row_factory):
        self.session.row_factory = new_row_factory

    def execute(self, query, parameters=None, timeout=_NOT_SET):
        return self.execute_async(query, parameters, timeout).result()

    def execute_async(self, query, parameters=None, timeout=_NOT_SET):
        trace_name = "{}.{}".format(self.context_name, "execute")
        span = self.server_span.make_child(trace_name)
        span.start()
        # TODO: include custom payload
        if isinstance(query, string_types):
            span.set_tag("statement", query)
        elif isinstance(query, (SimpleStatement, PreparedStatement)):
            span.set_tag("statement", query.query_string)
        elif isinstance(query, BoundStatement):
            span.set_tag("statement", query.prepared_statement.query_string)
        future = self.session.execute_async(
            query,
            parameters=parameters,
            timeout=timeout,
        )
        future.add_callback(_on_execute_complete, span)
        future.add_errback(_on_execute_failed, span)
        return future

    def prepare(self, query, cache=True):
        """Prepares a CQL statement.

        :param bool cache: If set to True (default), prepared statements will be
        automatically cached and reused. The cache is keyed on the text of the
        statement. Set to False if you don't want your prepared statements
        cached, which might be advisable if you have a very high-cardinality
        query set. Prepared statements are cached indefinitely, so be wary of
        memory usage.
        """

        if cache:
            try:
                return self.prepared_statements[query]
            except KeyError:
                pass

        trace_name = "{}.{}".format(self.context_name, "prepare")
        with self.server_span.make_child(trace_name) as span:
            span.set_tag("statement", query)
            prepared = self.session.prepare(query)
            if cache:
                self.prepared_statements[query] = prepared
            return prepared
