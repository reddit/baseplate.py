from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
import six

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

    def make_object_for_context(self, name, server_span):
        return CassandraSessionAdapter(name, server_span, self.session)


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
    def __init__(self, context_name, server_span, session):
        self.context_name = context_name
        self.server_span = server_span
        self.session = session

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
        span = self._get_span(trace_name, query)
        span.start()
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

    def prepare(self, query):
        trace_name = "{}.{}".format(self.context_name, "prepare")
        with self._get_span(trace_name, query):
            return self.session.prepare(query)

    def _get_span(self, trace_name, query):
        """Get trace span for query.

        Metadata from the passed query is extracted and the created span is
        tagged accordingly.

        """
        span = self.server_span.make_child(trace_name)
        cql_str = None
        if isinstance(query, string_types):
            cql_str = query
        elif isinstance(query, (SimpleStatement, PreparedStatement)):
            cql_str = query.query_string
        elif isinstance(query, BoundStatement):
            cql_str = query.prepared_statement.query_string

        # TODO: include custom payload
        span.set_tag("statement", cql_str)
        for key, value in CQLMetadataExtractor.extract(cql_str).items():
            span.set_tag(key, value)

        return span


class CQLMetadataExtractor(object):
    """Extract metadata from CQL statements.

    """
    _KEYSPACE_REGEX = r"((?P<kquote>[\"])|)(?P<keyspace>(?(kquote).{1,48}|[a-zA-Z_0-9]{1,48}))(?(kquote)\"|)(?(keyspace)\.|)"  # noqa
    _TABLE_REGEX = r"((?P<tquote>[\"])|)(?P<table>(?(tquote).{1,48}|[a-zA-Z_0-9]{1,48}))(?(tquote)\"|)"  # noqa

    CQL_KEYSPACE_TABLENAME_REGEX = r"({}|){}".format(_KEYSPACE_REGEX, _TABLE_REGEX)
    FROM_STATEMENT_REGEX = r".* FROM {}.*".format(CQL_KEYSPACE_TABLENAME_REGEX)
    INSERT_STATEMENT_REGEX = r"insert into {}.*".format(CQL_KEYSPACE_TABLENAME_REGEX)
    UPDATE_STATEMENT_REGEX = r"update {}.*".format(CQL_KEYSPACE_TABLENAME_REGEX)
    CQL_STATEMENT_EXTRACTORS = {
        "select": re.compile(FROM_STATEMENT_REGEX, re.I),
        "insert": re.compile(INSERT_STATEMENT_REGEX, re.I),
        "update": re.compile(UPDATE_STATEMENT_REGEX, re.I),
        "delete": re.compile(FROM_STATEMENT_REGEX, re.I),
    }

    _MATCHED_GROUP_WHITELIST = ("keyspace", "table")

    @classmethod
    def extract(cls, cql_str):
        first_token = cql_str.split(" ")[0].lower()
        statement_metadata = {"type": first_token}
        try:
            cql_regex = cls.CQL_STATEMENT_EXTRACTORS[first_token]
        except KeyError:
            # No extractors found for the first token.
            pass
        else:
            matches = cql_regex.match(cql_str)
            if matches is not None:
                # Looping through matches to include only whitelisted values
                matches_dict = matches.groupdict()
                filtered_matches_dict = {k: matches_dict[k] for k in cls._MATCHED_GROUP_WHITELIST}
                for k, v in six.iteritems(filtered_matches_dict):
                    if v is None:
                        continue
                    statement_metadata[k] = v
        finally:
            return statement_metadata
