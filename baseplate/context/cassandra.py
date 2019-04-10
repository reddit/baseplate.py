from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
from threading import Event

# pylint: disable=no-name-in-module
from cassandra.cluster import Cluster, _NOT_SET
# pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement, PreparedStatement, BoundStatement

from . import ContextFactory
from .. import config
from .._compat import string_types


logger = logging.getLogger(__name__)


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

    def make_object_for_context(self, name, span):
        return CassandraSessionAdapter(name, span, self.session, self.prepared_statements)


class CQLMapperContextFactory(CassandraContextFactory):
    """CQLMapper ORM connection context factory.

    This factory will attach a new CQLMapper
    :py:class:`cqlmapper.connection.Connection` to an attribute on the
    :term:`context object`. This Connection object will use the same proxy
    object that CassandraContextFactory attaches to a context to run queries
    so the `execute` command will automatically record diagnostic information.

    :param cassandra.cluster.Session session: A configured session object.

    """

    def make_object_for_context(self, name, span):
        # Import inline so you can still use the regular Cassandra integration
        # without installing cqlmapper
        import cqlmapper.connection
        session_adapter = super(
            CQLMapperContextFactory,
            self,
        ).make_object_for_context(name, span)
        return cqlmapper.connection.Connection(session_adapter)


class WaitForCallbackResponseFuture(object):
    """Wrap the ResponseFuture to ensure callbacks have completed.

    The callback_fn and errback_fn passed in the constructor are given
    special treatment: they must be complete before a result will be
    returned from result(). They are not given precedence over other
    callbacks or errbacks, so if another callback triggers the response
    from the service (and the server span is closed) the special callback might
    not complete.

    This fixes a race condition where the server span can complete before
    the callback has closed out the child span.

    """

    def __init__(self, future, callback_fn, callback_args, errback_fn, errback_args):
        self.callback_event = Event()

        future.add_callback(callback_fn, callback_args, self.callback_event)
        future.add_errback(errback_fn, errback_args, self.callback_event)

        self.future = future

    def result(self):
        exc = None

        try:
            result = self.future.result()
        except Exception as e:
            exc = e

        # wait for either _on_execute_complete or _on_execute_failed to run
        wait_result = self.callback_event.wait(timeout=0.01)
        if not wait_result:
            logger.warning("Cassandra metrics callback took too long. Some metrics may be lost.")

        if exc:
            raise exc   # pylint: disable=E0702

        return result

    # we need to define the following methods for compatibility with
    # execute_concurrent and execute_concurrent_with_args, which add callbacks
    # to futures returned by execute_async. we're not going to attempt to
    # ensure that our special callback completes before these callbacks.
    def add_callback(self, fn, *args, **kwargs):
        self.future.add_callback(fn, *args, **kwargs)

    def add_errback(self, fn, *args, **kwargs):
        self.future.add_callback(fn, *args, **kwargs)

    def add_callbacks(self, callback, errback, callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):
        self.add_callback(callback, *callback_args, **(callback_kwargs or {}))
        self.add_errback(errback, *errback_args, **(errback_kwargs or {}))

    def clear_callbacks(self):
        self.future.clear_callbacks()


def _on_execute_complete(_result, span, event):
    # TODO: tag with anything from the result set?
    # TODO: tag with any returned warnings
    try:
        span.finish()
    finally:
        event.set()


def _on_execute_failed(exc, span, event):
    try:
        exc_info = (type(exc), exc, None)
        span.finish(exc_info=exc_info)
    finally:
        event.set()


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
        future = WaitForCallbackResponseFuture(
            future=future,
            callback_fn=_on_execute_complete,
            callback_args=span,
            errback_fn=_on_execute_failed,
            errback_args=span,
        )
        return future

    def prepare(self, query, cache=True):
        """Prepare a CQL statement.

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
