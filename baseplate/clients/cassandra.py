import logging

from threading import Event
from typing import Any
from typing import Dict
from typing import Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import _NOT_SET  # pylint: disable=no-name-in-module
from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from cassandra.cluster import ExecutionProfile  # pylint: disable=no-name-in-module
from cassandra.cluster import ResponseFuture  # pylint: disable=no-name-in-module
from cassandra.query import BoundStatement  # pylint: disable=no-name-in-module
from cassandra.query import PreparedStatement  # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement  # pylint: disable=no-name-in-module

from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.secrets import SecretsStore


logger = logging.getLogger(__name__)


def cluster_from_config(
    app_config: config.RawConfig,
    secrets: Optional[SecretsStore] = None,
    prefix: str = "cassandra.",
    execution_profiles: Optional[Dict[str, ExecutionProfile]] = None,
    **kwargs: Any,
):
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
    * ``credentials_secret`` (optional): the key used to retrieve the database
        credentials from ``secrets`` as a :py:class:`~baseplate.lib.secrets.CredentialSecret`.

    :param execution_profiles: Configured execution profiles to provide to the
        rest of the application.

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {
            "contact_points": config.TupleOf(config.String),
            "port": config.Optional(config.Integer, default=None),
            "credentials_secret": config.Optional(config.String),
        }
    )
    options = parser.parse(prefix[:-1], app_config)

    if options.port:
        kwargs.setdefault("port", options.port)

    if options.credentials_secret:
        if not secrets:
            raise TypeError("'secrets' is required if 'credentials_secret' is set")
        credentials = secrets.get_credentials(options.credentials_secret)
        kwargs.setdefault(
            "auth_provider",
            PlainTextAuthProvider(username=credentials.username, password=credentials.password),
        )

    return Cluster(options.contact_points, execution_profiles=execution_profiles, **kwargs)


class CassandraClient(config.Parser):
    """Configure a Cassandra client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`cluster_from_config` for available configurables.

    :param keyspace: Which keyspace to set as the default for operations.

    """

    def __init__(self, keyspace: str, **kwargs):
        self.keyspace = keyspace
        self.kwargs = kwargs

    def parse(self, key_path: str, raw_config: config.RawConfig) -> ContextFactory:
        cluster = cluster_from_config(raw_config, prefix=f"{key_path}.", **self.kwargs)
        session = cluster.connect(keyspace=self.keyspace)
        return CassandraContextFactory(session)


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


class CQLMapperClient(config.Parser):
    """Configure a CQLMapper client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`cluster_from_config` for available configurables.

    :param keyspace: Which keyspace to set as the default for operations.

    """

    def __init__(self, keyspace: str, **kwargs):
        self.keyspace = keyspace
        self.kwargs = kwargs

    def parse(self, key_path: str, raw_config: config.RawConfig) -> ContextFactory:
        cluster = cluster_from_config(raw_config, prefix=f"{key_path}.", **self.kwargs)
        session = cluster.connect(keyspace=self.keyspace)
        return CQLMapperContextFactory(session)


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

        session_adapter = super().make_object_for_context(name, span)
        return cqlmapper.connection.Connection(session_adapter)


def wrap_future(response_future, callback_fn, callback_args, errback_fn, errback_args):
    """Patch ResponseFuture.result() to wait for callback or errback to complete.

    The callback_fn and errback_fn are given special treatment: they must be
    complete before a result will be returned from ResponseFuture.result().
    They are not given precedence over other callbacks or errbacks, so if
    another callback triggers the response from the service (and the server
    span is closed) the special callback might not complete. The special
    callback is added first and  callbacks are executed in order, so
    generally the special callback should finish before any other callbacks.

    This fixes a race condition where the server span can complete before
    the callback has closed out the child span.

    """
    response_future._callback_event = Event()

    response_future.add_callback(callback_fn, callback_args, response_future._callback_event)
    response_future.add_errback(errback_fn, errback_args, response_future._callback_event)

    def wait_for_callbacks_result(self):
        exc = None

        try:
            result = ResponseFuture.result(self)
        except Exception as e:
            exc = e

        # wait for either _on_execute_complete or _on_execute_failed to run
        wait_result = self._callback_event.wait(timeout=0.01)
        if not wait_result:
            logger.warning("Cassandra metrics callback took too long. Some metrics may be lost.")

        if exc:
            raise exc  # pylint: disable=E0702

        return result

    # call __get__ to turn wait_for_callbacks_result into a bound method
    bound_method = wait_for_callbacks_result.__get__(response_future, ResponseFuture)
    # patch the ResponseFuture instance
    response_future.result = bound_method
    return response_future


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


class CassandraSessionAdapter:
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
        if isinstance(query, str):
            span.set_tag("statement", query)
        elif isinstance(query, (SimpleStatement, PreparedStatement)):
            span.set_tag("statement", query.query_string)
        elif isinstance(query, BoundStatement):
            span.set_tag("statement", query.prepared_statement.query_string)
        future = self.session.execute_async(query, parameters=parameters, timeout=timeout)
        future = wrap_future(
            response_future=future,
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
