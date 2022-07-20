import logging
import time

from threading import Event
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import _NOT_SET  # pylint: disable=no-name-in-module
from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from cassandra.cluster import ExecutionProfile  # pylint: disable=no-name-in-module
from cassandra.cluster import ResponseFuture  # pylint: disable=no-name-in-module
from cassandra.cluster import Session  # pylint: disable=no-name-in-module
from cassandra.query import BoundStatement  # pylint: disable=no-name-in-module
from cassandra.query import PreparedStatement  # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement  # pylint: disable=no-name-in-module
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.prometheus_metrics import default_latency_buckets
from baseplate.lib.secrets import SecretsStore


class CassandraPrometheusLabels(NamedTuple):
    cassandra_client_name: str
    cassandra_keyspace: str
    cassandra_query_name: str


REQUEST_TIME = Histogram(
    "cassandra_client_latency_seconds",
    "Time spent executing cassandra query",
    CassandraPrometheusLabels._fields + ("cassandra_success",),
    buckets=default_latency_buckets,
)
REQUEST_ACTIVE = Gauge(
    "cassandra_client_active_requests",
    "Current number of active cassandra queries",
    CassandraPrometheusLabels._fields,
)
REQUEST_TOTAL = Counter(
    "cassandra_client_requests_total",
    "Total number of cassandra queries",
    CassandraPrometheusLabels._fields + ("cassandra_success",),
)

if TYPE_CHECKING:
    import cqlmapper.connection


logger = logging.getLogger(__name__)


def cluster_from_config(
    app_config: config.RawConfig,
    secrets: Optional[SecretsStore] = None,
    prefix: str = "cassandra.",
    execution_profiles: Optional[Dict[str, ExecutionProfile]] = None,
    **kwargs: Any,
) -> Cluster:
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

    See :py:func:`cluster_from_config` for available configuration settings.

    :param keyspace: Which keyspace to set as the default for operations.
    :param client_name: the service-provided name for the client to identify the backends for
        cassandra host. MUST be user specified, MAY be blank if not specified.

    """

    def __init__(self, keyspace: str, client_name: str = "", **kwargs: Any):
        self.keyspace = keyspace
        self.kwargs = kwargs
        self.client_name = client_name

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "CassandraContextFactory":
        cluster = cluster_from_config(raw_config, prefix=f"{key_path}.", **self.kwargs)
        session = cluster.connect(keyspace=self.keyspace)
        return CassandraContextFactory(session, prometheus_client_name=self.client_name)


class CassandraContextFactory(ContextFactory):
    """Cassandra session context factory.

    This factory will attach a proxy object which acts like a
    :py:class:`cassandra.cluster.Session` to an attribute on the
    :py:class:`~baseplate.RequestContext`. The :py:meth:`execute`,
    :py:meth:`execute_async` and :py:meth:`prepare` methods will automatically
    record diagnostic information.

    :param cassandra.cluster.Session session: A configured session object.
    :param prometheus_client_name: the service-provided name for the client to identify the backends
        for cassandra host. MUST be user specified, MAY be blank if not specified.

    """

    def __init__(self, session: Session, prometheus_client_name: Optional[str] = None):
        self.session = session
        self.prepared_statements: Dict[str, PreparedStatement] = {}
        self.prometheus_client_name = prometheus_client_name

    def make_object_for_context(self, name: str, span: Span) -> "CassandraSessionAdapter":
        return CassandraSessionAdapter(
            name,
            span,
            self.session,
            self.prepared_statements,
            prometheus_client_name=self.prometheus_client_name,
        )


class CQLMapperClient(config.Parser):
    """Configure a CQLMapper client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`cluster_from_config` for available configuration settings.

    :param keyspace: Which keyspace to set as the default for operations.

    """

    def __init__(self, keyspace: str, **kwargs: Any):
        self.keyspace = keyspace
        self.kwargs = kwargs

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "CQLMapperContextFactory":
        cluster = cluster_from_config(raw_config, prefix=f"{key_path}.", **self.kwargs)
        session = cluster.connect(keyspace=self.keyspace)
        return CQLMapperContextFactory(session)


class CQLMapperContextFactory(CassandraContextFactory):
    """CQLMapper ORM connection context factory.

    This factory will attach a new CQLMapper
    :py:class:`cqlmapper.connection.Connection` to an attribute on the
    :py:class:`~baseplate.RequestContext`. This Connection object will use the
    same proxy object that CassandraContextFactory attaches to a context to run
    queries so the `execute` command will automatically record diagnostic
    information.

    :param cassandra.cluster.Session session: A configured session object.

    """

    def make_object_for_context(self, name: str, span: Span) -> "cqlmapper.connection.Connection":
        # Import inline so you can still use the regular Cassandra integration
        # without installing cqlmapper
        # pylint: disable=redefined-outer-name
        import cqlmapper.connection

        session_adapter = super().make_object_for_context(name, span)
        return cqlmapper.connection.Connection(session_adapter)


def wrap_future(
    response_future: ResponseFuture,
    callback_fn: Callable[..., None],
    callback_args: Any,
    errback_fn: Callable[..., None],
    errback_args: Any,
) -> ResponseFuture:
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

    def wait_for_callbacks_result(self: ResponseFuture) -> Any:
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
    bound_method = wait_for_callbacks_result.__get__(  # type: ignore
        response_future, ResponseFuture
    )
    # patch the ResponseFuture instance
    response_future.result = bound_method
    return response_future


class CassandraCallbackArgs(NamedTuple):
    span: Span
    start_time: float
    prom_labels: CassandraPrometheusLabels


def _on_execute_complete(_result: Any, args: CassandraCallbackArgs, event: Event) -> None:
    # TODO: tag with anything from the result set?
    # TODO: tag with any returned warnings
    try:
        args.span.finish()
    finally:
        prom_labels = args.prom_labels._asdict()
        REQUEST_TIME.labels(**prom_labels, cassandra_success="true").observe(
            time.perf_counter() - args.start_time
        )
        REQUEST_TOTAL.labels(**prom_labels, cassandra_success="true").inc()
        REQUEST_ACTIVE.labels(**prom_labels).dec()
        event.set()


def _on_execute_failed(exc: BaseException, args: CassandraCallbackArgs, event: Event) -> None:
    try:
        exc_info = (type(exc), exc, None)
        args.span.finish(exc_info=exc_info)
    finally:
        prom_labels = args.prom_labels._asdict()
        REQUEST_TIME.labels(**prom_labels, cassandra_success="false").observe(
            time.perf_counter() - args.start_time
        )
        REQUEST_TOTAL.labels(**prom_labels, cassandra_success="false").inc()
        REQUEST_ACTIVE.labels(**prom_labels).dec()
        event.set()


RowFactory = Callable[[List[str], List[Tuple]], Any]
Query = Union[str, SimpleStatement, PreparedStatement, BoundStatement]
Parameters = Union[Sequence[Any], Mapping[str, Any]]


class CassandraSessionAdapter:
    def __init__(
        self,
        context_name: str,
        server_span: Span,
        session: Session,
        prepared_statements: Dict[str, PreparedStatement],
        prometheus_client_name: Optional[str] = None,
    ):
        self.context_name = context_name
        self.server_span = server_span
        self.session = session
        self.prepared_statements = prepared_statements
        self.prometheus_client_name = prometheus_client_name

    def __getattr__(self, name: str) -> Any:
        return getattr(self.session, name)

    def execute(
        self,
        query: Query,
        parameters: Optional[Parameters] = None,
        timeout: Union[float, object] = _NOT_SET,
        query_name: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        return self.execute_async(
            query, parameters=parameters, timeout=timeout, query_name=query_name, **kwargs
        ).result()

    def execute_async(
        self,
        query: Query,
        parameters: Optional[Parameters] = None,
        timeout: Union[float, object] = _NOT_SET,
        query_name: Optional[str] = None,
        **kwargs: Any,
    ) -> ResponseFuture:
        prom_labels = CassandraPrometheusLabels(
            cassandra_client_name=self.prometheus_client_name
            if self.prometheus_client_name is not None
            else self.context_name,
            cassandra_keyspace=self.session.keyspace,
            cassandra_query_name=query_name if query_name is not None else "",
        )

        REQUEST_ACTIVE.labels(**prom_labels._asdict()).inc()
        start_time = time.perf_counter()
        trace_name = f"{self.context_name}.execute"
        span = self.server_span.make_child(trace_name)
        span.start()
        # TODO: include custom payload
        if isinstance(query, str):
            span.set_tag("statement", query)
        elif isinstance(query, (SimpleStatement, PreparedStatement)):
            span.set_tag("statement", query.query_string)
        elif isinstance(query, BoundStatement):
            span.set_tag("statement", query.prepared_statement.query_string)
        future = self.session.execute_async(query, parameters=parameters, timeout=timeout, **kwargs)
        callback_args = CassandraCallbackArgs(
            span=span,
            start_time=start_time,
            prom_labels=prom_labels,
        )
        future = wrap_future(
            response_future=future,
            callback_fn=_on_execute_complete,
            callback_args=callback_args,
            errback_fn=_on_execute_failed,
            errback_args=callback_args,
        )
        return future

    def prepare(self, query: str, cache: bool = True) -> PreparedStatement:
        """Prepare a CQL statement.

        :param cache: If set to True (default), prepared statements will be
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

        trace_name = f"{self.context_name}.prepare"
        with self.server_span.make_child(trace_name) as span:
            span.set_tag("statement", query)
            prepared = self.session.prepare(query)
            if cache:
                self.prepared_statements[query] = prepared
            return prepared
