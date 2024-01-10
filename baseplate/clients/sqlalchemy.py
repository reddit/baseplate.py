import re

from time import perf_counter
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union

import sqlalchemy

from opentelemetry import trace
from opentelemetry.instrumentation.sqlcommenter_utils import _add_sql_comment
from opentelemetry.instrumentation.utils import _get_opentelemetry_values
from opentelemetry.semconv.trace import NetTransportValues
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status
from opentelemetry.trace.status import StatusCode
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.engine import ExceptionContext
from sqlalchemy.engine.interfaces import ExecutionContext
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import Session
from sqlalchemy.pool import QueuePool
from wrapt import wrap_function_wrapper as _w

from baseplate import _ExcInfo
from baseplate import Span
from baseplate import SpanObserver
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib import metrics
from baseplate.lib.prometheus_metrics import default_latency_buckets
from baseplate.lib.secrets import SecretsStore


def engine_from_config(
    app_config: config.RawConfig,
    secrets: Optional[SecretsStore] = None,
    prefix: str = "database.",
    **kwargs: Any,
) -> Engine:
    """Make an :py:class:`~sqlalchemy.engine.Engine` from a configuration dictionary.

    The keys useful to :py:func:`engine_from_config` should be prefixed, e.g.
    ``database.url``, etc. The ``prefix`` argument specifies the prefix used to
    filter keys.

    Supported keys:

    * ``url``: the connection URL to the database, passed to
        :py:func:`~sqlalchemy.engine.url.make_url` to create the
        :py:class:`~sqlalchemy.engine.url.URL` used to connect to the database.
    * ``credentials_secret`` (optional): the key used to retrieve the database
        credentials from ``secrets`` as a :py:class:`~baseplate.lib.secrets.CredentialSecret`.
        If this is supplied, any credentials given in ``url`` we be replaced by
        these.
    * ``pool_recycle`` (optional): this setting causes the pool to recycle connections after
        the given number of seconds has passed. It defaults to -1, or no timeout.
    * ``pool_pre_ping`` (optional): when set to true, this setting causes
        sqlalchemy to perform a liveness-check query each time a connection is
        checked out of the pool.  If the liveness-check fails, the connection
        is gracefully recycled.  This ensures severed connections are handled
        more gracefully, at the cost of doing a `SELECT 1` at the start of each
        checkout. When used, this obviates most of the reasons you might use
        pool_recycle, and as such they shouldn't normally be used
        simultaneously.  Requires SQLAlchemy 1.3.
    * ``pool_size`` (optional) : The number of connections that can be saved in the pool.
    * ``max_overflow`` (optional) : Max connections that can be opened beyond the pool size.

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {
            "url": config.String,
            "credentials_secret": config.Optional(config.String),
            "pool_recycle": config.Optional(config.Integer),
            "pool_pre_ping": config.Optional(config.Boolean),
            "pool_size": config.Optional(config.Integer),
            "max_overflow": config.Optional(config.Integer),
        }
    )
    options = parser.parse(prefix[:-1], app_config)
    url = make_url(options.url)

    if options.pool_recycle is not None:
        kwargs.setdefault("pool_recycle", options.pool_recycle)

    if options.pool_pre_ping is not None:
        kwargs.setdefault("pool_pre_ping", options.pool_pre_ping)

    if options.pool_size is not None:
        kwargs.setdefault("pool_size", options.pool_size)

    if options.max_overflow is not None:
        kwargs.setdefault("max_overflow", options.max_overflow)

    if options.credentials_secret:
        if not secrets:
            raise TypeError("'secrets' is required if 'credentials_secret' is set")
        credentials = secrets.get_credentials(options.credentials_secret)

        # support sqlalchemy 1.4+ where URL is immutable
        # https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#the-url-object-is-now-immutable
        if hasattr(url, "set"):
            url = url.set(username=credentials.username, password=credentials.password)
        else:
            url.username = credentials.username
            url.password = credentials.password

    return create_engine(url, **kwargs)


class SQLAlchemySession(config.Parser):
    """Configure a SQLAlchemy Session.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`engine_from_config` for available configuration settings.

    :param secrets: Required if configured to use credentials to talk to the database.

    """

    def __init__(self, secrets: Optional[SecretsStore] = None, **kwargs: Any):
        self.secrets = secrets
        self.kwargs = kwargs

    def parse(
        self, key_path: str, raw_config: config.RawConfig
    ) -> "SQLAlchemySessionContextFactory":
        engine = engine_from_config(
            raw_config, secrets=self.secrets, prefix=f"{key_path}.", **self.kwargs
        )

        return SQLAlchemySessionContextFactory(engine, key_path)


Parameters = Optional[Union[Dict[str, Any], Sequence[Any]]]


SAFE_TRACE_ID = re.compile("^[A-Za-z0-9_-]+$")


def _normalize_vendor(vendor):
    """Return a canonical name for a type of database"""

    if not vendor:
        return "db"
    if "sqlite" in vendor:
        return "sqlite"
    if "postgres" in vendor or vendor == "psycopg2":
        return "postgresql"

    return vendor


def _get_attributes_from_url(url):
    """Set connection tags from the url. return true if successful."""
    attrs = {}
    if url.host:
        attrs[SpanAttributes.NET_PEER_NAME] = url.host
    if url.port:
        attrs[SpanAttributes.NET_PEER_PORT] = url.port
    if url.database:
        attrs[SpanAttributes.DB_NAME] = url.database
    if url.username:
        attrs[SpanAttributes.DB_USER] = url.username
    return attrs, bool(url.host)


def _get_attributes_from_cursor(vendor, cursor, attrs):
    """Attempt to set db connection attributes by introspecting the cursor."""
    if vendor == "postgresql":
        info = getattr(getattr(cursor, "connection", None), "info", None)
        if not info:
            return attrs

        attrs[SpanAttributes.DB_NAME] = info.dbname
        is_unix_socket = info.host and info.host.startswith("/")

        if is_unix_socket:
            attrs[SpanAttributes.NET_TRANSPORT] = NetTransportValues.OTHER.value
            if info.port:
                # postgresql enforces this pattern on all socket names
                attrs[SpanAttributes.NET_PEER_NAME] = os.path.join(
                    info.host, f".s.PGSQL.{info.port}"
                )
        else:
            attrs[SpanAttributes.NET_TRANSPORT] = NetTransportValues.IP_TCP.value
            attrs[SpanAttributes.NET_PEER_NAME] = info.host
            if info.port:
                attrs[SpanAttributes.NET_PEER_PORT] = int(info.port)
    return attrs


def _wrap_connect(tracer):
    # pylint: disable=unused-argument
    def _wrap_connect_internal(func, module, args, kwargs):
        with tracer.start_as_current_span("connect", kind=trace.SpanKind.CLIENT) as span:
            if span.is_recording():
                attrs, _ = _get_attributes_from_url(module.url)
                span.set_attributes(attrs)
                span.set_attribute(SpanAttributes.DB_SYSTEM, _normalize_vendor(module.name))
            return func(*args, **kwargs)

    return _wrap_connect_internal


class SQLAlchemyEngineContextFactory(ContextFactory):
    """SQLAlchemy core engine context factory.

    This factory will attach a SQLAlchemy :py:class:`sqlalchemy.engine.Engine`
    to an attribute on the :py:class:`~baseplate.RequestContext`. All cursor
    (query) execution will automatically record diagnostic information.

    Additionally, the trace and span ID will be added as a comment to the text
    of the SQL statement. This is to aid correlation of queries with requests.

    .. seealso::

        The engine is the low-level SQLAlchemy API. If you want to use the ORM,
        consider using
        :py:class:`~baseplate.clients.sqlalchemy.SQLAlchemySessionContextFactory`
        instead.

    :param engine: A configured SQLAlchemy engine.

    """

    PROM_PREFIX = "sql_client"
    PROM_POOL_PREFIX = f"{PROM_PREFIX}_pool"
    PROM_POOL_LABELS = ["sql_pool"]

    max_connections_gauge = Gauge(
        f"{PROM_POOL_PREFIX}_max_size",
        "Maximum number of connections allowed in this pool",
        PROM_POOL_LABELS,
        multiprocess_mode="livesum",
    )

    checked_out_connections_gauge = Gauge(
        f"{PROM_POOL_PREFIX}_active_connections",
        "Number of connections in use by this pool (checked out + overflow)",
        PROM_POOL_LABELS,
        multiprocess_mode="livesum",
    )
    checked_in_connections_gauge = Gauge(
        f"{PROM_POOL_PREFIX}_idle_connections",
        "Number of connections not in use by this pool (unused pool connections)",
        PROM_POOL_LABELS,
        multiprocess_mode="livesum",
    )

    PROM_LABELS = [
        "sql_pool",
        "sql_address",
        "sql_database",
    ]

    latency_seconds = Histogram(
        f"{PROM_PREFIX}_latency_seconds",
        "Latency histogram of calls to database",
        PROM_LABELS + ["sql_success"],
        buckets=default_latency_buckets,
    )

    active_requests = Gauge(
        f"{PROM_PREFIX}_active_requests",
        "total requests that are in-flight",
        PROM_LABELS,
        multiprocess_mode="livesum",
    )

    requests_total = Counter(
        f"{PROM_PREFIX}_requests_total",
        "Total number of sql requests",
        PROM_LABELS + ["sql_success"],
    )

    def __init__(self, engine: Engine, name: str = "sqlalchemy"):
        self.engine = engine.execution_options()
        self.name = name
        event.listen(self.engine, "before_cursor_execute", self.on_before_execute, retval=True)
        event.listen(self.engine, "after_cursor_execute", self.on_after_execute)
        event.listen(self.engine, "handle_error", self.on_error)
        self.time_started = 0.0
        self.tracer = trace.get_tracer(__name__)
        self._leading_comment_remover = re.compile(r"^/\*.*?\*/")

        _w("sqlalchemy.engine.base", "Engine.connect", _wrap_connect(self.tracer))

    def _operation_name(self, db_name, statement):
        parts = []
        if isinstance(statement, str):
            parts.append(self._leading_comment_remover.sub("", statement).split()[0])
        if db_name:
            parts.append(db_name)
        if not parts:
            return _normalize_vendor(self.engine.name)
        return " ".join(parts)

    def report_runtime_metrics(self, batch: metrics.Client) -> None:
        pool = self.engine.pool
        if not isinstance(pool, QueuePool):
            return

        self.max_connections_gauge.labels(self.name).set(pool.size())
        self.checked_out_connections_gauge.labels(self.name).set(pool.checkedout())
        self.checked_in_connections_gauge.labels(self.name).set(pool.checkedin())

        batch.gauge("pool.size").replace(pool.size())
        batch.gauge("pool.open_and_available").replace(pool.checkedin())
        batch.gauge("pool.in_use").replace(pool.checkedout())
        batch.gauge("pool.overflow").replace(max(pool.overflow(), 0))

    def make_object_for_context(self, name: str, span: Span) -> Engine:
        engine = self.engine.execution_options(context_name=name, server_span=span)

        return engine

    def make_traced_object_for_context(
        self, name: str, span: trace.Span, legacy_span=None
    ) -> Engine:
        trace.set_span_in_context(span)
        return self.make_object_for_context(name, legacy_span)

    # pylint: disable=unused-argument, too-many-arguments
    def on_before_execute(
        self,
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Parameters,
        context: Optional[ExecutionContext],
        executemany: bool,
    ) -> Tuple[str, Parameters]:
        """Handle the engine's before_cursor_execute event."""
        labels = {
            "sql_pool": self.name,
            "sql_address": conn.engine.url.host,
            "sql_database": conn.engine.url.database,
        }
        self.active_requests.labels(**labels).inc()
        self.time_started = perf_counter()

        context_name = conn._execution_options["context_name"]
        server_span = conn._execution_options["server_span"]

        attrs, found = _get_attributes_from_url(conn.engine.url)
        if not found:
            attrs = _get_attributes_from_cursor(_normalize_vendor(self.engine.name), cursor, attrs)

        db_name = attrs.get(SpanAttributes.DB_NAME, "")
        trace_name = f"{context_name}.execute"
        span = server_span.make_child(trace_name)
        otelspan = self.tracer.start_span(
            self._operation_name(db_name, statement),
            kind=trace.SpanKind.CLIENT,
        )
        span.set_tag(
            "statement",
            statement[:1021] + "..." if len(statement) > 1024 else statement,
        )
        span.start()
        with trace.use_span(otelspan, end_on_exit=False):
            if otelspan.is_recording():
                otelspan.set_attribute(SpanAttributes.DB_STATEMENT, statement)
                otelspan.set_attribute(SpanAttributes.DB_SYSTEM, _normalize_vendor(self.name))
                for k, v in attrs.items():
                    otelspan.set_attribute(k, v)
            commenter_data = {
                "db_driver": conn.engine.driver,
                "db_framework": f"sqlalchemy:{sqlalchemy.__version__}",
            }
            commenter_data.update(**_get_opentelemetry_values())

            statement = _add_sql_comment(statement, **commenter_data)

        context._otel_span = otelspan

        conn.info["span"] = span

        return statement, parameters

    # pylint: disable=unused-argument, too-many-arguments
    def on_after_execute(
        self,
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Parameters,
        context: Optional[ExecutionContext],
        executemany: bool,
    ) -> None:
        """Handle the event which happens after successful cursor execution."""
        conn.info["span"].finish()
        conn.info["span"] = None

        labels = {
            "sql_pool": self.name,
            "sql_address": conn.engine.url.host,
            "sql_database": conn.engine.url.database,
        }

        self.active_requests.labels(**labels).dec()
        self.requests_total.labels(**labels, sql_success="true").inc()
        self.latency_seconds.labels(**labels, sql_success="true").observe(
            perf_counter() - self.time_started
        )

        otelspan = getattr(context, "_otel_span", None)
        if otelspan is None:
            return

        otelspan.end()

    def on_error(self, context: ExceptionContext) -> None:
        """Handle the event which happens on exceptions during execution."""
        if "span" in context.connection.info and context.connection.info["span"] is not None:
            exc_info = (type(context.original_exception), context.original_exception, None)
            context.connection.info["span"].finish(exc_info=exc_info)
            context.connection.info["span"] = None

        labels = {
            "sql_pool": self.name,
            "sql_address": context.connection.engine.url.host,
            "sql_database": context.connection.engine.url.database,
        }

        self.active_requests.labels(**labels).dec()
        self.requests_total.labels(**labels, sql_success="false").inc()
        self.latency_seconds.labels(**labels, sql_success="false").observe(
            perf_counter() - self.time_started
        )

        otelspan = getattr(context.execution_context, "_otel_span", None)
        if otelspan is None:
            return

        if otelspan.is_recording():
            otelspan.set_status(
                Status(
                    StatusCode.ERROR,
                    str(context.original_exception),
                )
            )
        otelspan.end()


class SQLAlchemySessionContextFactory(SQLAlchemyEngineContextFactory):
    """SQLAlchemy ORM session context factory.

    This factory will attach a new SQLAlchemy
    :py:class:`sqlalchemy.orm.session.Session` to an attribute on the
    :py:class:`~baseplate.RequestContext`. All cursor (query) execution will
    automatically record diagnostic information.

    The session will be automatically closed, but not committed or rolled back,
    at the end of each request.

    .. seealso::

        The session is part of the high-level SQLAlchemy ORM API. If you want
        to do raw queries, consider using
        :py:class:`~baseplate.clients.sqlalchemy.SQLAlchemyEngineContextFactory`
        instead.

    :param engine: A configured SQLAlchemy engine.

    """

    def make_object_for_context(self, name: str, span: Span) -> Session:
        engine = super().make_object_for_context(name, span)
        session = Session(bind=engine)
        span.register(SQLAlchemySessionSpanObserver(session))
        return session

    def make_traced_object_for_context(
        self, name: str, span: trace.Span, legacy_span=None
    ) -> Session:
        trace.set_span_in_context(span)
        return self.make_object_for_context(name, legacy_span)


class SQLAlchemySessionSpanObserver(SpanObserver):
    """Automatically close the session at the end of each request."""

    def __init__(self, session: Session):
        self.session = session

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        self.session.close()
