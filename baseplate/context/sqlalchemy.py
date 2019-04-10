from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import create_url, URL
from sqlalchemy.orm import Session

from baseplate import config
from baseplate.context import ContextFactory
from baseplate.core import ServerSpan, SpanObserver


def engine_from_config(app_config, secrets=None, prefix="database.", **kwargs):
    """Make an Engine from a configuration dictionary.

    The keys useful to :py:func:`engine_from_config` should be prefixed, e.g.
    ``database.drivername``, ``database.username``, etc. The ``prefix`` argument
    specifies the prefix used to filter keys.  Each key is mapped to a
    corresponding keyword argument on the :py:class:`sqlalchemy.engine.url.URL`
    constructor.  The :py:class:`sqlalchemy.engine.url.URL` object will then be
    passed to :py:func:`sqlalchemy.create_engine`.  Any additional keyword arguments
    that are passed in will override the values set in ``app_config`` that are
    passed to the :py:class:`sqlalchemy.engine.url.URL` constructor.

    Supported keys:

    * ``url`` (optional): the connection URL to the database, if this is supplied, it
        will be the only value used to connect to the database, all others will be
        ignored.
    * ``drivername`` (optional):  the name of the database backend. This name will
        correspond to a module in sqlalchemy/databases or a third party plug-in e.g.
        "postgresql".  Note that this is only optional if ``url`` is set, otherwise
        it will be required.
    * ``username`` (optional): the username used to connect to the database.
    * ``password_secret`` (optional): the key used to retrieve the password from
        ``secrets`` as a :py:class:`baseplate.secrets.SimpleSecret`.
    * ``host`` (optional): the name of the database host.
    * ``port`` (optional): the port number that the database is listening on on
    the host.
    * ``database`` (optional): the database name.
    * ``query`` (optional): a dictionary of options to be passed to the dialect
        and/or the DBAPI upon connect.

    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "url": config.Optional(config.String),
            "drivername": config.Optional(config.String),
            "credentials_secret": config.Optional(config.String),
            "host": config.Optional(config.String),
            "port": config.Optional(config.Integer),
            "database": config.Optional(config.String),
            "query": config.DictOf(config.String),  # optional
        }})
    options = getattr(cfg, config_prefix)
    credentials = None
    if options.credentials_secret:
        if not secrets:
            raise TypeError("'secrets' is a required argument to 'engine_from_config' "
                            "if 'credentials_secret' is set")
        credentials = secrets.get_credentials(options.credentials_secret)

    if options.url:
        connection_url = create_url(options.url)
        if credentials:
            connection_url.username = credentials.username
            connection_url.password = credentials.password
        return create_engine(connection_url)

    elif not options.drivername:
        raise config.ConfigurationError(
            "drivername",
            AttributeError("'drivername' is required if 'url' is not set"),
        )

    kwargs.setdefault("drivername", options.drivername)
    if credentials:
        kwargs.setdefault("username", credentials.username)
        kwargs.setdefault("password", credentials.password)
    if options.host:
        kwargs.setdefault("host", options.host)
    if options.port:
        kwargs.setdefault("port", options.port)
    if options.database:
        kwargs.setdefault("database", options.database)
    if options.query:
        kwargs.setdefault("query", options.query)
    return create_engine(URL(**kwargs))


class SQLAlchemyEngineContextFactory(ContextFactory):
    """SQLAlchemy core engine context factory.

    This factory will attach a SQLAlchemy :py:class:`sqlalchemy.engine.Engine`
    to an attribute on the :term:`context object`. All cursor (query) execution
    will automatically record diagnostic information.

    Additionally, the trace and span ID will be added as a comment to the text
    of the SQL statement. This is to aid correlation of queries with requests.

    .. seealso::

        The engine is the low-level SQLAlchemy API. If you want to use the ORM,
        consider using
        :py:class:`~baseplate.context.sqlalchemy.SQLAlchemySessionContextFactory`
        instead.

    :param sqlalchemy.engine.Engine engine: A configured SQLAlchemy engine.

    """

    def __init__(self, engine):
        self.engine = engine.execution_options()
        event.listen(self.engine, "before_cursor_execute", self.on_before_execute, retval=True)
        event.listen(self.engine, "after_cursor_execute", self.on_after_execute)
        event.listen(self.engine, "dbapi_error", self.on_dbapi_error)

    def make_object_for_context(self, name, span):
        engine = self.engine.execution_options(
            context_name=name,
            server_span=span,
        )
        return engine

    # pylint: disable=unused-argument, too-many-arguments
    def on_before_execute(self, conn, cursor, statement, parameters, context, executemany):
        """Handle the engine's before_cursor_execute event."""
        context_name = conn._execution_options["context_name"]
        server_span = conn._execution_options["server_span"]

        trace_name = "{}.{}".format(context_name, "execute")
        span = server_span.make_child(trace_name)
        span.set_tag("statement", statement)
        span.start()

        conn.info["span"] = span

        # add a comment to the sql statement with the trace and span ids
        # this is useful for slow query logs and active query views
        annotated_statement = "{} -- trace:{:d},span:{:d}".format(
            statement, span.trace_id, span.id)
        return annotated_statement, parameters

    # pylint: disable=unused-argument, too-many-arguments
    def on_after_execute(self, conn, cursor, statement, parameters, context, executemany):
        """Handle the event which happens after successful cursor execution."""
        conn.info["span"].finish()
        conn.info["span"] = None

    def on_dbapi_error(self, conn, cursor, statement, parameters, context, exception):
        """Handle the event which happens on exceptions during execution."""
        exc_info = (type(exception), exception, None)
        conn.info["span"].finish(exc_info=exc_info)
        conn.info["span"] = None


class SQLAlchemySessionContextFactory(SQLAlchemyEngineContextFactory):
    """SQLAlchemy ORM session context factory.

    This factory will attach a new SQLAlchemy
    :py:class:`sqlalchemy.orm.session.Session` to an attribute on the
    :term:`context object`. All cursor (query) execution will automatically
    record diagnostic information.

    The session will be automatically closed, but not committed or rolled back,
    at the end of each request.

    .. seealso::

        The session is part of the high-level SQLAlchemy ORM API. If you want
        to do raw queries, consider using
        :py:class:`~baseplate.context.sqlalchemy.SQLAlchemyEngineContextFactory`
        instead.

    :param sqlalchemy.engine.Engine engine: A configured SQLAlchemy engine.

    """

    def make_object_for_context(self, name, span):
        if isinstance(span, ServerSpan):
            engine = super(SQLAlchemySessionContextFactory,
                           self).make_object_for_context(name, span)
            session = Session(bind=engine)
        else:
            # Reuse session in the existing context
            #  There should always be one passed down from the
            #  root ServerSpan
            session = getattr(span.context, name)
        span.register(SQLAlchemySessionSpanObserver(session, span))
        return session


class SQLAlchemySessionSpanObserver(SpanObserver):
    """Automatically close the session at the end of each request."""

    def __init__(self, session, span):
        self.session = session
        self.span = span

    def on_finish(self, exc_info):
        # A session is passed down to child local spans
        #   in a request pipeline so only close the session
        #  if the parent ServerSpan is closing.
        if isinstance(self.span, ServerSpan):
            self.session.close()
