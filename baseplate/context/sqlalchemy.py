from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import threading

from sqlalchemy import event
from sqlalchemy.orm import Session

from ..context import ContextFactory
from ..core import (
    ServerSpan,
    SpanObserver,
)


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
        self.engine = engine

        # i'm not at all thrilled about this. is there another way to get
        # request context into the event handlers without "global" state?
        self.threadlocal = threading.local()

        event.listen(engine, "before_cursor_execute", self.on_before_execute, retval=True)
        event.listen(engine, "after_cursor_execute", self.on_after_execute)
        event.listen(engine, "dbapi_error", self.on_dbapi_error)

    def make_object_for_context(self, name, server_span):
        self.threadlocal.context_name = name
        self.threadlocal.server_span = server_span
        self.threadlocal.current_span = None
        return self.engine

    # pylint: disable=unused-argument, too-many-arguments
    def on_before_execute(self, conn, cursor, statement, parameters, context, executemany):
        """Handle the engine's before_cursor_execute event."""
        # http://docs.sqlalchemy.org/en/latest/orm/session_basics.html#is-the-session-thread-safe
        assert self.threadlocal.current_span is None, \
            "SQLAlchemy sessions cannot be used concurrently"

        trace_name = "{}.{}".format(self.threadlocal.context_name, "execute")
        span = self.threadlocal.server_span.make_child(trace_name)
        span.set_tag("statement", statement)
        span.start()
        self.threadlocal.current_span = span

        # add a comment to the sql statement with the trace and span ids
        # this is useful for slow query logs and active query views
        annotated_statement = "{} -- trace:{:d},span:{:d}".format(
            statement, span.trace_id, span.id)
        return annotated_statement, parameters

    # pylint: disable=unused-argument, too-many-arguments
    def on_after_execute(self, conn, cursor, statement, parameters, context, executemany):
        """Handle the event which happens after successful cursor execution."""
        self.threadlocal.current_span.finish()
        self.threadlocal.current_span = None

    def on_dbapi_error(self, conn, cursor, statement, parameters, context, exception):
        """Handle the event which happens on exceptions during execution."""
        exc_info = (type(exception), exception, None)
        self.threadlocal.current_span.finish(exc_info=exc_info)
        self.threadlocal.current_span = None


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
