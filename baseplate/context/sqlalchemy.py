from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import threading

from sqlalchemy import event
from sqlalchemy.orm import Session

from ..context import ContextFactory
from ..core import ServerSpanObserver


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
            "sqlalchemy sessions cannot be used concurrently"

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
        """Handle the engine's after_cursor_execute event."""
        self.threadlocal.current_span.finish()
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
    def make_object_for_context(self, name, server_span):
        engine = super(SQLAlchemySessionContextFactory,
            self).make_object_for_context(name, server_span)
        session = Session(bind=engine)
        server_span.register(SQLAlchemySessionServerSpanObserver(session))
        return session


class SQLAlchemySessionServerSpanObserver(ServerSpanObserver):
    """Automatically close the session at the end of each request."""
    def __init__(self, session):
        self.session = session

    def on_finish(self, exc_info):
        self.session.close()
