from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from cassandra.cluster import _NOT_SET

from . import ContextFactory


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

    def make_object_for_context(self, name, root_span):
        return CassandraSessionAdapter(name, root_span, self.session)


def _on_execute_complete(rows, span):
    # TODO: annotate with anything from the result set?
    # TODO: annotate with any returned warnings
    span.stop()


def _on_execute_failed(exc, span):
    span.annotate("error", str(exc))
    span.stop()


class CassandraSessionAdapter(object):
    def __init__(self, context_name, root_span, session):
        self.context_name = context_name
        self.root_span = root_span
        self.session = session

    def execute(self, query, parameters=None, timeout=_NOT_SET):
        return self.execute_async(query, parameters, timeout).result()

    def execute_async(self, query, parameters=None, timeout=_NOT_SET):
        trace_name = "{}.{}".format(self.context_name, "query")
        span = self.root_span.make_child(trace_name)
        span.start()
        # TODO: include custom payload
        future = self.session.execute_async(query, parameters, timeout)
        future.add_callback(_on_execute_complete, span)
        future.add_errback(_on_execute_failed, span)
        return future

    def prepare(self, query):
        trace_name = "{}.{}".format(self.context_name, "prepare")
        with self.root_span.make_child(trace_name) as span:
            # TODO: include custom payload
            span.annotate("query", query)
            return self.session.prepare(query)
