import unittest

from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

try:
    from sqlalchemy import Column, Integer, String
    from sqlalchemy.dialects.sqlite import BOOLEAN
    from sqlalchemy.exc import OperationalError, StatementError
    from sqlalchemy.ext.declarative import declarative_base
except ImportError:
    raise unittest.SkipTest("sqlalchemy is not installed")

from baseplate.clients.sqlalchemy import (
    engine_from_config,
    SQLAlchemyEngineContextFactory,
    SQLAlchemySession,
    SQLAlchemySessionContextFactory,
)
from baseplate import Baseplate

from . import TestBaseplateObserver


Base = declarative_base()


class TestObject(Base):
    __tablename__ = "test"

    id = Column(Integer, primary_key=True)
    flag = Column(BOOLEAN)
    name = Column(String)


class SQLAlchemyEngineTests(TestBase):
    def setUp(self):
        super().setUp()
        self.tracer = trace.get_tracer(__name__)
        engine = engine_from_config({"database.url": "sqlite://"})  # in-memory db
        Base.metadata.create_all(bind=engine)
        factory = SQLAlchemyEngineContextFactory(engine)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("db", factory)

        self.context = baseplate.make_context_object()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple_query(self):
        with self.tracer.start_as_current_span("foo") as s:
            with self.server_span:
                self.server_span.context.db.execute("SELECT * FROM test;")

        finished = self.get_finished_spans()
        select_span = finished[1]
        connect_span = finished[0]
        parent_span = finished[2]
        self.assertEqual("SELECT", select_span.name)
        self.assertEqual(trace.SpanKind.CLIENT, select_span.kind)
        self.assertEqual(parent_span.get_span_context().span_id, select_span.parent.span_id)
        self.assertEqual("connect", connect_span.name)
        self.assertEqual(trace.SpanKind.CLIENT, connect_span.kind)
        self.assertEqual(parent_span.get_span_context().span_id, connect_span.parent.span_id)
        self.assertEqual(select_span.attributes["db.statement"], "SELECT * FROM test;")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
        span_observer.assert_tag("statement", "SELECT * FROM test")

    def test_very_long_query(self):
        with self.server_span, self.tracer.start_as_current_span("test_very_long_query"):
            self.server_span.context.db.execute("SELECT *" + (" " * 1024) + "FROM test;")

        finished = self.get_finished_spans()
        select_span = finished[1]
        connect_span = finished[0]
        parent_span = finished[2]
        self.assertEqual("SELECT", select_span.name)
        self.assertEqual(trace.SpanKind.CLIENT, select_span.kind)
        self.assertEqual(parent_span.get_span_context().span_id, select_span.parent.span_id)
        self.assertEqual("connect", connect_span.name)
        self.assertEqual(trace.SpanKind.CLIENT, connect_span.kind)
        self.assertEqual(parent_span.get_span_context().span_id, connect_span.parent.span_id)

        # for OTel we will handle attribute truncation via the tracing pipeline.
        self.assertSpanHasAttributes(
            select_span, {"db.statement": "SELECT *" + (" " * 1024) + "FROM test;"}
        )

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
        tag = span_observer.tags["statement"]
        assert len(tag) <= 1024

    def test_nested_local_span(self):
        with self.server_span:
            with self.server_span.make_child(
                "local", local=True, component_name="example"
            ) as local:
                local.context.db.execute("SELECT * FROM test;")

        server_span_observer = self.baseplate_observer.get_only_child()
        local_span_observer = server_span_observer.get_only_child()
        span_observer = local_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
        span_observer.assert_tag("statement", "SELECT * FROM test;")

    def test_error_in_query(self):
        with self.server_span:
            with self.assertRaises(OperationalError):
                self.context.db.execute("SELECT * FROM does_not_exist;")

        finished = self.get_finished_spans()
        select_span = finished[1]
        self.assertEqual("SELECT", select_span.name)
        self.assertEqual(trace.SpanKind.CLIENT, select_span.kind)
        self.assertFalse(select_span.status.is_ok)
        self.assertEqual(select_span.status.description, "no such table: does_not_exist")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)


class SQLAlchemySessionTests(TestBase):
    def setUp(self):
        super().setUp()
        engine = engine_from_config({"database.url": "sqlite://"})  # in-memory db
        Base.metadata.create_all(bind=engine)
        factory = SQLAlchemySessionContextFactory(engine)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("db", factory)

        self.context = baseplate.make_context_object()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple_session(self):
        with self.server_span:
            new_object = TestObject(name="cool")
            self.context.db.add(new_object)
            self.context.db.commit()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)

        finished = self.get_finished_spans()
        span = finished[1]
        self.assertEqual(span.kind, trace.SpanKind.CLIENT)
        self.assertEqual(span.name, "INSERT")
        self.assertTrue(span.status.is_ok)

    def test_error_before_query_execution(self):
        with self.server_span:
            with self.assertRaises(StatementError):
                query = self.context.db.query(TestObject)
                query.filter(TestObject.flag == {"v": 123}).all()

        server_span_observer = self.baseplate_observer.get_only_child()
        self.assertEqual(len(server_span_observer.children), 0)

        finished = self.get_finished_spans()
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0].name, "connect")


class SQLAlchemySessionConfigTests(unittest.TestCase):
    def test_simple_config(self):
        baseplate = Baseplate({"db.url": "sqlite://"})
        baseplate.configure_context({"db": SQLAlchemySession()})

        context = baseplate.make_context_object()
        with baseplate.make_server_span(context, "test"):
            context.db.execute("SELECT 1;")
