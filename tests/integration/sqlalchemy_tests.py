import unittest

try:
    from sqlalchemy import Column, Integer, String
    from sqlalchemy.exc import OperationalError
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
    name = Column(String)


class SQLAlchemyEngineTests(unittest.TestCase):
    def setUp(self):
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
        with self.server_span:
            self.server_span.context.db.execute("SELECT * FROM test;")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
        span_observer.assert_tag("statement", "SELECT * FROM test;")

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

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)


class SQLAlchemySessionTests(unittest.TestCase):
    def setUp(self):
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


class SQLAlchemySessionConfigTests(unittest.TestCase):
    def test_simple_config(self):
        baseplate = Baseplate({"db.url": "sqlite://"})
        baseplate.configure_context({"db": SQLAlchemySession()})

        context = baseplate.make_context_object()
        with baseplate.make_server_span(context, "test"):
            context.db.execute("SELECT 1;")
