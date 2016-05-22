from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    from sqlalchemy import create_engine, Column, Integer, String
    from sqlalchemy.ext.declarative import declarative_base
except ImportError:
    raise unittest.SkipTest("sqlalchemy is not installed")

from baseplate.context.sqlalchemy import SQLAlchemySessionContextFactory
from baseplate.core import RootSpan, Span

from .. import mock


Base = declarative_base()


class TestObject(Base):
    __tablename__ = "test"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class SQLAlchemyTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")  # in-memory db
        Base.metadata.create_all(bind=self.engine)
        self.factory = SQLAlchemySessionContextFactory(self.engine)

    def test_session(self):
        root_span = mock.Mock(autospec=RootSpan)
        span = mock.Mock(autospec=Span)
        span.id = 1234
        span.trace_id = 2345
        root_span.make_child.return_value = span
        session = self.factory.make_object_for_context("db", root_span)

        new_object = TestObject(name="cool")
        session.add(new_object)
        session.commit()

        self.assertEqual(root_span.make_child.call_args,
                         mock.call("db.execute"))
