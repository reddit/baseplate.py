from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.context import ContextFactory, ContextObserver, ContextSpanObserver
from baseplate.core import (
    LocalSpan,
    Span,
)
from baseplate.integration import WrappedRequestContext
from baseplate.integration.thrift import RequestContext

from ... import mock


class ContextObserverTests(unittest.TestCase):
    def test_add_to_context(self):
        mock_factory = mock.Mock(spec=ContextFactory)
        mock_context = RequestContext()
        mock_span = mock.Mock(spec=Span)

        observer = ContextObserver("some_attribute", mock_factory)
        observer.on_server_span_created(mock_context, mock_span)

        self.assertEqual(mock_context.some_attribute,
            mock_factory.make_object_for_context.return_value)

    def test_add_to_context_local(self):
        mock_factory = mock.Mock(spec=ContextFactory)
        mock_context = mock.Mock()
        mock_context = WrappedRequestContext(mock_context)
        mock_local_span = mock.Mock(spec=LocalSpan)
        mock_local_span.component_name = 'test_component'
        mock_local_span.context = mock_context
        observer = ContextSpanObserver("some_attribute", mock_factory)
        observer.on_child_span_created(mock_local_span)

        self.assertEqual(mock_context.some_attribute,
            mock_factory.make_object_for_context.return_value)
