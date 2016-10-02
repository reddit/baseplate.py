from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.context import ContextFactory, ContextObserver
from baseplate.core import Span

from ... import mock


class ContextObserverTests(unittest.TestCase):
    def test_add_to_context(self):
        mock_factory = mock.Mock(spec=ContextFactory)
        mock_context = mock.Mock()
        mock_span = mock.Mock(spec=Span)

        observer = ContextObserver("some_attribute", mock_factory)
        observer.on_server_span_created(mock_context, mock_span)

        self.assertEqual(mock_context.some_attribute,
            mock_factory.make_object_for_context.return_value)
