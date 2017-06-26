"""Helpers that integrate common client libraries with baseplate's diagnostics.

This package contains modules which integrate various client libraries with
Baseplate's instrumentation. When using these client library integrations,
trace information is passed on and metrics are collected automatically.

To use these helpers, use the
:py:meth:`~baseplate.core.Baseplate.add_to_context` method on your
application's :py:class:`~baseplate.core.Baseplate` object::

    client = SomeClient("server, server, server")
    baseplate.add_to_context("my_client", SomeContextFactory(client))

and then a context-aware version of the client will show up on the
:term:`context object` during requests::

    def my_handler(self, context):
        context.my_client.make_some_remote_call()

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ..core import (
    BaseplateObserver,
    LocalSpan,
)


class ContextFactory(object):
    """An interface for adding stuff to the context object.

    Objects implementing this interface can be passed to
    :py:meth:`~baseplate.core.Baseplate.add_to_context`. The return value of
    :py:meth:`make_object_for_context` will be added to the :term:`context
    object` with the name specified in ``add_to_context``.

    """

    def make_object_for_context(self, name, server_span):
        """Return an object that can be added to the context object."""
        raise NotImplementedError


class ContextObserver(BaseplateObserver):
    def __init__(self, name, context_factory):
        self.name = name
        self.context_factory = context_factory

    def on_server_span_created(self, context, server_span):
        context_attr = self.context_factory.make_object_for_context(self.name, server_span)
        setattr(context, self.name, context_attr)

    def on_child_span_created(self, child_span):
        if isinstance(child_span, LocalSpan):
            context_attr = self.context_factory.make_object_for_context(self.name, child_span)
            child_span.context.shadow_context_attr(self.name, context_attr)
