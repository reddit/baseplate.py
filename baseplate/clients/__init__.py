"""Helpers that integrate common client libraries with Baseplate.py.

This package contains modules which integrate various client libraries with
Baseplate.py's instrumentation. When using these client library integrations,
trace information is passed on and metrics are collected automatically.

"""
from typing import Any
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import baseplate.lib.metrics


class ContextFactory:
    """An interface for adding stuff to the context object.

    Objects implementing this interface can be passed to
    :py:meth:`~baseplate.Baseplate.add_to_context`. The return value of
    :py:meth:`make_object_for_context` will be added to the
    :py:class:`~baseplate.RequestContext` with the name specified in
    ``add_to_context``.

    """

    def report_runtime_metrics(self, batch: "baseplate.lib.metrics.Client") -> None:
        """Report runtime metrics to the stats system.

        :param batch: A metrics client to report statistics to.

        """

    def make_object_for_context(self, name: str, span: "baseplate.Span") -> Any:
        """Return an object that can be added to the context object.

        :param name: The name assigned to this object on the context.
        :param span: The current span this object is being made for.

        """
        raise NotImplementedError
