"""Helpers for integration with various application frameworks.

This package contains modules which integrate Baseplate with common
application frameworks.

"""
from .wrapped_context import WrappedRequestContext


class _LazyAttributesMixin(object):
    """A mixin that allows instances to add a lazily evaluated attribute
       through `add_lazy_property`.
    """

    def __init__(self, *args, **kwargs):
        super(_LazyAttributesMixin, self).__init__(*args, **kwargs)
        self._lazy_attributes = {}

    # Called when an attribute is missing
    def __getattr__(self, name):
        try:
            lazy_fn = self._lazy_attributes[name]
        except KeyError:
            return super(_LazyAttributesMixin, self).__getattr__(name)

        prop_value = lazy_fn(self)
        setattr(self, name, prop_value)
        return prop_value

    def add_lazy_attribute(self, name, lazy_fn):
        """Add a lazily evaluated attribute to this instance.

        :param str name: The name of the attribute.
        :param func lazy_fn: The function to invoke when the attribute is fist accessed.
            This function will receive the instance as its first argument.

        """
        self._lazy_attributes[name] = lazy_fn


__all__ = [
    "WrappedRequestContext",
]
