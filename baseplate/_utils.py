"""Internal library helpers."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


import functools
import warnings


def warn_deprecated(message):
    """Emit a deprecation warning from the caller.

    The stacktrace for the warning will point to the place where the function
    calling this was called, rather than in baseplate. This allows the user to
    most easily see where in _their_ code the deprecation is coming from.

    """
    warnings.warn(message, DeprecationWarning, stacklevel=3)


# cached_property is a renamed copy of pyramid.decorator.reify
# see debian/copyright for full license information
class cached_property(object):
    """Like @property but the function will only be called once per instance.

    When used as a method decorator, this will act like @property but instead
    of calling the function each time the attribute is accessed, instead it
    will only call it on first access and then replace itself on the instance
    with the return value of the first call.

    """
    def __init__(self, wrapped):
        self.wrapped = wrapped
        functools.update_wrapper(self, wrapped)

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        ret = self.wrapped(obj)
        setattr(obj, self.wrapped.__name__, ret)
        return ret
