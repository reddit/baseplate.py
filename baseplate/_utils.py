"""Internal library helpers."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


import warnings


def warn_deprecated(message):
    """Emit a deprecation warning from the caller.

    The stacktrace for the warning will point to the place where the function
    calling this was called, rather than in baseplate. This allows the user to
    most easily see where in _their_ code the deprecation is coming from.

    """
    warnings.warn(message, DeprecationWarning, stacklevel=3)
