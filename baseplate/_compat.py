# pylint: disable=import-error
"""Python 3 compatibility."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys


if sys.version_info.major == 3:  # pragma: nocover
    import configparser
    import queue
    from io import BytesIO
else:  # pragma: nocover
    import ConfigParser as configparser
    import Queue as queue
    from cStringIO import StringIO as BytesIO


__all__ = [
    "configparser",
    "queue",
    "BytesIO",
]
