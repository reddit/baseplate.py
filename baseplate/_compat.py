"""Python 3 compatibility."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys


if sys.version_info.major == 3:  # pragma: nocover
    import configparser
    import queue
else:  # pragma: nocover
    import ConfigParser as configparser
    import Queue as queue


__all__ = [
    "configparser",
    "queue",
]
