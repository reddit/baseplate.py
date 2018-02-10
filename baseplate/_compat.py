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
    from io import BytesIO, StringIO
    import pickle
    from urllib.parse import urlparse, urljoin, unquote, quote  # pylint: disable=no-name-in-module
    range = range
    string_types = str,
    long = int
    import builtins

    def iteritems(d):
        return iter(d.items())

else:  # pragma: nocover
    import ConfigParser as configparser
    import Queue as queue
    import cPickle as pickle
    from cStringIO import StringIO
    from urllib import unquote, quote
    from urlparse import urlparse, urljoin
    range = xrange
    string_types = basestring,
    long = long
    BytesIO = StringIO
    import __builtin__ as builtins

    def iteritems(d):
        return d.iteritems()


__all__ = [
    "builtins",
    "BytesIO",
    "configparser",
    "pickle",
    "queue",
    "quote",
    "range",
    "string_types",
    "unquote",
    "urljoin",
    "urlparse",
]
