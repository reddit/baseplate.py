"""Watch a file and keep a parsed copy in memory that's updated on changes.

The contents of the file are re-loaded and parsed only when necessary.

For example, a JSON file like the following:

.. highlight:: ini

.. include:: ../watched_file.json
   :literal:

.. testsetup::

    import json
    from baseplate.file_watcher import FileWatcher
    path = "docs/watched_file.json"

might be watched and parsed like this:

.. highlight:: py

.. doctest::

    >>> watcher = FileWatcher(path, parser=json.load)
    >>> watcher.get_data() == {u"one": 1, u"two": 2}
    True

The return value of :py:meth:`~baseplate.file_watcher.FileWatcher.get_data`
would change whenever the underlying file changes.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import sys

from baseplate.retry import RetryPolicy


logger = logging.getLogger(__name__)


_NOT_LOADED = object()


class WatchedFileNotAvailableError(Exception):
    """Raised when the watched file could not be loaded."""

    def __init__(self, path, inner):
        super(WatchedFileNotAvailableError, self).__init__(
            "{}: {}".format(path, inner))
        self.path = path
        self.inner = inner


class FileWatcher(object):
    r"""Watch a file and load its data when it changes.

    :param str path: Full path to a file to watch.
    :param callable parser: A callable that takes an open file object, parses
        or otherwise interprets the file, and returns whatever data is meaningful.
    :param float timeout: (Optional) How long, in seconds, to block instantiation
        waiting for the watched file to become available (defaults to not blocking).
    :param bool binary: (Optionaly) Should the file be opened in binary mode. If
        `True` the file will be opened with the mode `"rb"`, otherwise it will be
        opened with the mode `"r"`. (defaults to `"r"`)
    :param str encoding: (Optional) The name of the encoding used to decode
        the file. The default encoding is platform dependent (whatever
        locale.getpreferredencoding() returns), but any text encoding supported
        by Python can be used.  This is not supported in Python 2 or if `binary`
        is set to `True`.
    :param str newline: (Optional) Controls how universal newlines mode works
        (it only applies to text mode). It can be `None`, `""`, `"\\n"`, `"\\r"`,
        and `"\\r\\n"`.  This is not supported in Python 2 or if `binary` is set
        to `True`.

    """

    def __init__(self, path, parser, timeout=None, binary=False, encoding=None,
                 newline=None):
        if sys.version_info.major < 3 and encoding is not None:
            raise TypeError("'encoding' keyword argument for FileWatcher() is "
                            "not supported in Python 2")

        if sys.version_info.major < 3 and newline is not None:
            raise TypeError("'newline' keyword argument for FileWatcher() is "
                            "not supported in Python 2")

        if binary and encoding is not None:
            raise TypeError("'encoding' is not supported in binary mode.")

        if binary and newline is not None:
            raise TypeError("'newline' is not supported in binary mode.")

        self._path = path
        self._parser = parser
        self._mtime = 0
        self._data = _NOT_LOADED
        self._mode = "rb" if binary else "r"
        # Since Python 2 does not support these kwargs, we store them as a dict
        # that we `**` in the call to `open` in `get_data` so we do not have to
        # call `open` in different ways depending on the Python version.  This
        # can change if/when Python 2 support is dropped.
        self._open_options = {}

        if encoding:
            self._open_options['encoding'] = encoding

        if newline is not None:
            self._open_options['newline'] = newline

        if timeout is not None:
            last_error = None
            for _ in RetryPolicy.new(budget=timeout, backoff=0.01):
                if self._data is not _NOT_LOADED:
                    break

                try:
                    self.get_data()
                except WatchedFileNotAvailableError as exc:
                    last_error = exc
                else:
                    break

                logging.warning("%s: file not yet available. sleeping.", path)
            else:
                raise WatchedFileNotAvailableError(self._path,
                    "timed out. last error was: %s" % last_error.inner)

    def get_data(self):
        """Return the current contents of the file, parsed.

        The watcher ensures that the file is re-loaded and parsed whenever its
        contents change. Parsing only occurs when necessary, not on each call
        to this method. This method returns whatever the most recent call to
        the parser returned.

        Make sure to call this method each time you need data from the file
        rather than saving its results elsewhere. This ensures you always have
        the freshest data.

        """
        try:
            current_mtime = os.path.getmtime(self._path)
        except OSError as exc:
            if self._data is _NOT_LOADED:
                raise WatchedFileNotAvailableError(self._path, exc)
            return self._data

        if self._mtime < current_mtime:
            logger.debug("Loading %s.", self._path)
            try:
                with open(self._path, self._mode, **self._open_options) as f:
                    self._data = self._parser(f)
            except Exception as exc:
                if self._data is _NOT_LOADED:
                    raise WatchedFileNotAvailableError(self._path, exc)
                logger.warning("%s: failed to load, using cached data: %s",
                               self._path, exc)
            self._mtime = current_mtime

        return self._data
