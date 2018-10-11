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
    """Watch a file and load its data when it changes.

    :param str path: Full path to a file to watch.
    :param callable parser: A callable that takes an open file object, parses
        or otherwise interprets the file, and returns whatever data is
        meaningful.
    :param float timeout: How long, in seconds, to block instantiation waiting
        for the watched file to become available (defaults to not blocking).

    """
    def __init__(self, path, parser, timeout=None):
        self._path = path
        self._parser = parser
        self._mtime = 0
        self._data = _NOT_LOADED

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
                with open(self._path, "r") as f:
                    self._data = self._parser(f)
            except Exception as exc:
                if self._data is _NOT_LOADED:
                    raise WatchedFileNotAvailableError(self._path, exc)
                logger.warning("%s: failed to load, using cached data: %s",
                               self._path, exc)
            self._mtime = current_mtime

        return self._data
