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

    """
    def __init__(self, path, parser):
        self._path = path
        self._parser = parser
        self._mtime = 0
        self._data = _NOT_LOADED

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
            file_changed = self._mtime < os.path.getmtime(self._path)
        except OSError:
            file_changed = False

        if self._data is _NOT_LOADED or file_changed:
            logger.debug("Loading %s.", self._path)

            try:
                with open(self._path, "r") as f:
                    self._data = self._parser(f)
                    self._mtime = os.fstat(f.fileno()).st_mtime
            except IOError as exc:
                raise WatchedFileNotAvailableError(self._path, exc)

        return self._data
