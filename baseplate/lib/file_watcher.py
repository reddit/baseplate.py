"""Watch a file and keep a parsed copy in memory that's updated on changes.

The contents of the file are re-loaded and parsed only when necessary.

For example, a JSON file like the following:

.. highlight:: ini

.. include:: ../../../watched_file.json
   :literal:

.. testsetup::

    import json
    from baseplate.lib.file_watcher import FileWatcher
    path = "docs/watched_file.json"

might be watched and parsed like this:

.. highlight:: py

.. doctest::

    >>> watcher = FileWatcher(path, parser=json.load)
    >>> watcher.get_data() == {u"one": 1, u"two": 2}
    True

The return value of :py:meth:`~baseplate.lib.file_watcher.FileWatcher.get_data`
would change whenever the underlying file changes.

"""
import logging
import os
import typing

from typing import Callable
from typing import Generic
from typing import IO
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

from baseplate.lib.retry import RetryPolicy


logger = logging.getLogger(__name__)

DEFAULT_FILEWATCHER_BACKOFF = 0.01


class _NOT_LOADED:
    pass


class WatchedFileNotAvailableError(Exception):
    """Raised when the watched file could not be loaded."""

    def __init__(self, path: str, inner: Union[Exception, str]):
        super().__init__(f"{path}: {inner}")
        self.path = path
        self.inner = inner


T = TypeVar("T")


class _OpenOptions(NamedTuple):
    mode: str
    encoding: Optional[str]
    newline: Optional[str]


class FileWatcher(Generic[T]):
    r"""Watch a file and load its data when it changes.

    :param path: Full path to a file to watch.
    :param parser: A callable that takes an open file object, parses
        or otherwise interprets the file, and returns whatever data is meaningful.
    :param timeout: How long, in seconds, to block instantiation
        waiting for the watched file to become available (defaults to not blocking).
    :param binary: Should the file be opened in binary mode. If
        `True` the file will be opened with the mode `"rb"`, otherwise it will be
        opened with the mode `"r"`. (defaults to `"r"`)
    :param encoding: The name of the encoding used to decode the file. The
        default encoding is platform dependent (whatever
        :py:func:`locale.getpreferredencoding` returns), but any text encoding
        supported by Python can be used.  This is not supported if `binary` is
        set to `True`.
    :param newline: Controls how universal newlines mode works
        (it only applies to text mode). It can be `None`, `""`, `"\\n"`, `"\\r"`,
        and `"\\r\\n"`.  This is not supported if `binary` is set to `True`.
    :param backoff: retry backoff time for the file watcher. Defaults to
        None, which is mapped to DEFAULT_FILEWATCHER_BACKOFF.

    """

    def __init__(
        self,
        path: str,
        parser: Callable[[IO], T],
        timeout: Optional[float] = None,
        binary: bool = False,
        encoding: Optional[str] = None,
        newline: Optional[str] = None,
        backoff: Optional[float] = None,
    ):
        if binary and encoding is not None:
            raise TypeError("'encoding' is not supported in binary mode.")

        if binary and newline is not None:
            raise TypeError("'newline' is not supported in binary mode.")

        self._path = path
        self._parser = parser
        self._mtime = 0.0
        self._data: Union[T, Type[_NOT_LOADED]] = _NOT_LOADED
        self._open_options = _OpenOptions(
            mode="rb" if binary else "r",
            encoding=encoding or ("UTF-8" if not binary else None),
            newline=newline,
        )

        backoff = backoff or DEFAULT_FILEWATCHER_BACKOFF

        if timeout is not None:
            last_error = None
            for _ in RetryPolicy.new(budget=timeout, backoff=backoff):
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
                last_error = typing.cast(WatchedFileNotAvailableError, last_error)
                raise WatchedFileNotAvailableError(
                    self._path, f"timed out. last error was: {last_error.inner}"
                )

    def get_data(self) -> T:
        """Return the current contents of the file, parsed.

        The watcher ensures that the file is re-loaded and parsed whenever its
        contents change. Parsing only occurs when necessary, not on each call
        to this method. This method returns whatever the most recent call to
        the parser returned.

        Make sure to call this method each time you need data from the file
        rather than saving its results elsewhere. This ensures you always have
        the freshest data.

        """
        return self.get_data_and_mtime()[0]

    def get_data_and_mtime(self) -> Tuple[T, float]:
        """Return tuple of the current contents of the file and file mtime.

        The watcher ensures that the file is re-loaded and parsed whenever its
        contents change. Parsing only occurs when necessary, not on each call
        to this method. This method returns whatever the most recent call to
        the parser returned.

        When file content was changed, it returns the recent mtime,
        notify the caller the content is different from previous cached.

        Make sure to call this method each time you need data from the file
        rather than saving its results elsewhere. This ensures you always have
        the freshest data.

        """
        try:
            current_mtime = os.path.getmtime(self._path)
        except OSError as exc:
            if self._data is _NOT_LOADED:
                raise WatchedFileNotAvailableError(self._path, exc)
            return typing.cast(T, self._data), self._mtime

        if self._mtime < current_mtime:
            logger.debug("Loading %s.", self._path)
            try:
                # pylint: disable=unspecified-encoding
                with open(self._path, **self._open_options._asdict()) as f:
                    self._data = self._parser(f)
            except Exception as exc:
                if self._data is _NOT_LOADED:
                    raise WatchedFileNotAvailableError(self._path, exc)
                logger.warning("%s: failed to load, using cached data: %s", self._path, exc)
            self._mtime = current_mtime

        return typing.cast(T, self._data), self._mtime
