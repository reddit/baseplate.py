import typing

from typing import Tuple
from typing import Type
from typing import Union

from baseplate.lib.file_watcher import _NOT_LOADED
from baseplate.lib.file_watcher import FileWatcher
from baseplate.lib.file_watcher import T
from baseplate.lib.file_watcher import WatchedFileNotAvailableError


class FakeFileWatcher(FileWatcher):
    """Fake file watcher for testing purposes.

    Use this in place of a :py:class:`~baseplate.lib.file_watcher.FileWatcher`
    in tests to avoid having to load an actual file:

    .. testsetup::

        from baseplate.testing.lib.file_watcher import FakeFileWatcher

    .. doctest::

        >>> file_watcher = FakeFileWatcher()
        >>> file_watcher.get_data()
        Traceback (most recent call last):
        baseplate.lib.file_watcher.WatchedFileNotAvailableError: /fake-file-watcher: no value set
        >>> file_watcher.data = "test"
        >>> file_watcher.get_data()
        'test'

    .. versionadded:: 1.5

    """

    # pylint: disable=super-init-not-called
    def __init__(self, data: Union[T, Type[_NOT_LOADED]] = _NOT_LOADED, mtime: float = 1234):
        self.data = data
        self.mtime = mtime

    def get_data_and_mtime(self) -> Tuple[T, float]:
        if self.data is _NOT_LOADED:
            raise WatchedFileNotAvailableError("/fake-file-watcher", Exception("no value set"))
        return typing.cast(T, self.data), self.mtime
