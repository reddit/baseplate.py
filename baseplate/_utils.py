"""Internal library helpers."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


import collections
import functools
import time
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


SerializedBatch = collections.namedtuple("SerializedBatch", "count bytes")


class BatchFull(Exception):
    pass


class Batch(object):
    def add(self, item):
        raise NotImplementedError

    def serialize(self):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError


class RawJSONBatch(Batch):
    def __init__(self, max_size):
        self.max_size = max_size
        self.reset()

    def add(self, item):
        if not item:
            return

        serialized_size = len(item) + 1  # the comma at the end

        if self._size + serialized_size > self.max_size:
            raise BatchFull

        self._items.append(item)
        self._size += serialized_size

    def serialize(self):
        return SerializedBatch(
            count=len(self._items),
            bytes=b"[" + b",".join(self._items) + b"]",
        )

    def reset(self):
        self._items = []
        self._size = 2  # the [] that wrap the json list


class TimeLimitedBatch(Batch):
    def __init__(self, inner, max_age):
        self.batch = inner
        self.batch_start = None
        self.max_age = max_age

    @property
    def age(self):
        if not self.batch_start:
            return 0
        return time.time() - self.batch_start

    def add(self, item):
        if self.age >= self.max_age:
            raise BatchFull

        self.batch.add(item)

        if not self.batch_start:
            self.batch_start = time.time()

    def serialize(self):
        return self.batch.serialize()

    def reset(self):
        self.batch.reset()
        self.batch_start = None
