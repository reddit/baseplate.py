import collections
import time


SerializedBatch = collections.namedtuple("SerializedBatch", "count bytes")


class BatchFull(Exception):
    pass


class Batch:
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
        return SerializedBatch(count=len(self._items), bytes=b"[" + b",".join(self._items) + b"]")

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
