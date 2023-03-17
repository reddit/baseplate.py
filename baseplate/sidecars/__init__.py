import time

from typing import List
from typing import NamedTuple
from typing import Optional


class SerializedBatch(NamedTuple):
    item_count: int
    serialized: bytes


class BatchFull(Exception):
    pass


class Batch:
    def add(self, item: Optional[bytes]) -> None:
        raise NotImplementedError

    def serialize(self) -> SerializedBatch:
        raise NotImplementedError

    def reset(self) -> None:
        raise NotImplementedError


class RawJSONBatch(Batch):
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.reset()

    def add(self, item: Optional[bytes]) -> None:
        if not item:
            return

        serialized_size = len(item) + 1  # the comma at the end

        if self._size + serialized_size > self.max_size:
            raise BatchFull

        self._items.append(item)
        self._size += serialized_size

    def serialize(self) -> SerializedBatch:
        return SerializedBatch(
            item_count=len(self._items), serialized=b"[" + b",".join(self._items) + b"]"
        )

    def reset(self) -> None:
        self._items: List[bytes] = []
        self._size = 2  # the [] that wrap the json list


class TimeLimitedBatch(Batch):
    def __init__(self, inner: Batch, max_age: float):
        self.batch = inner
        self.batch_start: Optional[float] = None
        self.max_age = max_age

    @property
    def age(self) -> float:
        if not self.batch_start:
            return 0
        return time.time() - self.batch_start

    def add(self, item: Optional[bytes]) -> None:
        if self.age >= self.max_age:
            raise BatchFull

        self.batch.add(item)

        if not self.batch_start:
            self.batch_start = time.time()

    def serialize(self) -> SerializedBatch:
        return self.batch.serialize()

    def reset(self) -> None:
        self.batch.reset()
        self.batch_start = None
