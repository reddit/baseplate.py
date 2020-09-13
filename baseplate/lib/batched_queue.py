from datetime import timedelta
from queue import Queue
from typing import Generic
from typing import List
from typing import Sequence
from typing import TYPE_CHECKING
from typing import TypeVar

from baseplate.lib.timer import Timer

T = TypeVar("T")

if TYPE_CHECKING:
    WorkQueue = Queue[Sequence[T]]  # pylint: disable=unsubscriptable-object
else:
    WorkQueue = Queue


class BatchedQueue(Generic[T]):
    """A queue which collects and placed items in batches."""

    def __init__(self, work_queue: WorkQueue, batch_size: int, flush_interval: timedelta) -> None:
        assert batch_size >= 1
        assert work_queue is not None
        self._work_queue = work_queue
        self._batch: List[T] = []
        self._batch_size = batch_size
        self._flusher: Timer = Timer(self._flush, flush_interval)

    def put(self, data: T) -> None:
        self._batch.append(data)
        if len(self._batch) >= self._batch_size:
            self._flush()
        else:
            self._flusher.start()

    def _flush(self) -> None:
        batch = self.drain()
        self._work_queue.put(batch)

    def drain(self) -> List[T]:
        self._flusher.stop()
        batch = list(self._batch)
        self._batch = []
        return batch
