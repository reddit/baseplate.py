from datetime import timedelta
from queue import Queue
from time import time
from typing import Generic
from typing import List
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING
from typing import TypeVar

T = TypeVar("T")

if TYPE_CHECKING:
    WorkQueue = Queue[Sequence[T]]  # pylint: disable=unsubscriptable-object
else:
    WorkQueue = Queue


class BatchedQueue(Generic[T]):
    """A queue which collects and places items in batches."""

    def __init__(self, work_queue: WorkQueue, batch_size: int, flush_interval: timedelta) -> None:
        assert batch_size >= 1
        assert work_queue is not None
        self._work_queue = work_queue
        self._batch_size: int = batch_size
        self._flush_interval: timedelta = flush_interval
        self._batch: List[T] = []
        self._last_flush: Optional[float] = None

    def put(self, data: T) -> None:
        self._batch.append(data)

        if self._last_flush is None:
            self._last_flush = time()

        self.flush()

    def drain(self) -> List[T]:
        batch = list(self._batch)
        self._batch = []
        return batch

    def flush(self, force: bool = False) -> None:
        if force or self._should_flush():
            self._flush()

    def _flush(self) -> None:
        batch = self.drain()
        self._work_queue.put(batch)
        self._last_flush = time()

    def _should_flush(self) -> bool:
        return self._batch_is_full() or self._flush_interval_has_expired()

    def _batch_is_full(self) -> bool:
        return len(self._batch) >= self._batch_size

    def _flush_interval_has_expired(self) -> bool:
        return (
            self._last_flush is not None
            and time() - self._last_flush >= self._flush_interval.total_seconds()
        )
