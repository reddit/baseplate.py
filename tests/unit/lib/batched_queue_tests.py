from datetime import timedelta

from queue import Queue
from time import sleep
from typing import Sequence
from typing import TYPE_CHECKING

import pytest

from baseplate.lib.batched_queue import BatchedQueue

if TYPE_CHECKING:
    WorkQueue = Queue[Sequence[int]]
else:
    WorkQueue = Queue


@pytest.fixture
def queue() -> WorkQueue:
    return Queue()


class TestBatchedQueue:
    def test_drain(self, queue: WorkQueue) -> None:
        batched_queue: BatchedQueue[int] = BatchedQueue(queue, batch_size=5, flush_interval=timedelta(seconds=1))
        batched_queue.put(0)
        batched_queue.put(1)
        batched_queue.put(2)
        items = batched_queue.drain()
        assert len(items) == 3 and items[0] == 0 and items[1] == 1 and items[2] == 2

    def test_time_expiration(self, queue: WorkQueue) -> None:
        batched_queue: BatchedQueue[int] = BatchedQueue(queue, batch_size=5, flush_interval=timedelta(seconds=0.1))
        batched_queue.put(0)
        batched_queue.put(1)
        batched_queue.put(2)

        sleep(0.2)

        assert queue.qsize() == 1
        batch = queue.get(True, 0.1)
        assert len(batch) == 3 and batch[0] == 0 and batch[1] == 1 and batch[2] == 2

        items = batched_queue.drain()
        assert len(items) == 0

    def test_queue_limit(self, queue: WorkQueue) -> None:
        batched_queue: BatchedQueue[int] = BatchedQueue(queue, batch_size=2, flush_interval=timedelta(seconds=0.5))
        batched_queue.put(0)
        batched_queue.put(1)
        batched_queue.put(2)
        batched_queue.put(3)
        batched_queue.put(4)

        sleep(0.21)

        assert queue.qsize() == 2
        batch_1 = queue.get(True, 0.1)
        batch_2 = queue.get(True, 0.1)
        assert len(batch_1) == 2 and batch_1[0] == 0 and batch_1[1] == 1
        assert len(batch_2) == 2 and batch_2[0] == 2 and batch_2[1] == 3

        items = batched_queue.drain()
        assert len(items) == 1 and items[0] == 4

    def test_queue_limit_and_expiration(self, queue: WorkQueue) -> None:
        batched_queue: BatchedQueue[int] = BatchedQueue(queue, batch_size=3, flush_interval=timedelta(seconds=0.1))
        batched_queue.put(0)
        batched_queue.put(1)
        batched_queue.put(2)
        batched_queue.put(3)
        batched_queue.put(4)

        sleep(0.21)

        assert queue.qsize() == 2
        batch_1 = queue.get(True, 0.1)
        batch_2 = queue.get(True, 0.1)
        assert len(batch_1) == 3 and batch_1[0] == 0 and batch_1[1] == 1 and batch_1[2] == 2
        assert len(batch_2) == 2 and batch_2[0] == 3 and batch_2[1] == 4

        items = batched_queue.drain()
        assert len(items) == 0
