import importlib
import queue
import unittest

import gevent.queue

from baseplate.server.monkey import is_gevent_patched
from baseplate.server.monkey import patch_stdlib_queues


class MonkeyPatchTests(unittest.TestCase):
    def test_patch_stdlib_queues(self):
        assert queue.LifoQueue is not gevent.queue.LifoQueue
        patch_stdlib_queues()
        assert queue.LifoQueue is gevent.queue.LifoQueue
        importlib.reload(queue)
        assert queue.LifoQueue is not gevent.queue.LifoQueue

    def test_is_gevent_patched(self):
        assert not is_gevent_patched()
        patch_stdlib_queues()
        assert is_gevent_patched()
        importlib.reload(queue)
