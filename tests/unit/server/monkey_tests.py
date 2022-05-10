import importlib
import queue
import unittest

import gevent.monkey
import gevent.queue

from baseplate.server.monkey import gevent_is_patched
from baseplate.server.monkey import patch_stdlib_queues


class MonkeyPatchTests(unittest.TestCase):
    def tearDown(self):
        importlib.reload(queue)
        gevent.monkey.saved.clear()

    def test_patch_stdlib_queues(self):
        assert queue.LifoQueue is not gevent.queue.LifoQueue
        patch_stdlib_queues()
        assert queue.LifoQueue is gevent.queue.LifoQueue

    def test_is_gevent_patched(self):
        assert not gevent_is_patched()
        patch_stdlib_queues()
        assert gevent_is_patched()
