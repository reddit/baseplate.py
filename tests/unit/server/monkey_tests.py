import importlib
import queue
import unittest

import gevent.queue

from baseplate.server.monkey import patch_stdlib_queues


class MonkeyPatchTests(unittest.TestCase):
    def test_patch_stdlib_queues(self):
        assert queue.LifoQueue is not gevent.queue.LifoQueue
        patch_stdlib_queues()
        assert queue.LifoQueue is gevent.queue.LifoQueue
        importlib.reload(queue)
        assert queue.LifoQueue is not gevent.queue.LifoQueue
