from gevent import monkey


def patch_stdlib_queues() -> None:
    """It is a common pattern to use a queue as a connection pool.
    This pattern can leak connections if the queue is not patched by gevent
    Gevent doesn't patch most queues by default.
    https://github.com/gevent/gevent/issues/1875
    """
    import queue
    import gevent.queue

    monkey.patch_module(queue, gevent.queue, items=["Queue", "LifoQueue", "PriorityQueue"])


def gevent_is_patched() -> bool:
    """Returns true if gevent is patched, otherwise returns false."""
    return bool(monkey.saved)
