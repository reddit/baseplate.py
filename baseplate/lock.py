"""Simple locks in memcached or redis."""
from __future__ import absolute_import

import os
import random
import socket
import time

from pymemcache.client.base import PooledClient
from redis import StrictRedis


class TimeoutExpired(Exception):
    pass


MIN_RETRY_WAIT = 0.1
MAX_RETRY_WAIT = 1.0


class WrappedMemcache(object):
    def __init__(self, cache):
        self.cache = cache

    def add_if_unset(self, key, value, ttl):
        did_add = self.cache.add(key, value, expire=ttl, noreply=False)
        return did_add

    def get(self, key):
        return self.cache.get(key)

    def delete(self, key):
        self.cache.delete(key, noreply=False)


class WrappedRedis(object):
    def __init__(self, cache):
        self.cache = cache

    def add_if_unset(self, key, value, ttl):
        did_add = self.cache.set(key, value, nx=True, ex=ttl)

        # StrictRedis returns True for 'set', and None for 'not set'
        return bool(did_add)

    def get(self, key):
        return self.cache.get(key)

    def delete(self, key):
        self.cache.delete(key)


class CacheLock(object):
    """A lock implemented on memcached or redis.

    Use the cache operation to add a key only if it doesn't exist. If
    the key is not present in the cache its value is set and the server
    responds that it was set. If the key is already present in the cache its
    value is not modified and the server responds that it was not set.

    We use this operation to ensure that only one client can obtain the lock
    at a time. The first client gets the "set" response and interprets that to
    mean that the lock was acquired. Any subsequent clients will get the
    "not set" response and interpret that to mean that some other process has
    the lock. They can wait and try again, or raise an exception.

    """

    def __init__(self, wrapped_cache, key, hold_limit, wait_limit):
        self.wrapped_cache = wrapped_cache
        self.key = key
        self.hold_limit = hold_limit
        self.wait_limit = wait_limit
        self.have_lock = False

    def acquire(self):
        """Attempt to acquire the lock.

        CacheLocks also support the `context manager protocol`_, for use with
        Python's ``with`` statement. When the context is entered, the lock
        calls :py:meth:`acquire` and when the context is exited it
        automatically calls :py:meth:`release`.

        :raises: :py:exc:`TimeoutExpired` if ``wait_limit`` is exceeded
            attempting to get the lock.

        .. _context manager protocol:
            https://docs.python.org/3/reference/datamodel.html#context-managers

        """
        self.nonce = "%s.%s" % (socket.gethostname(), os.getpid())

        start = time.time()
        did_add = False
        while not did_add:
            did_add = self.wrapped_cache.add_if_unset(
                self.key, self.nonce, ttl=self.hold_limit)

            if did_add:
                self.have_lock = True
                return

            wait_so_far = time.time() - start
            remaining_wait_budget = self.wait_limit - wait_so_far
            if remaining_wait_budget <= 0:
                raise TimeoutExpired
            else:
                # sleep before attempting to get the lock again
                sleep_time = min(
                    random.uniform(MIN_RETRY_WAIT, MAX_RETRY_WAIT),
                    remaining_wait_budget,
                )
                time.sleep(sleep_time)

    def release(self):
        """Release the lock."""
        if self.wrapped_cache.get(self.key) == self.nonce:
            self.wrapped_cache.delete(self.key)
        else:
            # the lock expired, don't delete someone else's lock
            pass

        self.have_lock = False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, type, value, tb):
        self.release()


def make_lock(cache, key, hold_limit=30, wait_limit=30):
    """Return a CacheLock for the given key and limits.

    :param cache:
        pymemcache client instance
        (:py:class:`pymemcache.client.base.PooledClient`) or
        redis client instance (:py:class:`redis.StrictRedis`).
        Typically in baseplate this is an instance of
        :py:class:`baseplate.context.memcache.MonitoredMemcacheConnection` or
        :py:class:`baseplate.context.redis.MonitoredRedisConnection`.
    :param str key: The lock's identifier.
    :param int hold_limit: The maximum lifetime of the lock, in seconds. The
        lock is deleted on successful exit, so this is mostly concerned with
        making sure the lock doesn't persist too long if the delete fails. On
        the other hand if the lifetime is too short the lock may expire (and
        be aquireable by other processes) before its owner is finished with it.
    :param int wait_limit: The maximum time (in seconds) to wait and retry if
        the lock isn't available.

    """

    if isinstance(cache, PooledClient):
        wrapped_cache = WrappedMemcache(cache)
    elif isinstance(cache, StrictRedis):
        wrapped_cache = WrappedRedis(cache)
    else:
        raise ValueError(
            "cache must be PooledClient or StrictRedis, got %s", cache)

    return CacheLock(wrapped_cache, key, hold_limit, wait_limit)
