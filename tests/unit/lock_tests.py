from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate.lock import (
    CacheLock,
    make_lock,
    TimeoutExpired,
    WrappedMemcache,
    WrappedRedis,
)
from pymemcache.client.base import PooledClient
from redis import StrictRedis

from .. import mock


class WrappedMemcacheTest(unittest.TestCase):
    def test_add_set(self):
        cache = mock.Mock()
        cache.add.return_value = True
        wrapped_memcache = WrappedMemcache(cache)

        ret = wrapped_memcache.add_if_unset('key', 'value', ttl=1)

        self.assertEqual(ret, True)
        cache.add.assert_called_once_with(
            'key', 'value', expire=1, noreply=False)

    def test_add_not_set(self):
        cache = mock.Mock()
        cache.add.return_value = False
        wrapped_memcache = WrappedMemcache(cache)

        ret = wrapped_memcache.add_if_unset('key', 'value', ttl=1)

        self.assertEqual(ret, False)
        cache.add.assert_called_once_with(
            'key', 'value', expire=1, noreply=False)

    def test_get(self):
        cache = mock.Mock()
        cache.get.return_value = 'value'
        wrapped_memcache = WrappedMemcache(cache)

        ret = wrapped_memcache.get('key')

        self.assertEqual(ret, 'value')
        cache.get.assert_called_once_with('key')

    def test_delete(self):
        cache = mock.Mock()
        wrapped_memcache = WrappedMemcache(cache)

        ret = wrapped_memcache.delete('key')

        self.assertEqual(ret, None)
        cache.delete.assert_called_once_with('key', noreply=False)


class WrappedRedisTest(unittest.TestCase):
    def test_add_set(self):
        cache = mock.Mock()
        cache.set.return_value = True
        wrapped_redis = WrappedRedis(cache)

        ret = wrapped_redis.add_if_unset('key', 'value', ttl=1)

        self.assertEqual(ret, True)
        cache.set.assert_called_once_with('key', 'value', nx=True, ex=1)

    def test_add_not_set(self):
        cache = mock.Mock()
        cache.set.return_value = None
        wrapped_redis = WrappedRedis(cache)

        ret = wrapped_redis.add_if_unset('key', 'value', ttl=1)

        self.assertEqual(ret, False)
        cache.set.assert_called_once_with('key', 'value', nx=True, ex=1)

    def test_get(self):
        cache = mock.Mock()
        cache.get.return_value = 'value'
        wrapped_redis = WrappedRedis(cache)

        ret = wrapped_redis.get('key')

        self.assertEqual(ret, 'value')
        cache.get.assert_called_once_with('key')

    def test_delete(self):
        cache = mock.Mock()
        wrapped_redis = WrappedRedis(cache)

        ret = wrapped_redis.delete('key')

        self.assertEqual(ret, None)
        cache.delete.assert_called_once_with('key')


class LockTest(unittest.TestCase):
    def setUp(self):
        self.wrapped_cache = mock.Mock()
        self.wrapped_cache.add_if_unset.return_value = True

    @mock.patch('time.sleep')
    @mock.patch('time.time', side_effect=[0, 1, 2, 3])
    @mock.patch('socket.gethostname', return_value='app-01')
    @mock.patch('os.getpid', return_value=123)
    def test_normal(self, os_getpid, socket_gethostname, time, sleep):
        # when we release the lock we check whether we still own it. make it
        # look like we do
        self.wrapped_cache.get.return_value = 'app-01.123'

        lock = CacheLock(
            wrapped_cache=self.wrapped_cache,
            key='key',
            hold_limit=10,
            wait_limit=2,
        )

        lock.acquire()
        self.assertTrue(lock.have_lock)
        self.wrapped_cache.add_if_unset.assert_called_once_with(
            'key', 'app-01.123', ttl=10)
        self.assertFalse(sleep.called)

        lock.release()
        self.assertFalse(lock.have_lock)
        self.wrapped_cache.get.assert_called_once_with('key')
        self.wrapped_cache.delete.assert_called_once_with('key')

    @mock.patch('time.sleep')
    @mock.patch('time.time', side_effect=[0, 1, 2, 3])
    @mock.patch('random.uniform', return_value=1)
    def test_wait_retry(self, random_uniform, time, sleep):
        # make the first attempt to add fail
        self.wrapped_cache.add_if_unset.side_effect = [False, True]

        lock = CacheLock(
            wrapped_cache=self.wrapped_cache,
            key='key',
            hold_limit=10,
            wait_limit=2,
        )

        lock.acquire()

        # time.time() is called twice: once to get the start time and once
        # after failing to get the lock to see how long the wait had been
        time.assert_has_calls([mock.call(), mock.call()])

        random_uniform.assert_called_once_with(0.1, 1)

        # we had to sleep for 1 second
        sleep.assert_called_once_with(1)
        self.assertTrue(lock.have_lock)

        lock.release()
        self.assertFalse(lock.have_lock)

    @mock.patch('time.sleep')
    @mock.patch('time.time')
    @mock.patch('socket.gethostname', return_value='app-01')
    @mock.patch('os.getpid', return_value=123)
    def test_wait_timeout(self, os_getpid, socket_gethostname, time, sleep):
        # make the first attempt to add fail
        self.wrapped_cache.add_if_unset.side_effect = [False, True]

        # make the second call to time.time() look like it's 20 seconds later
        time.side_effect = [0, 20]

        lock = CacheLock(
            wrapped_cache=self.wrapped_cache,
            key='key',
            hold_limit=10,
            wait_limit=2,
        )

        with self.assertRaises(TimeoutExpired):
            lock.acquire()

        self.wrapped_cache.add_if_unset.assert_called_once_with(
            'key', 'app-01.123', ttl=10)

        # time.time() is called twice: once to get the start time and once
        # after failing to get the lock to see how long the wait had been
        time.assert_has_calls([mock.call(), mock.call()])

        self.assertFalse(sleep.called)

    @mock.patch('time.sleep')
    @mock.patch('time.time', side_effect=[0, 1, 2, 3])
    @mock.patch('socket.gethostname', return_value='app-01')
    @mock.patch('os.getpid', return_value=123)
    def test_hold_expire(self, os_getpid, socket_gethostname, time, sleep):
        # when we release the lock we check whether we still own it. make it
        # look like we don't
        self.wrapped_cache.get.return_value = 'app-02.456'

        lock = CacheLock(
            wrapped_cache=self.wrapped_cache,
            key='key',
            hold_limit=10,
            wait_limit=2,
        )

        lock.acquire()
        self.assertTrue(lock.have_lock)
        self.wrapped_cache.add_if_unset.assert_called_once_with(
            'key', 'app-01.123', ttl=10)
        self.assertFalse(sleep.called)

        lock.release()
        self.assertFalse(lock.have_lock)
        self.wrapped_cache.get.assert_called_once_with('key')
        self.assertFalse(self.wrapped_cache.delete.called)


class MakeLockTest(unittest.TestCase):
    def test_pymemcache(self):
        cache = PooledClient('')
        lock = make_lock(cache, 'key')

        self.assertTrue(isinstance(lock, CacheLock))
        self.assertTrue(isinstance(lock.wrapped_cache, WrappedMemcache))

    def test_redis(self):
        cache = StrictRedis()
        lock = make_lock(cache, 'key')

        self.assertTrue(isinstance(lock, CacheLock))
        self.assertTrue(isinstance(lock.wrapped_cache, WrappedRedis))

    def test_other(self):
        cache = mock.Mock()

        with self.assertRaises(ValueError):
            make_lock(cache, 'key')
