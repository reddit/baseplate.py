from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import unittest

from baseplate import ratelimit
from pymemcache.client.base import PooledClient
import pytz
from redis import StrictRedis

from .. import mock


class WrappedMemcacheTest(unittest.TestCase):
    def test_incr_and_get(self):
        cache = mock.Mock()
        cache.incr.return_value = 3
        wrapped_memcache = ratelimit.WrappedMemcache(cache)

        ret = wrapped_memcache.incr_and_get('key', ttl=1)

        self.assertEqual(ret, 3)
        cache.add.assert_called_once_with('key', 0, expire=1)
        cache.incr.assert_called_once_with('key', 1)


class WrappedRedisTest(unittest.TestCase):
    def test_incr_and_get(self):
        pipeline_context = mock.Mock()
        pipeline_context.execute.return_value = [3]

        pipeline_mock = mock.Mock()
        pipeline_mock.__enter__ = mock.Mock(return_value=pipeline_context)

        def __exit__(*args, **kw):
            pass
        pipeline_mock.__exit__ = __exit__

        cache = mock.Mock()
        cache.pipeline.return_value = pipeline_mock

        wrapped_redis = ratelimit.WrappedRedis(cache)

        ret = wrapped_redis.incr_and_get('key', ttl=1)

        self.assertEqual(ret, 3)
        pipeline_context.incr.assert_called_with('key', 1)
        pipeline_context.expire.assert_called_with('key', time=1)
        pipeline_context.execute.call_count == 1


class RateLimitTest(unittest.TestCase):
    @mock.patch('baseplate.ratelimit.get_current_interval')
    def test_incr_and_get(self, get_current_interval):
        get_current_interval.return_value = 23
        wrapped_cache = mock.Mock()
        wrapped_cache.incr_and_get.return_value = 3

        result = ratelimit._incr_and_get(
            wrapped_cache=wrapped_cache,
            key='key',
            interval_seconds=10,
            cache_prefix='rl:',
        )

        self.assertEqual(result, 3)
        get_current_interval.assert_called_once_with(10)
        wrapped_cache.incr_and_get.assert_called_once_with('rl:23:key', 20)

    @mock.patch('baseplate.ratelimit.get_current_interval')
    def test_incr_and_get_no_cache_prefix(self, get_current_interval):
        get_current_interval.return_value = 23
        wrapped_cache = mock.Mock()
        wrapped_cache.incr_and_get.return_value = 3

        result = ratelimit._incr_and_get(
            wrapped_cache=wrapped_cache,
            key='key',
            interval_seconds=10,
        )

        self.assertEqual(result, 3)
        get_current_interval.assert_called_once_with(10)
        wrapped_cache.incr_and_get.assert_called_once_with('23:key', 20)

    @mock.patch('baseplate.ratelimit.datetime')
    def test_get_current_interval_0(self, ratelimit_datetime):
        now = datetime.datetime(2018, 2, 21, 0, 0, 0, tzinfo=pytz.UTC)
        ratelimit_datetime.datetime.now.return_value = now

        result = ratelimit.get_current_interval(60)

        self.assertEqual(result, 0)
        ratelimit_datetime.datetime.now.assert_called_once_with(pytz.UTC)

    @mock.patch('baseplate.ratelimit.datetime')
    def test_get_current_interval_1(self, ratelimit_datetime):
        now = datetime.datetime(2018, 2, 21, 3, 24, 59, tzinfo=pytz.UTC)
        ratelimit_datetime.datetime.now.return_value = now

        result = ratelimit.get_current_interval(60)

        self.assertEqual(result, 204)
        ratelimit_datetime.datetime.now.assert_called_once_with(pytz.UTC)

    @mock.patch('baseplate.ratelimit.datetime')
    def test_get_current_interval_2(self, ratelimit_datetime):
        now = datetime.datetime(2018, 2, 21, 23, 59, 59, tzinfo=pytz.UTC)
        ratelimit_datetime.datetime.now.return_value = now

        result = ratelimit.get_current_interval(60)

        self.assertEqual(result, 1439)
        ratelimit_datetime.datetime.now.assert_called_once_with(pytz.UTC)


class RateLimitWrapperTest(unittest.TestCase):
    @mock.patch('baseplate.ratelimit._incr_and_get')
    def test_pymemcache(self, _incr_and_get):
        cache = mock.create_autospec(PooledClient)

        result = ratelimit.incr_and_get(cache, 'key', 10, 'rl:')

    @mock.patch('baseplate.ratelimit._incr_and_get')
    def test_redis(self, _incr_and_get):
        cache = mock.create_autospec(StrictRedis)

        result = ratelimit.incr_and_get(cache, 'key', 10, 'rl:')

    @mock.patch('baseplate.ratelimit._incr_and_get')
    def test_other(self, _incr_and_get):
        cache = mock.Mock()

        with self.assertRaises(ValueError):
            ratelimit.incr_and_get(cache, 'key')
