"""Simple bucketed rate limits in memcached or redis.

Break up time into fixed buckets and allow a maximum number of counts per
buckets.

Buckets are formed by dividing the 86400 seconds of the day into equal length
intervals.

Example:

If an interval length of 60 seconds is used there are 1440 buckets, one for
each minute. When the minute ticks over (e.g. from 12:31:59 to 12:32:00) the
count is reset because we use a new bucket.

"""

from __future__ import division

import datetime
import pytz

from pymemcache.client.base import PooledClient
from redis import StrictRedis


class WrappedMemcache(object):
    def __init__(self, cache):
        self.cache = cache

    def incr_and_get(self, key, ttl):
        self.cache.add(key, 0, expire=ttl)
        count = self.cache.incr(key, 1)
        return count


class WrappedRedis(object):
    def __init__(self, cache):
        self.cache = cache

    def incr_and_get(self, key, ttl):
        with self.cache.pipeline('incr') as pipe:
            pipe.incr(key, 1)
            pipe.expire(key, time=ttl)
            responses = pipe.execute()

        # extract the first response from the pipeline, which is the value of
        # the key that was incr'd
        count = responses[0]
        return count


def get_current_interval(interval_seconds):
    """Convert the current time to an interval.

    The 86400 seconds in a day are broken into into buckets of length
    ``interval_seconds``. The current interval is then the bucket that the
    current time falls into.

    """
    assert interval_seconds <= 86400, 'maximum interval is 1 day'

    now = datetime.datetime.now(pytz.UTC)
    seconds = (now.hour * 3600) + (now.minute * 60) + now.second

    # use floor division to get the current time's bucket
    interval = seconds // interval_seconds
    return interval


def _incr_and_get(wrapped_cache, key, interval_seconds=60, cache_prefix=''):
    interval = get_current_interval(interval_seconds)

    if cache_prefix:
        cache_key = '%s%s:%s' % (cache_prefix, interval, key)
    else:
        cache_key = '%s:%s' % (interval, key)

    # set the key to expire in 2 intervals (1 interval would be enough but
    # let's be a bit more careful)
    ttl = 2 * interval_seconds
    count = wrapped_cache.incr_and_get(cache_key, ttl)
    return count


def incr_and_get(cache, key, interval_seconds=60, cache_prefix=''):
    """Increment the counter for ``key`` and return its current value.

    The caller can then decide whether the current value is over the limit.

    :param cache:
        pymemcache client instance
        (:py:class:`pymemcache.client.base.PooledClient`) or
        redis client instance (:py:class:`redis.StrictRedis`).
        Typically in baseplate this is an instance of
        :py:class:`baseplate.context.memcache.MonitoredMemcacheConnection` or
        :py:class:`baseplate.context.redis.MonitoredRedisConnection`.
    :param str key: The rate limit's identifier.
    :param int interval_seconds: Bucket size in seconds.
    :param str cache_prefix: (optional) The prefix to prepend to cache keys.
        May be needed for prefix routing with mcrouter.

    """
    if isinstance(cache, PooledClient):
        wrapped_cache = WrappedMemcache(cache)
    elif isinstance(cache, StrictRedis):
        wrapped_cache = WrappedRedis(cache)
    else:
        raise ValueError(
            "cache must be PooledClient or StrictRedis, got %s", cache)

    assert interval_seconds >= 1, 'minimum interval is 1 second'
    assert interval_seconds <= 86400, 'maximum interval is 1 day'

    return _incr_and_get(wrapped_cache, key, interval_seconds, cache_prefix)
