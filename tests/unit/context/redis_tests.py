from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    import redis
except ImportError:
    raise unittest.SkipTest("redis-py is not installed")
else:
    del redis

from baseplate.config import ConfigurationError
from baseplate.context.redis import pool_from_config


class PoolFromConfigTests(unittest.TestCase):
    def test_empty_config(self):
        with self.assertRaises(ConfigurationError):
            pool_from_config({})

    def test_basic_url(self):
        pool = pool_from_config({
            "redis.url": "redis://localhost:1234/0",
        })

        self.assertEqual(pool.connection_kwargs["host"], "localhost")
        self.assertEqual(pool.connection_kwargs["port"], 1234)
        self.assertEqual(pool.connection_kwargs["db"], 0)

    def test_timeouts(self):
        pool = pool_from_config({
            "redis.url": "redis://localhost:1234/0",
            "redis.socket_timeout": "30 seconds",
            "redis.socket_connect_timeout": "300 milliseconds",
        })

        self.assertEqual(pool.connection_kwargs["socket_timeout"], 30)
        self.assertEqual(pool.connection_kwargs["socket_connect_timeout"], .3)

    def test_max_connections(self):
        pool = pool_from_config({
            "redis.url": "redis://localhost:1234/0",
            "redis.max_connections": "300",
        })

        self.assertEqual(pool.max_connections, 300)

    def test_kwargs_passthrough(self):
        pool = pool_from_config({
            "redis.url": "redis://localhost:1234/0",
        }, example="present")

        self.assertEqual(pool.connection_kwargs["example"], "present")

    def test_alternate_prefix(self):
        pool_from_config({
            "noodle.url": "redis://localhost:1234/0",
        }, prefix="noodle.")
