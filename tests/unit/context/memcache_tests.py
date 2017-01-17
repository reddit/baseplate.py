from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    import pymemcache
except ImportError:
    raise unittest.SkipTest("pymemcache is not installed")
else:
    del pymemcache

from baseplate.config import ConfigurationError
from baseplate.context.memcache import pool_from_config


class PoolFromConfigTests(unittest.TestCase):
    def test_empty_config(self):
        with self.assertRaises(ConfigurationError):
            pool_from_config({})

    def test_basic_url(self):
        pool = pool_from_config({
            "memcache.endpoint": "localhost:1234",
        })

        self.assertEqual(pool.server[0], "localhost")
        self.assertEqual(pool.server[1], 1234)

    def test_timeouts(self):
        pool = pool_from_config({
            "memcache.endpoint": "localhost:1234",
            "memcache.timeout": 1.23,
            "memcache.connect_timeout": 4.56,
        })

        self.assertEqual(pool.timeout, 1.23)
        self.assertEqual(pool.connect_timeout, 4.56)

    def test_max_connections(self):
        pool = pool_from_config({
            "memcache.endpoint": "localhost:1234",
            "memcache.max_pool_size": 300,
        })

        self.assertEqual(pool.client_pool.max_size, 300)

    def test_alternate_prefix(self):
        pool_from_config({
            "noodle.endpoint": "localhost:1234",
        }, prefix="noodle.")
