from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from ... import mock

try:
    import pymemcache
except ImportError:
    raise unittest.SkipTest("pymemcache is not installed")
else:
    del pymemcache

from baseplate.config import ConfigurationError
from baseplate.context.memcache import pool_from_config
from baseplate.context.memcache import lib as memcache_lib


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


class BaseSerdeTests(unittest.TestCase):
    def test_serialize_str(self):
        value, flags = memcache_lib.python_memcache_serializer(
            key="key", value="val")
        self.assertEqual(value, "val")
        self.assertEqual(flags, 0)

    def test_serialize_int(self):
        value, flags = memcache_lib.python_memcache_serializer(
            key="key", value=100)
        self.assertEqual(value, "100")
        self.assertEqual(flags, memcache_lib.FLAG_INTEGER)

    def test_serialize_long(self):
        try:
            long
        except NameError:
            # python3
            value = 100
            expected_flags = memcache_lib.FLAG_INTEGER
        else:
            value = long(100)
            expected_flags = memcache_lib.FLAG_LONG

        value, flags = memcache_lib.python_memcache_serializer(
            key="key", value=value)
        self.assertEqual(value, "100")
        self.assertEqual(flags, expected_flags)

    def test_serialize_other(self):
        string_io_instance = mock.Mock()
        string_io_patch = mock.patch.object(memcache_lib, "StringIO",
            return_value=string_io_instance)
        string_io = string_io_patch.start()
        self.addCleanup(string_io_patch.stop)

        pickler_patch = mock.patch.object(memcache_lib.pickle, "Pickler")
        pickler = pickler_patch.start()
        self.addCleanup(pickler_patch.stop)

        value, flags = memcache_lib.python_memcache_serializer(
            key="key", value=("stuff", 1, False))

        pickler.assertCalledWith(string_io_instance, protocol=-1)
        pickler.dump.assertCalledWith(("stuff", 1, False))
        self.assertEqual(flags, memcache_lib.FLAG_PICKLE)

    def test_deserialize_str(self):
        value = memcache_lib.python_memcache_deserializer(
            key="key", value="val", flags=0)
        self.assertEqual(value, "val")

    def test_deserialize_int(self):
        value = memcache_lib.python_memcache_deserializer(
            key="key", value="100", flags=memcache_lib.FLAG_INTEGER)
        self.assertEqual(value, 100)

    def test_deserialize_long(self):
        value = memcache_lib.python_memcache_deserializer(
            key="key", value="100", flags=memcache_lib.FLAG_LONG)

        try:
            expected_class = long
        except NameError:
            # python3
            expected_class = int

        self.assertEqual(value, 100)
        self.assertTrue(isinstance(value, expected_class))

    def test_deserialize_other(self):
        string_io_instance = mock.Mock()
        string_io_patch = mock.patch.object(memcache_lib, "StringIO",
            return_value=string_io_instance)
        string_io = string_io_patch.start()
        self.addCleanup(string_io_patch.stop)

        unpickler_instance = mock.Mock()
        unpickler_patch = mock.patch.object(memcache_lib.pickle, "Unpickler",
            return_value=unpickler_instance)
        unpickler = unpickler_patch.start()
        self.addCleanup(unpickler_patch.stop)

        expected_value = object()
        unpickler_instance.load.return_value = expected_value

        value = memcache_lib.python_memcache_deserializer(
            key="key", value="garbage", flags=memcache_lib.FLAG_PICKLE)

        string_io.assertCalledWith("garbage")
        unpickler.assertCalledWith(string_io_instance)
        unpickler_instance.load.assertCalledOnce()
        self.assertEqual(value, expected_value)


class FullSerdeTests(unittest.TestCase):
    def test_serialize_no_compress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        serializer = memcache_lib.make_memcache_serializer(
            min_compress_length=0,  # disable compression
        )
        value, flags = serializer(key="key", value="simple string")
        self.assertEqual(value, "simple string")
        self.assertEqual(flags, 0)
        zlib.compress.assertNotCalled()

    def test_serialize_compress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_value = object()
        zlib.compress.return_value = expected_value

        serializer = memcache_lib.make_memcache_serializer(
            min_compress_length=1,
            compress_level=1,
        )
        value, flags = serializer(key="key", value="simple string")
        self.assertEqual(value, expected_value)
        self.assertEqual(flags, memcache_lib.FLAG_ZLIB)
        zlib.compress.assertCalledWith("simple string", 1)

    def test_deserialize_no_decompress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        value = memcache_lib.memcache_deserializer(
            key="key", value="stuff", flags=0)
        self.assertEqual(value, "stuff")
        zlib.decompress.assertNotCalled()

    def test_deserialize_decompress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_value = object()
        zlib.decompress.return_value = expected_value

        value = memcache_lib.memcache_deserializer(
            key="key", value="nonsense", flags=memcache_lib.FLAG_ZLIB)
        self.assertEqual(value, expected_value)
        zlib.decompress.assertCalledWith("nonsense")
