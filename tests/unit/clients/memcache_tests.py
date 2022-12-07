import builtins
import unittest

from unittest import mock

try:
    import pymemcache
except ImportError:
    raise unittest.SkipTest("pymemcache is not installed")
else:
    del pymemcache

from prometheus_client import REGISTRY
from baseplate.lib.config import ConfigurationError
from baseplate.clients.memcache import pool_from_config
from baseplate.clients.memcache import MonitoredMemcacheConnection
from baseplate.clients.memcache import lib as memcache_lib


class PrometheusInstrumentationTests(unittest.TestCase):
    def setUp(self):
        # stub out the server span, each call to MonitoredMemcacheConnection
        # will create a new span - but we don't use that for prom
        span_inner = mock.Mock()
        span_inner.__enter__ = mock.Mock(return_value=mock.Mock())
        span_inner.__exit__ = mock.Mock(return_value=None)
        self.span = mock.Mock()
        self.span.make_child = lambda trace_name: span_inner

    # test the @_prom_instrument decorator by calling one of the decorated functions
    def test_prometheus_decorator(self):
        # stub out the client, we only use the server name
        pooled_client = mock.Mock()
        pooled_client.server = "server name"

        prom_labels = {
            "memcached_address": "server name",
            "memcached_command": "incr",
        }

        mmc = MonitoredMemcacheConnection("test", self.span, pooled_client)
        mmc.incr("blah", 1)

        assert (
            REGISTRY.get_sample_value(
                "memcached_client_active_requests",
                prom_labels,
            )
            == 0
        )
        assert (
            REGISTRY.get_sample_value(
                "memcached_client_requests_total",
                {
                    **prom_labels,
                    "memcached_success": "true",
                },
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                "memcached_client_latency_seconds_bucket",
                {
                    **prom_labels,
                    "memcached_success": "true",
                    "le": "+Inf",
                },
            )
            == 1
        )

    def test_prometheus_decorator_failing(self):
        class FailClient(mock.Mock):
            server = "hello"

            def incr(self, _a, _b, noreply=None):
                raise TypeError

        fail_mmc = MonitoredMemcacheConnection("test", self.span, FailClient())
        self.assertRaises(TypeError, fail_mmc.incr, "aaa", 1)

        assert (
            REGISTRY.get_sample_value(
                "memcached_client_requests_total",
                {
                    "memcached_address": "hello",
                    "memcached_command": "incr",
                    "memcached_success": "false",
                },
            )
            == 1
        )


class PoolFromConfigTests(unittest.TestCase):
    def test_empty_config(self):
        with self.assertRaises(ConfigurationError):
            pool_from_config({})

    def test_basic_url(self):
        pool = pool_from_config({"memcache.endpoint": "localhost:1234"})

        self.assertEqual(pool.server[0], "localhost")
        self.assertEqual(pool.server[1], 1234)

    def test_timeouts(self):
        pool = pool_from_config(
            {
                "memcache.endpoint": "localhost:1234",
                "memcache.timeout": "1.23",
                "memcache.connect_timeout": "4.56",
            }
        )

        self.assertEqual(pool.timeout, 1.23)
        self.assertEqual(pool.connect_timeout, 4.56)

    def test_max_connections(self):
        pool = pool_from_config(
            {"memcache.endpoint": "localhost:1234", "memcache.max_pool_size": "300"}
        )

        self.assertEqual(pool.client_pool.max_size, 300)

    def test_alternate_prefix(self):
        pool_from_config({"noodle.endpoint": "localhost:1234"}, prefix="noodle.")

    def test_nodelay(self):
        pool = pool_from_config(
            {"memcache.endpoint": "localhost:1234", "memcache.no_delay": "False"}
        )
        self.assertEqual(pool.no_delay, False)


class SerdeTests(unittest.TestCase):
    def test_serialize_str(self):
        dump_no_compress = memcache_lib.make_dump_and_compress_fn()
        value, flags = dump_no_compress(key="key", value="val")
        self.assertEqual(value, b"val")
        self.assertEqual(flags, 0)

    def test_serialize_bytes(self):
        dump_no_compress = memcache_lib.make_dump_and_compress_fn()
        value, flags = dump_no_compress(key="key", value=b"val")
        self.assertEqual(value, b"val")
        self.assertEqual(flags, 0)

    def test_serialize_int(self):
        dump_no_compress = memcache_lib.make_dump_and_compress_fn()
        value, flags = dump_no_compress(key="key", value=100)
        self.assertEqual(value, b"100")
        self.assertEqual(flags, memcache_lib.Flags.INTEGER)

    def test_serialize_other(self):
        json_patch = mock.patch.object(memcache_lib, "json")
        json = json_patch.start()
        self.addCleanup(json_patch.stop)

        json.dumps.return_value = "expected"

        dump_no_compress = memcache_lib.make_dump_and_compress_fn()
        value, flags = dump_no_compress(key="key", value=("stuff", 1, False))

        json.dumps.assert_called_with(("stuff", 1, False))
        self.assertEqual(value, b"expected")
        self.assertEqual(flags, memcache_lib.Flags.JSON)

    def test_deserialize_str(self):
        value = memcache_lib.decompress_and_load(key="key", serialized="val", flags=0)
        self.assertEqual(value, "val")

    def test_deserialize_bytes(self):
        value = memcache_lib.decompress_and_load(key="key", serialized=b"val", flags=0)
        self.assertEqual(value, b"val")

    def test_deserialize_int(self):
        value = memcache_lib.decompress_and_load(
            key="key", serialized="100", flags=memcache_lib.Flags.INTEGER
        )
        self.assertEqual(value, 100)
        self.assertTrue(isinstance(value, int))

    def test_deserialize_long(self):
        value = memcache_lib.decompress_and_load(
            key="key", serialized="100", flags=memcache_lib.Flags.LONG
        )

        self.assertEqual(value, 100)
        self.assertTrue(isinstance(value, int))

    def test_deserialize_other(self):
        json_patch = mock.patch.object(memcache_lib, "json")
        json = json_patch.start()
        self.addCleanup(json_patch.stop)

        expected_value = object()
        json.loads.return_value = expected_value

        value = memcache_lib.decompress_and_load(
            key="key", serialized=b"garbage", flags=memcache_lib.Flags.JSON
        )

        json.loads.assert_called_with(b"garbage")
        self.assertEqual(value, expected_value)

    def test_serialize_no_compress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        dump_no_compress = memcache_lib.make_dump_and_compress_fn(
            min_compress_length=0  # disable compression
        )
        value, flags = dump_no_compress(key="key", value="simple string")
        self.assertEqual(value, b"simple string")
        self.assertEqual(flags, 0)
        zlib.compress.assert_not_called()

    def test_serialize_compress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_value = object()
        zlib.compress.return_value = expected_value

        json_and_compress = memcache_lib.make_dump_and_compress_fn(
            min_compress_length=1, compress_level=1
        )
        value, flags = json_and_compress(key="key", value="simple string")
        self.assertEqual(value, expected_value)
        self.assertEqual(flags, memcache_lib.Flags.ZLIB)
        zlib.compress.assert_called_with(b"simple string", 1)

    def test_deserialize_no_decompress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        value = memcache_lib.decompress_and_load(key="key", serialized="stuff", flags=0)
        self.assertEqual(value, "stuff")
        zlib.decompress.assert_not_called()

    def test_deserialize_decompress_str(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_value = object()
        zlib.decompress.return_value = expected_value

        flags = 0 | memcache_lib.Flags.ZLIB
        value = memcache_lib.decompress_and_load(key="key", serialized=b"nonsense", flags=flags)
        self.assertEqual(value, expected_value)
        zlib.decompress.assert_called_with(b"nonsense")

    def test_deserialize_decompress_int(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_zlib_value = object()
        zlib.decompress.return_value = expected_zlib_value

        int_patch = mock.patch.object(builtins, "int")
        int_cls = int_patch.start()
        self.addCleanup(int_patch.stop)

        expected_int_value = object()
        int_cls.return_value = expected_int_value
        flags = memcache_lib.Flags.INTEGER | memcache_lib.Flags.ZLIB
        value = memcache_lib.decompress_and_load(key="key", serialized="nonsense", flags=flags)
        zlib.decompress.assert_called_with("nonsense")
        int_cls.assert_called_with(expected_zlib_value)
        self.assertEqual(value, expected_int_value)

    def test_deserialize_decompress_long(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_zlib_value = object()
        zlib.decompress.return_value = expected_zlib_value

        long_patch = mock.patch.object(memcache_lib, "int")
        long_cls = long_patch.start()
        self.addCleanup(long_patch.stop)

        expected_long_value = object()
        long_cls.return_value = expected_long_value
        flags = memcache_lib.Flags.LONG | memcache_lib.Flags.ZLIB
        value = memcache_lib.decompress_and_load(key="key", serialized="nonsense", flags=flags)
        zlib.decompress.assert_called_with("nonsense")
        long_cls.assert_called_with(expected_zlib_value)
        self.assertEqual(value, expected_long_value)

    def test_deserialize_decompress_unjson(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_zlib_value = object()
        zlib.decompress.return_value = expected_zlib_value

        json_patch = mock.patch.object(memcache_lib, "json")
        json = json_patch.start()
        self.addCleanup(json_patch.stop)

        expected_json_value = object()
        json.loads.return_value = expected_json_value

        flags = memcache_lib.Flags.JSON | memcache_lib.Flags.ZLIB
        value = memcache_lib.decompress_and_load(key="key", serialized="nonsense", flags=flags)
        zlib.decompress.assert_called_with("nonsense")
        json.loads.assert_called_with(expected_zlib_value)
        self.assertEqual(value, expected_json_value)


class R2CompatSerdeTests(unittest.TestCase):
    def test_serialize_str(self):
        pickle_no_compress = memcache_lib.make_pickle_and_compress_fn()
        value, flags = pickle_no_compress(key="key", value="val")
        self.assertEqual(value, b"val")
        self.assertEqual(flags, 0)

    def test_serialize_bytes(self):
        pickle_no_compress = memcache_lib.make_pickle_and_compress_fn()
        value, flags = pickle_no_compress(key="key", value=b"val")
        self.assertEqual(value, b"val")
        self.assertEqual(flags, 0)

    def test_serialize_int(self):
        pickle_no_compress = memcache_lib.make_pickle_and_compress_fn()
        value, flags = pickle_no_compress(key="key", value=100)
        self.assertEqual(value, b"100")
        self.assertEqual(flags, memcache_lib.PickleFlags.INTEGER)

    def test_serialize_other(self):
        pickle_patch = mock.patch.object(memcache_lib, "pickle")
        pickle = pickle_patch.start()
        self.addCleanup(pickle_patch.stop)

        expected_value = object()
        pickle.dumps.return_value = expected_value

        pickle_no_compress = memcache_lib.make_pickle_and_compress_fn()
        value, flags = pickle_no_compress(key="key", value=("stuff", 1, False))

        pickle.dumps.assert_called_with(("stuff", 1, False), protocol=2)
        self.assertEqual(value, expected_value)
        self.assertEqual(flags, memcache_lib.PickleFlags.PICKLE)

    def test_deserialize_str(self):
        value = memcache_lib.decompress_and_unpickle(key="key", serialized="val", flags=0)
        self.assertEqual(value, "val")

    def test_deserialize_bytes(self):
        value = memcache_lib.decompress_and_unpickle(key="key", serialized=b"val", flags=0)
        self.assertEqual(value, b"val")

    def test_deserialize_int(self):
        value = memcache_lib.decompress_and_unpickle(
            key="key", serialized="100", flags=memcache_lib.PickleFlags.INTEGER
        )
        self.assertEqual(value, 100)
        self.assertTrue(isinstance(value, int))

    def test_deserialize_long(self):
        value = memcache_lib.decompress_and_unpickle(
            key="key", serialized="100", flags=memcache_lib.PickleFlags.LONG
        )

        self.assertEqual(value, 100)
        self.assertTrue(isinstance(value, int))

    def test_deserialize_other(self):
        pickle_patch = mock.patch.object(memcache_lib, "pickle")
        pickle = pickle_patch.start()
        self.addCleanup(pickle_patch.stop)

        expected_value = object()
        pickle.loads.return_value = expected_value

        value = memcache_lib.decompress_and_unpickle(
            key="key", serialized="garbage", flags=memcache_lib.PickleFlags.PICKLE
        )

        pickle.loads.assert_called_with("garbage")
        self.assertEqual(value, expected_value)

    def test_serialize_no_compress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        pickle_no_compress = memcache_lib.make_pickle_and_compress_fn(
            min_compress_length=0  # disable compression
        )
        value, flags = pickle_no_compress(key="key", value="simple string")
        self.assertEqual(value, b"simple string")
        self.assertEqual(flags, 0)
        zlib.compress.assert_not_called()

    def test_serialize_compress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_value = object()
        zlib.compress.return_value = expected_value

        pickle_and_compress = memcache_lib.make_pickle_and_compress_fn(
            min_compress_length=1, compress_level=1
        )
        value, flags = pickle_and_compress(key="key", value="simple string")
        self.assertEqual(value, expected_value)
        self.assertEqual(flags, memcache_lib.PickleFlags.ZLIB)
        zlib.compress.assert_called_with(b"simple string", 1)

    def test_deserialize_no_decompress(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        value = memcache_lib.decompress_and_unpickle(key="key", serialized="stuff", flags=0)
        self.assertEqual(value, "stuff")
        zlib.decompress.assert_not_called()

    def test_deserialize_decompress_str(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_value = object()
        zlib.decompress.return_value = expected_value

        flags = 0 | memcache_lib.PickleFlags.ZLIB
        value = memcache_lib.decompress_and_unpickle(key="key", serialized="nonsense", flags=flags)
        self.assertEqual(value, expected_value)
        zlib.decompress.assert_called_with("nonsense")

    def test_deserialize_decompress_int(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_zlib_value = object()
        zlib.decompress.return_value = expected_zlib_value

        int_patch = mock.patch.object(builtins, "int")
        int_cls = int_patch.start()
        self.addCleanup(int_patch.stop)

        expected_int_value = object()
        int_cls.return_value = expected_int_value
        flags = memcache_lib.PickleFlags.INTEGER | memcache_lib.PickleFlags.ZLIB
        value = memcache_lib.decompress_and_unpickle(key="key", serialized="nonsense", flags=flags)
        zlib.decompress.assert_called_with("nonsense")
        int_cls.assert_called_with(expected_zlib_value)
        self.assertEqual(value, expected_int_value)

    def test_deserialize_decompress_long(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_zlib_value = object()
        zlib.decompress.return_value = expected_zlib_value

        long_patch = mock.patch.object(memcache_lib, "int")
        long_cls = long_patch.start()
        self.addCleanup(long_patch.stop)

        expected_long_value = object()
        long_cls.return_value = expected_long_value
        flags = memcache_lib.PickleFlags.LONG | memcache_lib.PickleFlags.ZLIB
        value = memcache_lib.decompress_and_unpickle(key="key", serialized="nonsense", flags=flags)
        zlib.decompress.assert_called_with("nonsense")
        long_cls.assert_called_with(expected_zlib_value)
        self.assertEqual(value, expected_long_value)

    def test_deserialize_decompress_unpickle(self):
        zlib_patch = mock.patch.object(memcache_lib, "zlib")
        zlib = zlib_patch.start()
        self.addCleanup(zlib_patch.stop)

        expected_zlib_value = object()
        zlib.decompress.return_value = expected_zlib_value

        pickle_patch = mock.patch.object(memcache_lib, "pickle")
        pickle = pickle_patch.start()
        self.addCleanup(pickle_patch.stop)

        expected_pickle_value = object()
        pickle.loads.return_value = expected_pickle_value

        flags = memcache_lib.PickleFlags.PICKLE | memcache_lib.PickleFlags.ZLIB
        value = memcache_lib.decompress_and_unpickle(key="key", serialized="nonsense", flags=flags)
        zlib.decompress.assert_called_with("nonsense")
        pickle.loads.assert_called_with(expected_zlib_value)
        self.assertEqual(value, expected_pickle_value)
