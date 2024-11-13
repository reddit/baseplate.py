import unittest

from unittest import mock

try:
    from pymemcache.exceptions import MemcacheClientError
except ImportError:
    raise unittest.SkipTest("pymemcache is not installed")

from baseplate.clients.memcache import MemcacheClient, MonitoredMemcacheConnection, make_keys_str
from baseplate import Baseplate, LocalSpan, ServerSpan

from . import TestBaseplateObserver, get_endpoint_or_skip_container


memcached_endpoint = get_endpoint_or_skip_container("memcached", 11211)


class MemcacheIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate({"memcache.endpoint": str(memcached_endpoint)})
        baseplate.register(self.baseplate_observer)
        baseplate.configure_context({"memcache": MemcacheClient()})

        self.context = baseplate.make_context_object()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple(self):
        with self.server_span:
            self.context.memcache.get("whatever")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "memcache.get")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)

    def test_error(self):
        with self.server_span:
            with self.assertRaises(MemcacheClientError):
                self.context.memcache.cas("key", b"value", b"whatever")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "memcache.cas")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)


class MonitoredMemcacheConnectionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.mocked_pool = mock.Mock(server=memcached_endpoint.address)
        self.context_name = "memcache"
        self.server_span = mock.MagicMock(spec_set=ServerSpan)
        self.local_span = mock.MagicMock(spec_set=LocalSpan)
        self.local_span.__enter__.return_value = self.local_span
        self.server_span.make_child.return_value = self.local_span
        self.connection = MonitoredMemcacheConnection(
            self.context_name, self.server_span, self.mocked_pool
        )

        self.key = b"key"
        self.value = b"value"
        self.expire = 0
        self.noreply = False

    def test_close(self):
        self.connection.close()
        self.mocked_pool.close.assert_called_with()
        self.assertEqual(self.local_span.set_tag.call_count, 1)

    def test_set(self):
        self.connection.set(self.key, self.value, expire=self.expire, noreply=self.noreply)
        self.mocked_pool.set.assert_called_with(
            self.key, self.value, expire=self.expire, noreply=self.noreply
        )
        self.assertEqual(self.local_span.set_tag.call_count, 4)

    def test_replace(self):
        self.connection.replace(self.key, self.value, expire=self.expire, noreply=self.noreply)
        self.mocked_pool.replace.assert_called_with(
            self.key, self.value, expire=self.expire, noreply=self.noreply
        )
        self.assertEqual(self.local_span.set_tag.call_count, 4)

    def test_append(self):
        self.connection.append(self.key, self.value, expire=self.expire, noreply=self.noreply)
        self.mocked_pool.append.assert_called_with(
            self.key, self.value, expire=self.expire, noreply=self.noreply
        )
        self.assertEqual(self.local_span.set_tag.call_count, 4)

    def test_prepend(self):
        self.connection.prepend(self.key, self.value, expire=self.expire, noreply=self.noreply)
        self.mocked_pool.prepend.assert_called_with(
            self.key, self.value, expire=self.expire, noreply=self.noreply
        )
        self.assertEqual(self.local_span.set_tag.call_count, 4)

    def test_cas(self):
        cas = b"cascas"
        self.connection.cas(self.key, self.value, cas, expire=self.expire, noreply=self.noreply)
        self.mocked_pool.cas.assert_called_with(
            self.key, self.value, cas, expire=self.expire, noreply=self.noreply
        )
        self.assertEqual(self.local_span.set_tag.call_count, 5)

    def test_get(self):
        self.connection.get(self.key)
        self.mocked_pool.get.assert_called_with(self.key)
        self.assertEqual(self.local_span.set_tag.call_count, 2)

    def test_gets(self):
        self.connection.gets(self.key)
        self.mocked_pool.gets.assert_called_with(self.key, default=None, cas_default=None)
        self.assertEqual(self.local_span.set_tag.call_count, 2)

    def test_gets_many(self):
        keys = [b"key1", b"key2"]
        self.connection.gets_many(keys)
        self.mocked_pool.gets_many.assert_called_with(keys)
        self.assertEqual(self.local_span.set_tag.call_count, 3)

    def test_delete(self):
        self.connection.delete(self.key, noreply=self.noreply)
        self.mocked_pool.delete.assert_called_with(self.key, noreply=self.noreply)
        self.assertEqual(self.local_span.set_tag.call_count, 3)

    def test_delete_many(self):
        keys = [b"key1", b"key2"]
        self.connection.delete_many(keys, noreply=self.noreply)
        self.mocked_pool.delete_many.assert_called_with(keys, noreply=self.noreply)
        self.assertEqual(self.local_span.set_tag.call_count, 4)

    def test_add(self):
        self.connection.add(self.key, self.value, expire=self.expire, noreply=self.noreply)
        self.mocked_pool.add.assert_called_with(
            self.key, self.value, expire=self.expire, noreply=self.noreply
        )
        self.assertEqual(self.local_span.set_tag.call_count, 4)

    def test_incr(self):
        value = 1
        self.connection.incr(self.key, value, noreply=self.noreply)
        self.mocked_pool.incr.assert_called_with(self.key, value, noreply=self.noreply)
        self.assertEqual(self.local_span.set_tag.call_count, 3)

    def test_decr(self):
        value = 1
        self.connection.decr(self.key, value, noreply=self.noreply)
        self.mocked_pool.decr.assert_called_with(self.key, value, noreply=self.noreply)
        self.assertEqual(self.local_span.set_tag.call_count, 3)

    def test_touch(self):
        self.connection.touch(self.key, expire=self.expire, noreply=self.noreply)
        self.mocked_pool.touch.assert_called_with(
            self.key, expire=self.expire, noreply=self.noreply
        )
        self.assertEqual(self.local_span.set_tag.call_count, 4)

    def test_flush_all(self):
        delay = 0
        self.connection.flush_all(delay=delay, noreply=self.noreply)
        self.mocked_pool.flush_all.assert_called_with(delay=delay, noreply=self.noreply)
        self.assertEqual(self.local_span.set_tag.call_count, 3)

    def test_quit(self):
        self.connection.quit()
        self.mocked_pool.quit.assert_called_with()
        self.assertEqual(self.local_span.set_tag.call_count, 1)


class MakeKeysStrTests(unittest.TestCase):
    def test_bytes(self):
        expected_string = "key_1,key_2"
        keys = [b"key_1", b"key_2"]
        self.assertEqual(expected_string, make_keys_str(keys))

    def test_str(self):
        expected_string = "key_1,key_2"
        keys = ["key_1", "key_2"]
        self.assertEqual(expected_string, make_keys_str(keys))
