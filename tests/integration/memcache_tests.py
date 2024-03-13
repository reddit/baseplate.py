import unittest

from unittest import mock

from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

try:
    from pymemcache.exceptions import MemcacheClientError
except ImportError:
    raise unittest.SkipTest("pymemcache is not installed")

from baseplate.clients.memcache import MemcacheClient, MonitoredMemcacheConnection, make_keys_str
from baseplate import Baseplate

from . import TestBaseplateObserver, get_endpoint_or_skip_container


memcached_endpoint = get_endpoint_or_skip_container("memcached", 11211)

tracer = trace.get_tracer(__name__)


class MemcacheIntegrationTests(TestBase):
    def setUp(self):
        super().setUp()
        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate({"memcache.endpoint": str(memcached_endpoint)})
        baseplate.register(self.baseplate_observer)
        baseplate.configure_context({"memcache": MemcacheClient()})

        self.context = baseplate.make_context_object()
        self.server_span = tracer.start_span("test")
        self.context.span = self.server_span

    def test_simple(self):
        with self.server_span:
            self.context.memcache.get("whatever")

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.get")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )

    def test_error(self):
        with self.server_span:
            with self.assertRaises(MemcacheClientError):
                self.context.memcache.cas("key", b"value", b"whatever")

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.cas")
        self.assertFalse(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )

    def test_close(self):
        with self.server_span:
            self.context.memcache.close()
        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.close")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )

    def test_set(self):
        with self.server_span:
            self.context.memcache.set("key", "value", expire=0, noreply=False)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.set")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "key"})

    def test_replace(self):
        with self.server_span:
            self.context.memcache.replace("key", "value", expire=0, noreply=False)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.replace")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "key"})

    def test_append(self):
        with self.server_span:
            self.context.memcache.append("key", "value", expire=0, noreply=False)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.append")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "key"})

    def test_prepend(self):
        with self.server_span:
            self.context.memcache.prepend("key", "value", expire=0, noreply=False)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.prepend")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "key"})

    def test_cas(self):
        with self.server_span:
            self.context.memcache.cas("caskey2", "value", b"0", expire=0, noreply=False)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.cas")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "caskey2", "cas": "0"})

    def test_get(self):
        self.context.memcache.set("key", "value")
        with self.server_span:
            self.context.memcache.get("key")

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 1)
        self.assertEqual(finished_spans[1].name, "memcache.get")
        self.assertTrue(finished_spans[1].status.is_ok)
        self.assertEqual(
            finished_spans[1].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[1], {"key": "key"})

    def test_gets(self):
        self.context.memcache.set("key", "value")
        with self.server_span:
            self.context.memcache.gets("key")

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 1)
        self.assertEqual(finished_spans[1].name, "memcache.gets")
        self.assertTrue(finished_spans[1].status.is_ok)
        self.assertEqual(
            finished_spans[1].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[1], {"key": "key"})

    def test_gets_many(self):
        keys = [b"key1", b"key2"]
        self.context.memcache.set_many({"key1": "value", "key2": "value"})
        with self.server_span:
            self.context.memcache.gets_many(keys)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 1)
        self.assertEqual(finished_spans[1].name, "memcache.gets_many")
        self.assertTrue(finished_spans[1].status.is_ok)
        self.assertEqual(
            finished_spans[1].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[1], {"keys": "key1,key2"})

    def test_delete(self):
        self.context.memcache.set("key", "value")
        with self.server_span:
            self.context.memcache.delete("key")

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 1)
        self.assertEqual(finished_spans[1].name, "memcache.delete")
        self.assertTrue(finished_spans[1].status.is_ok)
        self.assertEqual(
            finished_spans[1].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[1], {"key": "key"})

    def test_delete_many(self):
        keys = [b"key1", b"key2"]
        self.context.memcache.set_many({"key1": "value", "key2": "value"})
        with self.server_span:
            self.context.memcache.delete_many(keys)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 1)
        self.assertEqual(finished_spans[1].name, "memcache.delete_many")
        self.assertTrue(finished_spans[1].status.is_ok)
        self.assertEqual(
            finished_spans[1].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[1], {"keys": "key1,key2"})

    def test_add(self):
        with self.server_span:
            self.context.memcache.add("key", 1)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.add")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "key"})

    def test_incr(self):
        with self.server_span:
            self.context.memcache.incr("key_val", 1)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.incr")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "key_val"})

    def test_decr(self):
        with self.server_span:
            self.context.memcache.decr("key_val", 1)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.decr")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "key_val"})

    def test_touch(self):
        with self.server_span:
            self.context.memcache.touch("key_val", 1)

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.touch")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"key": "key_val"})

    def test_flush_all(self):
        with self.server_span:
            self.context.memcache.flush_all()

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.flush_all")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"delay": 0})

    def test_quit(self):
        with self.server_span:
            self.context.memcache.quit()

        finished_spans = self.get_finished_spans()
        self.assertGreater(len(finished_spans), 0)
        self.assertEqual(finished_spans[0].name, "memcache.quit")
        self.assertTrue(finished_spans[0].status.is_ok)
        self.assertEqual(
            finished_spans[0].parent.span_id, self.server_span.get_span_context().span_id
        )
        self.assertSpanHasAttributes(finished_spans[0], {"method": "quit"})


class MakeKeysStrTests(unittest.TestCase):
    def test_bytes(self):
        expected_string = "key_1,key_2"
        keys = [b"key_1", b"key_2"]
        self.assertEqual(expected_string, make_keys_str(keys))

    def test_str(self):
        expected_string = "key_1,key_2"
        keys = ["key_1", "key_2"]
        self.assertEqual(expected_string, make_keys_str(keys))
