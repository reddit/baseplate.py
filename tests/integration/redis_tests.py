import contextlib
import unittest

try:
    import redis
except ImportError:
    raise unittest.SkipTest("redis-py is not installed")

from baseplate.clients.redis import ACTIVE_REQUESTS
from baseplate.clients.redis import REQUESTS_TOTAL
from baseplate.clients.redis import LATENCY_SECONDS
from baseplate.clients.redis import RedisClient

from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from . import get_endpoint_or_skip_container
from .redis_testcase import RedisIntegrationTestCase, redis_url

from baseplate.clients.redis import MessageQueue
from baseplate.lib.message_queue import TimedOutError
from prometheus_client import REGISTRY

redis_endpoint = get_endpoint_or_skip_container("redis", 6379)


class RedisIntegrationTests(RedisIntegrationTestCase):
    def setUp(self):
        self.baseplate_app_config = {"redis.url": redis_url}
        self.redis_client_builder = RedisClient
        self.tracer = trace.get_tracer(__name__)

        super().setUp()

    def test_simple_command(self):
        with self.server_span, self.tracer.start_as_current_span("test_simple_command"):
            result = self.context.redis.ping()

        self.assertTrue(result)
        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "redis.PING")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)

        # OTel Tracing Assertions
        finished = self.get_finished_spans()
        self.assertEqual(finished[0].name, "PING")
        self.assertEqual(finished[0].kind, trace.SpanKind.CLIENT)
        self.assertIsNotNone(finished[0].parent)
        self.assertTrue(finished[0].status.is_ok)

    def test_error(self):
        with self.server_span, self.tracer.start_as_current_span("test_error"):
            with self.assertRaises(redis.ResponseError):
                self.context.redis.execute_command("crazycommand")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)

        # OTel Tracing Assertions
        finished = self.get_finished_spans()
        self.assertEqual(finished[0].name, "crazycommand")
        self.assertEqual(finished[0].kind, trace.SpanKind.CLIENT)
        self.assertIsNotNone(finished[0].parent)
        self.assertFalse(finished[0].status.is_ok)

    def test_lock(self):
        with self.server_span, self.tracer.start_as_current_span("test_lock"):
            with self.context.redis.lock("foo"):
                pass

        server_span_observer = self.baseplate_observer.get_only_child()

        self.assertGreater(len(server_span_observer.children), 0)
        for span_observer in server_span_observer.children:
            self.assertTrue(span_observer.on_start_called)
            self.assertTrue(span_observer.on_finish_called)

        # OTel Tracing Assertions
        finished = self.get_finished_spans()
        self.assertEqual(finished[0].name, "SET")
        self.assertEqual(finished[0].kind, trace.SpanKind.CLIENT)
        self.assertEqual(finished[1].name, "EVALSHA")
        self.assertEqual(finished[1].kind, trace.SpanKind.CLIENT)
        self.assertIsNotNone(finished[0].parent)
        self.assertTrue(finished[0].parent.span_id, finished[1].context.span_id)
        self.assertTrue(finished[0].status.is_ok)

    def test_pipeline(self):
        with self.tracer.start_as_current_span("test_pipeline"):
            with self.server_span:
                with self.context.redis.pipeline("foo") as pipeline:
                    pipeline.ping()
                    pipeline.execute()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "redis.pipeline_foo")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)

        # OTel Tracing Assertions
        finished = self.get_finished_spans()
        self.assertEqual(finished[0].name, "PING")
        self.assertEqual(finished[0].kind, trace.SpanKind.CLIENT)
        self.assertIsNotNone(finished[0].parent)
        self.assertTrue(finished[0].status.is_ok)

    def test_metrics(self):
        client_name = "redisclient"
        for client_name_kwarg_name in [
            "redis_client_name",
            "client_name",
        ]:
            with self.subTest():
                self.setup_baseplate_redis(
                    redis_client_kwargs={
                        client_name_kwarg_name: client_name,
                    },
                )
                expected_labels = {
                    "redis_client_name": client_name,
                    "redis_type": "standalone",
                    "redis_command": "SET",
                    "redis_database": "0",
                }
                with self.server_span:
                    self.context.redis.set("prometheus", "rocks")

                request_labels = {**expected_labels, "redis_success": "true"}
                assert (
                    REGISTRY.get_sample_value(f"{REQUESTS_TOTAL._name}_total", request_labels)
                    == 1.0
                ), "Unexpected value for REQUESTS_TOTAL metric. Expected one 'set' command"
                assert (
                    REGISTRY.get_sample_value(
                        f"{LATENCY_SECONDS._name}_bucket", {**request_labels, "le": "+Inf"}
                    )
                    == 1.0
                ), "Expected one 'set' latency request"
                assert (
                    REGISTRY.get_sample_value(
                        ACTIVE_REQUESTS._name, {**expected_labels, "redis_type": "standalone"}
                    )
                    == 0.0
                ), "Should have 0 (and not None) active requests"

                # Each iteration of this loop is effectively a different testcase
                self.tearDown()


class RedisMessageQueueTests(TestBase):
    qname = "redisTestQueue"

    def setUp(self):
        super().setUp()
        self.pool = redis.ConnectionPool(
            host=redis_endpoint.address.host, port=redis_endpoint.address.port
        )

    def test_put_get(self):
        message_queue = MessageQueue(self.qname, self.pool)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x")
            message = mq.get()
            self.assertEqual(message, b"x")

    def test_get_zero_timeout(self):
        message_queue = MessageQueue(self.qname, self.pool)

        message_queue.put(b"y")
        message = message_queue.get(timeout=0)
        self.assertEqual(message, b"y")

        with self.assertRaises(TimedOutError):
            message_queue.get(timeout=0)

    def test_put_zero_timeout(self):
        message_queue = MessageQueue(self.qname, self.pool)

        with contextlib.closing(message_queue) as mq:
            mq.put(b"x", timeout=0)
            message = mq.get()
            self.assertEqual(message, b"x")

    def tearDown(self):
        super().tearDown()
        redis.Redis(connection_pool=self.pool).delete(self.qname)
