import unittest

try:
    import rediscluster
except ImportError:
    raise unittest.SkipTest("redis-py-cluster is not installed")


from baseplate.clients.redis_cluster import ClusterRedisClient
from baseplate import Baseplate
from . import TestBaseplateObserver, get_endpoint_or_skip_container

redis_endpoint = get_endpoint_or_skip_container("redis-cluster-node", 7000)


class RedisClusterIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate(
            {
                "rediscluster.url": f"redis://{redis_endpoint}/0",
                "rediscluster.timeout": "1 second",
                "rediscluster.max_connections": "4",
            }
        )
        baseplate.register(self.baseplate_observer)
        baseplate.configure_context({"rediscluster": ClusterRedisClient()})

        self.context = baseplate.make_context_object()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple_command(self):
        with self.server_span:
            result = self.context.rediscluster.ping()

        self.assertTrue(result)

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "rediscluster.PING")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)

    def test_error(self):
        with self.server_span:
            with self.assertRaises(rediscluster.RedisClusterException):
                self.context.rediscluster.execute_command("crazycommand")

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)

    def test_lock(self):
        with self.server_span:
            with self.context.rediscluster.lock("foo-lock"):
                pass

        server_span_observer = self.baseplate_observer.get_only_child()

        self.assertGreater(len(server_span_observer.children), 0)
        for span_observer in server_span_observer.children:
            self.assertTrue(span_observer.on_start_called)
            self.assertTrue(span_observer.on_finish_called)

    def test_pipeline(self):
        with self.server_span:
            with self.context.rediscluster.pipeline("foo") as pipeline:
                pipeline.set("foo", "bar")
                pipeline.get("foo")
                pipeline.get("foo")
                pipeline.get("foo")
                pipeline.get("foo")
                pipeline.get("foo")
                pipeline.delete("foo")
                pipeline.execute()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertEqual(span_observer.span.name, "rediscluster.pipeline_foo")
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
