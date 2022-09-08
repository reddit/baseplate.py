import unittest

try:
    import rediscluster
except ImportError:
    raise unittest.SkipTest("redis-py-cluster is not installed")

from baseplate.lib.config import ConfigurationError
from baseplate.clients.redis_cluster import cluster_pool_from_config

from baseplate.clients.redis_cluster import ClusterRedisClient
from baseplate import Baseplate
from . import TestBaseplateObserver, get_endpoint_or_skip_container

redis_endpoint = get_endpoint_or_skip_container("redis-cluster-node", 7000)


# This belongs on the unit tests section but the client class attempts to initialise
# the list of nodes when being instantiated so it's simpler to test here with a redis
# cluster available
class ClusterPoolFromConfigTests(unittest.TestCase):
    def test_empty_config(self):
        with self.assertRaises(ConfigurationError):
            cluster_pool_from_config({})

    def test_basic_url(self):
        pool = cluster_pool_from_config({"rediscluster.url": f"redis://{redis_endpoint}/0"})

        self.assertEqual(pool.nodes.startup_nodes[0]["host"], "redis-cluster-node")
        self.assertEqual(pool.nodes.startup_nodes[0]["port"], "7000")

    def test_timeouts(self):
        pool = cluster_pool_from_config(
            {
                "rediscluster.url": f"redis://{redis_endpoint}/0",
                "rediscluster.timeout": "30 seconds",
            }
        )

        self.assertEqual(pool.timeout, 30)

    def test_max_connections(self):
        pool = cluster_pool_from_config(
            {
                "rediscluster.url": f"redis://{redis_endpoint}/0",
                "rediscluster.max_connections": "300",
            }
        )

        self.assertEqual(pool.max_connections, 300)

    def test_max_connections_default(self):
        # https://github.com/Grokzen/redis-py-cluster/issues/435
        pool = cluster_pool_from_config({"rediscluster.url": f"redis://{redis_endpoint}/0"})

        self.assertEqual(pool.max_connections, 50)

    def test_kwargs_passthrough(self):
        pool = cluster_pool_from_config(
            {"rediscluster.url": f"redis://{redis_endpoint}/0"}, example="present"
        )

        self.assertEqual(pool.connection_kwargs["example"], "present")

    def test_alternate_prefix(self):
        pool = cluster_pool_from_config(
            {"noodle.url": f"redis://{redis_endpoint}/0"}, prefix="noodle."
        )
        self.assertEqual(pool.nodes.startup_nodes[0]["host"], "redis-cluster-node")
        self.assertEqual(pool.nodes.startup_nodes[0]["port"], "7000")

    def test_only_primary_available(self):
        pool = cluster_pool_from_config({"rediscluster.url": f"redis://{redis_endpoint}/0"})
        node_list = [pool.get_node_by_slot(slot=1, read_command=False) for _ in range(0, 100)]

        # The primary is on port 7000 so that's the only port we expect to see
        self.assertTrue(all(node["port"] == 7000 for node in node_list))

    def test_read_from_replicas(self):
        pool = cluster_pool_from_config({"rediscluster.url": f"redis://{redis_endpoint}/0"})

        node_list = [pool.get_node_by_slot(slot=1, read_command=True) for _ in range(0, 100)]

        # Both replicas and primary are available, so we expect to see some non-primaries here
        self.assertTrue(any(node["port"] != 7000 for node in node_list))


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
