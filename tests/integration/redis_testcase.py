import unittest

import redis

import baseplate.clients.redis as baseplate_redis

from baseplate import Baseplate

from . import get_endpoint_or_skip_container
from . import TestBaseplateObserver

redis_url = f'redis://{get_endpoint_or_skip_container("redis", 6379)}'
redis_cluster_url = f'redis://{get_endpoint_or_skip_container("redis-cluster-node", 7000)}'


class RedisIntegrationTestConfigurationError(Exception):
    pass


class RedisIntegrationTestCase(unittest.TestCase):
    baseplate_app_config = None
    redis_client_builder = None
    redis_context_name = "redis"

    def setUp(self):
        if not self.baseplate_app_config:
            raise RedisIntegrationTestConfigurationError(
                "Unable to run tests. `baseplate_app_config` is not set",
            )

        if not self.redis_client_builder:
            raise RedisIntegrationTestConfigurationError(
                "Unable to setup Redis client, 'redis_client_builder' not set",
            )

        self.setup_baseplate_redis()

    def setup_baseplate_redis(self, redis_client_kwargs={}):
        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate(self.baseplate_app_config)
        baseplate.register(self.baseplate_observer)
        baseplate.configure_context(
            {self.redis_context_name: self.redis_client_builder(**redis_client_kwargs)}
        )

        self.context = baseplate.make_context_object()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def tearDown(self):
        # You can't use the Redis connection on the Baseplate context because its
        # parent server span has been closed already
        redis_cli = redis.Redis.from_url(redis_url)
        redis_cli.flushall()

        redis_cluster_cli = redis.Redis.from_url(redis_cluster_url)
        redis_cluster_cli.flushall()

        # Clear Prometheus metrics
        baseplate_redis.LATENCY_SECONDS.clear()
        baseplate_redis.REQUESTS_TOTAL.clear()
        baseplate_redis.ACTIVE_REQUESTS.clear()
        baseplate_redis.MAX_CONNECTIONS.clear()
        baseplate_redis.IDLE_CONNECTIONS.clear()
        baseplate_redis.OPEN_CONNECTIONS.clear()
